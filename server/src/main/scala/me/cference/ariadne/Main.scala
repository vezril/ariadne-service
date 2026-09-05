package me.cference.ariadne

import me.cference.ariadne.build.BuildInfo
import me.cference.ariadne.config.AppConfig
import me.cference.ariadne.http.{
  CatalogRoutes,
  DocsRoutes,
  HealthRoutes,
  HelloRoutes,
  HttpServer,
  ReviewRoutes
}
import me.cference.ariadne.config.ScrapeConfig
import me.cference.ariadne.domain.CorrelationId
import me.cference.ariadne.domain.price.{PriceCommand, PriceSource}
import me.cference.ariadne.domain.resolution.{ResolutionCommand, ResolutionId}
import me.cference.ariadne.ingest.PostgresRawArchive
import me.cference.ariadne.ingest.flipp.{
  FlippMapper,
  PostgresFlyerLedger,
  ScrapeRun,
  ScrapeScheduler
}
import me.cference.ariadne.ingest.http.{
  HttpPolicyConfig,
  PekkoTransport,
  PoliteFetcher,
  RateLimiter
}
import me.cference.ariadne.persistence.{
  PriceStreamEntity,
  ResolutionCaseEntity,
  SchemaMigration,
  Sharding
}
import me.cference.ariadne.projection.{AriadneProjections, ReadModelRepository}
import me.cference.ariadne.resolver.{ResolutionService, Resolver}
import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import org.apache.pekko.Done
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/**
 * Entry point. Loads configuration, binds the HTTP surface (`GET /` hello + `GET /health`), and
 * wires Pekko Coordinated Shutdown (withdraw readiness -> unbind -> drain -> terminate). A bind
 * failure (e.g. an occupied port) logs clearly and exits non-zero.
 */
object Main:
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    val raw = ConfigFactory.load()
    val cfg = AppConfig.load(raw)

    given system: ActorSystem[Nothing] =
      ActorSystem[Nothing](Behaviors.empty[Nothing], "ariadne", raw)
    import system.executionContext

    // Schema first: the projections and the journal both need their tables to exist
    // before anything starts reading or writing.
    SchemaMigration.applyIfEnabled(raw)

    val repo = new ReadModelRepository(connectionFactory(raw))

    // Sharding before projections: the projections read the journal these entities write.
    Sharding.init(system)
    AriadneProjections.init(repo)

    given Timeout = Timeout(5.seconds)
    val decide: (ResolutionId, ResolutionCommand) => Future[Either[String, Unit]] =
      (id, cmd) =>
        Sharding
          .resolution(system, id.value)
          .askWithStatus[Done](ResolutionCaseEntity.Execute(cmd, _))
          .map(_ => Right(()))
          // A domain refusal is a legitimate answer, not a failure of the call: the
          // route turns it into a 409. Only genuine faults propagate.
          .recover { case e: org.apache.pekko.pattern.StatusReply.ErrorMessage =>
            Left(e.getMessage)
          }

    startScraping(cfg.scrape, repo)

    // Readiness flips UP once the server is bound; withdrawn first on shutdown.
    val readiness = new AtomicBoolean(false)
    val routes =
      HelloRoutes() ~
        HealthRoutes(BuildInfo.version, () => readiness.get()) ~
        new CatalogRoutes(repo).routes ~
        new ReviewRoutes(repo, decide).routes ~
        new DocsRoutes().routes

    HttpServer.bind(routes, cfg.http.host, cfg.http.port).onComplete {
      case Success(binding: ServerBinding) =>
        HttpServer.wireShutdown(binding, readiness)
        readiness.set(true)
        log.info(
          "ariadne {} bound HTTP :{} — readiness UP",
          BuildInfo.version,
          Integer.valueOf(binding.localAddress.getPort)
        )
      case Failure(ex) =>
        log.error(
          s"Failed to bind HTTP ${cfg.http.host}:${cfg.http.port} — ${ex.getMessage}",
          ex
        )
        system.terminate()
        System.exit(1)
    }

  /**
   * Wire the scraping policy (§2.3) to the real world — HTTP, the archive, the resolver, and the
   * price entities — and put it on a clock.
   *
   * Disabled by default, and nothing below is constructed when it is off: with no sources
   * configured there is no fetcher, no rate limiter and no timer. A service that merely *could*
   * scrape should not hold a client pointed at a third-party endpoint.
   */
  private def startScraping(cfg: ScrapeConfig, repo: ReadModelRepository)(using
      system: ActorSystem[?],
      timeout: Timeout
  ): Unit =
    import system.executionContext
    if !cfg.enabled then log.info("Scraping disabled (ariadne.scrape.enabled=false)")
    else if cfg.sources.isEmpty then
      // Enabled with nothing to scrape is almost certainly a config mistake, and the
      // symptom otherwise is silence — indistinguishable from a working scraper that
      // found nothing.
      log.warn("Scraping enabled but ariadne.scrape.sources is empty — nothing will be scraped")
    else
      val cf = connectionFactory(system.settings.config)
      val policy = HttpPolicyConfig()
      val fetcher = new PoliteFetcher(
        new PekkoTransport(system),
        policy,
        RateLimiter(policy.rateLimit, policy.rateWindow, system),
        system
      )
      val resolution = new ResolutionService(new Resolver(repo), system)

      val run = new ScrapeRun(
        fetcher = fetcher,
        archive = new PostgresRawArchive(cf),
        ledger = new PostgresFlyerLedger(cf),
        resolve = (subject, scraper) =>
          resolution.resolveForScrape(subject, scraper, CorrelationId(UUID.randomUUID().toString)),
        observe = observePrice
      )

      ScrapeScheduler.init(cfg.sources, run.run, cfg.interval, cfg.initialDelay)
      log.info(
        "Scraping enabled for {} source(s): {}",
        Integer.valueOf(cfg.sources.size),
        cfg.sources.map(_.name).mkString(", ")
      )

  /**
   * Append one scraped price to the product x scope stream (§2.3.1).
   *
   * `PriceSource.Scrape` demands the archived response id, so the fact and the bytes it was read
   * out of stay linked all the way to the journal — the observation is re-derivable if the decoder
   * later turns out to have been wrong.
   *
   * A domain refusal — same price, same source, same day — is a NO-OP, not a failure. The scraper
   * re-reads a flyer whose window has not closed and must be free to; a run that errored on its own
   * idempotency would fail every time it worked correctly.
   */
  private def observePrice(
      productId: me.cference.ariadne.domain.ProductId,
      obs: FlippMapper.Observation
  )(using system: ActorSystem[?], timeout: Timeout): scala.concurrent.Future[Unit] =
    import system.executionContext
    val now = java.time.Instant.now()
    Sharding
      .price(system, productId, obs.scope)
      .askWithStatus[Done](
        PriceStreamEntity.Execute(
          PriceCommand.ObservePrice(
            productId = productId,
            scope = obs.scope,
            price = obs.price,
            observedAt = obs.observedAt,
            source = PriceSource.Scrape(obs.provenance.scraper, obs.provenance.rawResponseId),
            unitPrice = None,
            promo = None,
            priceConfidence = obs.priceConfidence,
            sizeConfidence = obs.sizeConfidence,
            correlationId = CorrelationId(UUID.randomUUID().toString)
          ),
          now,
          _
        )
      )
      .map(_ => ())
      .recover { case _: org.apache.pekko.pattern.StatusReply.ErrorMessage => () }

  /** The read side shares the journal's connection settings — one database, one config block. */
  private def connectionFactory(raw: com.typesafe.config.Config): PostgresqlConnectionFactory =
    val c = raw.getConfig("pekko.persistence.r2dbc.connection-factory")
    PostgresqlConnectionFactory(
      PostgresqlConnectionConfiguration
        .builder()
        .host(c.getString("host"))
        .port(c.getInt("port"))
        .database(c.getString("database"))
        .username(c.getString("user"))
        .password(c.getString("password"))
        .build()
    )

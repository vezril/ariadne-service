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
import me.cference.ariadne.domain.resolution.{ResolutionCommand, ResolutionId}
import me.cference.ariadne.persistence.{ResolutionCaseEntity, SchemaMigration, Sharding}
import me.cference.ariadne.projection.{AriadneProjections, ReadModelRepository}
import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import org.apache.pekko.Done
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.slf4j.LoggerFactory

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

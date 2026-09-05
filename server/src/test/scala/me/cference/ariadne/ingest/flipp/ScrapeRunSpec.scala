package me.cference.ariadne.ingest.flipp

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.{ChainId, PriceScope, ProductId}
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.ingest.PostgresRawArchive
import me.cference.ariadne.ingest.http.*
import me.cference.ariadne.projection.PostgresFixture
import me.cference.ariadne.text.Locale
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
 * The run end to end, against a real Postgres and a stubbed network.
 *
 * Every layer below has its own tests; what this covers is the ORDER — archive before parse,
 * re-stamp before trusting a merchant, resolve before attributing a price. Each is a place where a
 * shortcut yields a run that looks entirely successful and leaves the corpus wrong.
 */
final class ScrapeRunSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with PostgresFixture {

  private given scala.concurrent.ExecutionContext = system.executionContext

  // Sub-microsecond nanos on purpose: Postgres stores microseconds, and the ledger's
  // comparison bug was invisible on macOS precisely because the bare clock hides it.
  private val now = Instant.now().truncatedTo(ChronoUnit.MICROS).plusNanos(311)

  private lazy val cf = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new PostgresqlConnectionFactory(cfg)
  }
  private lazy val archive = new PostgresRawArchive(cf)
  private lazy val ledger = new PostgresFlyerLedger(cf)

  override def beforeAll(): Unit = { super.beforeAll(); applySchema() }
  override def afterAll(): Unit = { container.stop(); super.afterAll() }

  private val from = now.minusSeconds(3600).toString
  private val to = now.plusSeconds(86400).toString

  private def listingJson(flyerIds: Long*) = {
    val flyers = flyerIds.map(id =>
      s"""{"id":$id,"merchant_id":42,"merchant":"IGA","name":"Weekly","valid_from":"$from",
          |"valid_to":"$to","postal_code":"H2X 1Y4","locale":"en-ca"}""".stripMargin
    )
    s"""{"flyers":[${flyers.mkString(",")}]}"""
  }

  /** The per-flyer shape: `id`, `price` as a string, and NO merchant. */
  private def itemsJson(prices: String*) = {
    val items = prices.zipWithIndex.map { case (p, i) =>
      s"""{"id":${i + 1},"flyer_id":9,"name":"Item $i","price":$p,"valid_from":"$from","valid_to":"$to"}"""
    }
    s"""{"items":[${items.mkString(",")}]}"""
  }

  private class StubTransport(responses: Map[String, String]) extends Transport {
    val requested = new AtomicReference[List[String]](Nil)
    def get(
        url: String,
        headers: Map[String, String],
        timeout: FiniteDuration
    ): Future[HttpOutcome] = {
      requested.updateAndGet(_ :+ url)
      val body = responses
        .collectFirst { case (k, v) if url.contains(k) => v }
        .getOrElse("""{"flyers":[]}""")
      Future.successful(HttpOutcome(200, body, "application/json"))
    }
  }

  private def runWith(
      transport: Transport,
      resolve: (MatchSubject, String) => Future[Option[ProductId]] = (_, _) =>
        Future.successful(Some(ProductId("p-1"))),
      observed: AtomicReference[List[(ProductId, FlippMapper.Observation)]] = new AtomicReference(
        Nil
      )
  ) = {
    val config = HttpPolicyConfig(backoffBase = 1.milli, rateLimit = 100, rateWindow = 1.milli)
    val fetcher =
      new PoliteFetcher(transport, config, RateLimiter(100, 1.milli, system), system, () => 0.0)
    val run = new ScrapeRun(
      fetcher,
      archive,
      ledger,
      resolve,
      (pid, obs) => { observed.updateAndGet(_ :+ (pid, obs)); Future.unit }
    )
    (run, observed)
  }

  private val source = ScrapeSource(
    "flipp",
    "https://flipp.test",
    PostalCode.unsafe("H2X1Y4"),
    Locale.EnCa,
    Map(MerchantId(42) -> ChainId("iga"))
  )

  "a run" should {

    "archive before anything parses, and record the flyer in the ledger" in {
      val t =
        new StubTransport(Map("/flyers?" -> listingJson(9), "/flyers/9" -> itemsJson("\"4.99\"")))
      val (run, observed) = runWith(t)
      val report = run.run(source, "run-basic", now).futureValue

      report.flyersListed shouldBe 1
      report.flyersFetched shouldBe 1
      report.observationsAppended shouldBe 1
      // Two archived responses: the listing and the flyer's items. Both kept before parse.
      archive.replay("run-basic").futureValue should have size 2
      ledger.entriesFor(List(FlyerId(9))).futureValue should have size 1
      observed.get().head._2.scope shouldBe PriceScope.Regional(
        ChainId("iga"),
        me.cference.ariadne.domain.Area("H2X")
      )
    }

    "cite the ITEMS response it read the price from, not the listing that led to it" in {
      // A flyer id of its own: the ledger is real and shared across this suite, so
      // reusing another test's id would make this one silently fetch nothing.
      val t = new StubTransport(
        Map("/flyers?" -> listingJson(91), "/flyers/91" -> itemsJson("\"4.99\""))
      )
      val (run, observed) = runWith(t)
      run.run(source, "run-prov", now).futureValue

      // Both responses are archived, in order: the flyer listing, then that flyer's
      // items. The price was read out of the SECOND, and that is the one a
      // PriceSource.Scrape has to name — citing the listing would point a re-derivation
      // at bytes the price does not appear in, which is worse than citing nothing
      // because it looks like provenance.
      val archived = archive.replay("run-prov").futureValue
      archived should have size 2
      val itemsResponse = archived.last

      val provenance = observed.get().head._2.provenance
      provenance.scraper shouldBe "flipp"
      provenance.rawResponseId shouldBe itemsResponse.id
      archive.get(provenance.rawResponseId).futureValue.map(_.kind) shouldBe Some("flyer_items")
    }

    "record a REGIONAL scope, never an exact one — the feed has no franchise" in {
      val t =
        new StubTransport(Map("/flyers?" -> listingJson(11), "/flyers/11" -> itemsJson("\"1.99\"")))
      val (run, observed) = runWith(t)
      run.run(source, "run-scope", now).futureValue
      all(observed.get().map(_._2.scope.isExact)) shouldBe false
    }

    "SKIP the second run's fetch — the ledger is what makes this affordable" in {
      // Quirk #2. If this ever stops holding, the run is ~9x the load on a bot-walled
      // upstream while reporting complete success.
      val t =
        new StubTransport(Map("/flyers?" -> listingJson(21), "/flyers/21" -> itemsJson("\"2.99\"")))
      val (run, _) = runWith(t)
      run.run(source, "run-first", now).futureValue.flyersSelected shouldBe 1
      val second = run.run(source, "run-second", now).futureValue
      second.flyersSelected shouldBe 0
      second.flyersFetched shouldBe 0
      second.selectionRatio shouldBe Some(0.0)
    }

    "count a zero price under ITS OWN reason, not as a decode failure" in {
      // A real advertised 0.00 (carrier handsets). The number is true; using it as a
      // price would be false. Folding it into a decode-drop total would make that
      // counter mean two different things.
      val t = new StubTransport(
        Map("/flyers?" -> listingJson(31), "/flyers/31" -> itemsJson("\"0.00\"", "\"3.99\""))
      )
      val (run, observed) = runWith(t)
      val report = run.run(source, "run-zero", now).futureValue

      report.skipped.get(SkipReason.ZeroPriced) shouldBe Some(1)
      report.itemsDroppedByDecoder shouldBe 0 // it decoded perfectly; we refused it
      report.observationsAppended shouldBe 1
      observed.get() should have size 1
    }

    "count a priceless item separately again — it is normal, not a failure" in {
      val t = new StubTransport(
        Map("/flyers?" -> listingJson(41), "/flyers/41" -> itemsJson("\"\"", "\"5.99\""))
      )
      val (run, _) = runWith(t)
      val report = run.run(source, "run-nopr", now).futureValue
      report.skipped.get(SkipReason.NoPrice) shouldBe Some(1)
      report.skipped.get(SkipReason.ZeroPriced) shouldBe None
      report.observationsAppended shouldBe 1
    }

    "PARK an unresolved item rather than attributing its price to a guess" in {
      val t =
        new StubTransport(Map("/flyers?" -> listingJson(51), "/flyers/51" -> itemsJson("\"6.99\"")))
      val (run, observed) = runWith(t, resolve = (_, _) => Future.successful(None))
      val report = run.run(source, "run-parked", now).futureValue
      report.skipped.get(SkipReason.ParkedForReview) shouldBe Some(1)
      report.observationsAppended shouldBe 0
      observed.get() shouldBe empty
    }

    "skip a flyer whose merchant has no chain mapping rather than guessing one" in {
      val unmapped = source.copy(chains = Map.empty)
      val t =
        new StubTransport(Map("/flyers?" -> listingJson(61), "/flyers/61" -> itemsJson("\"7.99\"")))
      val (run, observed) = runWith(t)
      val report = run.run(unmapped, "run-unmapped", now).futureValue
      report.observationsAppended shouldBe 0
      observed.get() shouldBe empty
      // Still recorded as fetched: the bytes were archived and the flyer was seen.
      ledger.entriesFor(List(FlyerId(61))).futureValue should have size 1
    }

    "report a bot wall as a failure without retrying into it" in {
      val walled = new Transport {
        def get(url: String, h: Map[String, String], t: FiniteDuration): Future[HttpOutcome] =
          Future.successful(HttpOutcome(403, "", "text/html"))
      }
      val (run, _) = runWith(walled)
      val report = run.run(source, "run-walled", now).futureValue
      report.failures.map(_.getClass.getSimpleName) shouldBe List("BotWall")
      report.flyersFetched shouldBe 0
    }
  }
}

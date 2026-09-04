package me.cference.ariadne.ingest.flipp

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.ingest.{PostgresRawArchive, RawResponse}
import me.cference.ariadne.projection.PostgresFixture
import me.cference.ariadne.text.Locale
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*

/**
 * Quirk #2 — the ledger is what makes the pipeline affordable against a bot-walled upstream:
 * roughly 18 of 164 listed flyers on a typical day, against ~9x the load without it.
 */
final class FlyerLedgerSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with PostgresFixture {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Millis))

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
  private lazy val ledger = new PostgresFlyerLedger(cf)
  private lazy val archive = new PostgresRawArchive(cf)

  override def beforeAll(): Unit = applySchema()
  override def afterAll(): Unit = container.stop()

  // Clock-relative, never literal — demeter's DailyRunSpec went red at midnight on a
  // hardcoded window, on a docs-only PR, taking five tests with it.
  private val now = Instant.now()

  private def flyer(
      id: Long,
      from: Instant = now.minusSeconds(3600),
      to: Instant = now.plusSeconds(86400)
  ) =
    Flyer(FlyerId(id), MerchantId(42), "Weekly", from, to, PostalCode.unsafe("H2X1Y4"), Locale.EnCa)

  private def archived(): Long =
    archive
      .archive(
        RawResponse(
          "run",
          "flipp",
          "flyer_items",
          "u",
          None,
          None,
          now,
          "application/json",
          "{}".getBytes
        )
      )
      .futureValue
      .id

  "needsFetch — the pure selection rule" should {

    "fetch a flyer never fetched" in {
      FlyerLedger.needsFetch(flyer(1), None, now, 7.days) shouldBe true
    }

    "SKIP a flyer already fetched with the same window" in {
      // This is the entire saving. Without it every listed flyer is fetched daily.
      val entry = LedgerEntry(FlyerId(1), now.minusSeconds(3600), now.plusSeconds(86400), now)
      FlyerLedger.needsFetch(flyer(1), Some(entry), now, 7.days) shouldBe false
    }

    "re-fetch when the validity window CHANGED — a re-issued flyer is new content" in {
      val entry = LedgerEntry(FlyerId(1), now.minusSeconds(99999), now.minusSeconds(1), now)
      FlyerLedger.needsFetch(flyer(1), Some(entry), now, 7.days) shouldBe true
    }

    "re-fetch when the recorded fetch is older than maxAge" in {
      val entry = LedgerEntry(
        FlyerId(1),
        now.minusSeconds(3600),
        now.plusSeconds(86400),
        now.minusSeconds(8.days.toSeconds)
      )
      FlyerLedger.needsFetch(flyer(1), Some(entry), now, 7.days) shouldBe true
    }
  }

  "the ledger against Postgres" should {

    "select only the flyers that need fetching" in {
      val a = flyer(101)
      val b = flyer(102)
      ledger
        .selectToFetch(List(a, b), now)
        .futureValue
        .map(_.id) should contain theSameElementsAs List(a.id, b.id)

      ledger.markFetched(a.id, a.validFrom, a.validTo, archived(), now).futureValue
      ledger.selectToFetch(List(a, b), now).futureValue.map(_.id) shouldBe List(b.id)
    }

    "UPDATE the row when a flyer is re-issued, never insert a second one" in {
      // Demeter phrase the key as (flyer_id, window_from, window_to). Read literally as a
      // composite PK, a re-issue would add a row and the selection lookup would have two
      // entries with no rule for choosing. Their table is flyer_id PRIMARY KEY with the
      // window as compared columns, and this test pins that difference.
      val f = flyer(103)
      ledger.markFetched(f.id, f.validFrom, f.validTo, archived(), now).futureValue

      val reissued = flyer(103, from = now.plusSeconds(86400), to = now.plusSeconds(172800))
      ledger
        .markFetched(reissued.id, reissued.validFrom, reissued.validTo, archived(), now)
        .futureValue

      val entries = ledger.entriesFor(List(FlyerId(103))).futureValue
      entries should have size 1
      entries(FlyerId(103)).windowFrom shouldBe reissued.validFrom
    }

    "re-select a flyer whose window moved, using the stored window" in {
      val f = flyer(104)
      ledger.markFetched(f.id, f.validFrom, f.validTo, archived(), now).futureValue
      ledger.selectToFetch(List(f), now).futureValue shouldBe Nil

      val reissued = flyer(104, from = now.plusSeconds(86400), to = now.plusSeconds(172800))
      ledger.selectToFetch(List(reissued), now).futureValue.map(_.id) shouldBe List(FlyerId(104))
    }

    "return nothing for an empty listing without hitting the database" in {
      ledger.entriesFor(Nil).futureValue shouldBe empty
    }
  }
}

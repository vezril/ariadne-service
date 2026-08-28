package me.cference.ariadne.projection

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.projection.ReadModelRepository.{CurrentPriceRow, PriceHistoryRow}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * The read models against a REAL Postgres. The SQL is the risky part — the CHECK constraints, the
 * conflict targets, and above all the exact-over-area resolution of §2.3.1 — and none of it is
 * exercised by a mock.
 */
final class ReadModelRepositorySpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with PostgresFixture {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  private lazy val repo = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new ReadModelRepository(new PostgresqlConnectionFactory(cfg))
  }

  override def beforeAll(): Unit = applySchema()
  override def afterAll(): Unit = container.stop()

  private val now = Instant.parse("2026-08-26T12:00:00Z")

  private def history(
      scopeKind: String,
      storeId: Option[String],
      chain: Option[String],
      area: Option[String],
      pid: String,
      seq: Long
  ) =
    PriceHistoryRow(
      "p-1",
      scopeKind,
      storeId,
      chain,
      area,
      now,
      BigDecimal("4.99"),
      "CAD",
      None,
      None,
      None,
      None,
      1.0,
      1.0,
      "Scrape",
      Some("c-1"),
      pid,
      seq
    )

  private def current(
      scopeKey: String,
      scopeKind: String,
      storeId: Option[String],
      chain: Option[String],
      area: Option[String],
      amount: String,
      at: Instant
  ) =
    CurrentPriceRow(
      "p-1",
      scopeKey,
      scopeKind,
      storeId,
      chain,
      area,
      BigDecimal(amount),
      "CAD",
      None,
      at,
      "Scrape",
      1.0
    )

  "the product read model" should {

    "upsert idempotently and follow merge redirects to the canonical id" in {
      repo.upsertProduct("p-a", "Butter", Some("Lactantia"), None, None, "Active", None).futureValue
      repo
        .upsertProduct("p-a", "Butter Salted", Some("Lactantia"), None, None, "Active", None)
        .futureValue

      // p-b is merged into p-a: a lookup on the dead id must still resolve, forever.
      repo
        .upsertProduct("p-b", "Butter dup", None, None, None, "MergedInto", Some("p-a"))
        .futureValue
      repo.resolveCanonical("p-b").futureValue shouldBe Some("p-a")
      repo.resolveCanonical("p-a").futureValue shouldBe Some("p-a")
    }

    "follow a redirect CHAIN, not just one hop" in {
      // Merges compose: c -> b -> a. Following only one hop would strand callers
      // holding the oldest id.
      repo.upsertProduct("m-a", "Canonical", None, None, None, "Active", None).futureValue
      repo.upsertProduct("m-b", "Loser 1", None, None, None, "MergedInto", Some("m-a")).futureValue
      repo.upsertProduct("m-c", "Loser 2", None, None, None, "MergedInto", Some("m-b")).futureValue
      repo.resolveCanonical("m-c").futureValue shouldBe Some("m-a")
    }
  }

  "price history" should {
    "accept both scope kinds and treat re-delivery as a no-op" in {
      repo.appendPriceHistory(history("exact", Some("s-1"), None, None, "pid-1", 1)).futureValue
      repo
        .appendPriceHistory(history("area", None, Some("iga"), Some("H2X"), "pid-2", 1))
        .futureValue
      // At-least-once: the same envelope arrives again after a restart.
      repo.appendPriceHistory(history("exact", Some("s-1"), None, None, "pid-1", 1)).futureValue
      countHistory("p-1") shouldBe 2
    }
  }

  "current price resolution (§2.3.1)" should {

    "fall back to the AREA price when the store has no exact observation" in {
      repo.upsertStore("s-10", "IGA Plateau", "iga", "H2X", None, active = true).futureValue
      repo
        .upsertCurrentPrice(
          current("area:iga:H2X", "area", None, Some("iga"), Some("H2X"), "5.49", now)
        )
        .futureValue

      val r = repo.currentPriceForStore("p-1", "s-10").futureValue.getOrElse(fail("should resolve"))
      r.amount.compareTo(new java.math.BigDecimal("5.49")) shouldBe 0
      r.isExact shouldBe false // and it SAYS so — a flyer claim, not a receipt
    }

    "PREFER the exact price over the area price, even when the area one is newer" in {
      // This is the whole point. A receipt is a better fact than a flyer, so recency
      // must not override provenance — otherwise every flyer scrape would bury the
      // price actually paid.
      repo.upsertStore("s-11", "IGA Mile End", "iga", "H2T", None, active = true).futureValue
      repo
        .upsertCurrentPrice(current("store:s-11", "exact", Some("s-11"), None, None, "3.99", now))
        .futureValue
      repo
        .upsertCurrentPrice(
          current(
            "area:iga:H2T",
            "area",
            None,
            Some("iga"),
            Some("H2T"),
            "5.49",
            now.plusSeconds(3600)
          )
        )
        .futureValue

      val r = repo.currentPriceForStore("p-1", "s-11").futureValue.getOrElse(fail("should resolve"))
      r.amount.compareTo(new java.math.BigDecimal("3.99")) shouldBe 0
      r.isExact shouldBe true
    }

    "not leak one chain's area price to another chain's store in the same region" in {
      // Same postal area, different banner. Metro's shelf price is not IGA's.
      repo.upsertStore("s-12", "Metro Plateau", "metro", "H2X", None, active = true).futureValue
      repo.currentPriceForStore("p-1", "s-12").futureValue shouldBe None
    }

    "price a NEWLY registered franchise immediately, with no backfill" in {
      // The reason resolution is a query rather than materialised per-store rows:
      // a store registered after the flyer was scraped is priced the moment it exists.
      repo
        .upsertCurrentPrice(
          current("area:sobeys:H4A", "area", None, Some("sobeys"), Some("H4A"), "6.29", now)
        )
        .futureValue
      repo.currentPriceForStore("p-1", "s-new").futureValue shouldBe None // not registered yet

      repo.upsertStore("s-new", "Sobeys NDG", "sobeys", "H4A", None, active = true).futureValue
      val r =
        repo.currentPriceForStore("p-1", "s-new").futureValue.getOrElse(fail("should resolve now"))
      r.amount.compareTo(new java.math.BigDecimal("6.29")) shouldBe 0
    }

    "not let an out-of-order backfill overwrite a newer price" in {
      // Backfill replays ORIGINAL timestamps, so older facts arrive after newer ones.
      repo.upsertStore("s-13", "IGA Verdun", "iga", "H4G", None, active = true).futureValue
      repo
        .upsertCurrentPrice(current("store:s-13", "exact", Some("s-13"), None, None, "4.00", now))
        .futureValue
      repo
        .upsertCurrentPrice(
          current("store:s-13", "exact", Some("s-13"), None, None, "9.99", now.minusSeconds(86400))
        )
        .futureValue

      val r = repo.currentPriceForStore("p-1", "s-13").futureValue.getOrElse(fail("should resolve"))
      r.amount.compareTo(new java.math.BigDecimal("4.00")) shouldBe 0
    }
  }

  private def countHistory(productId: String): Int =
    scala.util.Using.resource(connection()) { c =>
      val rs = c
        .createStatement()
        .executeQuery(s"SELECT count(*) FROM price_history WHERE product_id = '$productId'")
      rs.next()
      rs.getInt(1)
    }
}

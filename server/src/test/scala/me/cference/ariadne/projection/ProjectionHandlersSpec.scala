package me.cference.ariadne.projection

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.store.StoreEvent
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Using

/** Events folded into the read models, end to end, against a real Postgres. */
final class ProjectionHandlersSpec
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
  private val money = Money.unsafe(BigDecimal("4.99"))

  private def onProduct(pid: String, e: ProductEvent) =
    ProjectionHandlers.product(repo)(s"product|$pid", e).futureValue
  private def onStore(sid: String, e: StoreEvent) =
    ProjectionHandlers.store(repo)(s"store|$sid", e).futureValue
  private def onPrice(pid: String, seq: Long, e: PriceEvent) =
    ProjectionHandlers.price(repo)(pid, seq, e).futureValue

  private def scalar(sql: String): Option[String] =
    Using.resource(connection()) { c =>
      val rs = c.createStatement().executeQuery(sql)
      if rs.next() then Option(rs.getString(1)) else None
    }

  "the product projection" should {

    "register a product and index its GTIN" in {
      onProduct(
        "p-1",
        ProductEvent.ProductRegistered(
          ProductId("p-1"),
          "Lactantia Butter",
          Some("Lactantia"),
          Some("dairy"),
          Some(Quantity.unsafe(BigDecimal(454), MeasureUnit.Gram)),
          Some(Gtin.unsafe("4006381333931")),
          Origin.Manual,
          ProductStatus.Active
        )
      )
      scalar("SELECT name FROM products WHERE id = 'p-1'") shouldBe Some("Lactantia Butter")
      scalar("SELECT product_id FROM product_gtins WHERE gtin = '04006381333931'") shouldBe Some(
        "p-1"
      )
    }

    "record a link with its method AND matcher version" in {
      // Without matcher_version a resolver change silently orphans links (§6.6).
      onProduct(
        "p-1",
        ProductEvent.ListingLinked(
          ListingKey(StoreId("s-1"), "ext-9"),
          Confidence.unsafe(0.93),
          MatchMethod.Fuzzy,
          MatcherVersion("v1")
        )
      )
      scalar(
        "SELECT matcher FROM product_listings WHERE store_id='s-1' AND external_id='ext-9'"
      ) shouldBe Some("v1")
      scalar(
        "SELECT method FROM product_listings WHERE store_id='s-1' AND external_id='ext-9'"
      ) shouldBe Some("Fuzzy")
    }

    "turn a merged product into a forwarding tombstone rather than deleting it" in {
      onProduct(
        "p-2",
        ProductEvent.ProductRegistered(
          ProductId("p-2"),
          "Dup",
          None,
          None,
          None,
          None,
          Origin.Manual,
          ProductStatus.Active
        )
      )
      onProduct("p-2", ProductEvent.ProductMerged(ProductId("p-1")))

      // The row SURVIVES — Dionysus and Demeter hold ids we do not control.
      scalar("SELECT status FROM products WHERE id = 'p-2'") shouldBe Some("MergedInto")
      repo.resolveCanonical("p-2").futureValue shouldBe Some("p-1")
    }

    "move the loser's GTINs onto the winner when absorbed" in {
      val moved = Gtin.unsafe("036000291452")
      onProduct(
        "p-1",
        ProductEvent.ProductAbsorbed(ProductId("p-2"), Set(moved), Set("beurre"), Set.empty)
      )
      scalar(s"SELECT product_id FROM product_gtins WHERE gtin = '${moved.value}'") shouldBe Some(
        "p-1"
      )
      scalar(
        "SELECT alias FROM product_aliases WHERE product_id='p-1' AND alias='beurre'"
      ) shouldBe Some("beurre")
    }
  }

  "the store projection" should {

    "register a franchise and make it immediately coverable" in {
      onStore(
        "s-1",
        StoreEvent.StoreRegistered(
          StoreId("s-1"),
          "IGA Plateau",
          ChainId("iga"),
          Area("H2X"),
          Some("Plateau")
        )
      )
      scalar("SELECT chain_id FROM store_coverage WHERE store_id = 's-1'") shouldBe Some("iga")
    }

    "apply a PARTIAL update without blanking untouched fields" in {
      onStore("s-1", StoreEvent.StoreDetailsUpdated(Some("IGA Plateau Est"), None, None))
      scalar("SELECT name FROM stores WHERE id = 's-1'") shouldBe Some("IGA Plateau Est")
      scalar("SELECT area FROM stores WHERE id = 's-1'") shouldBe Some("H2X") // untouched
      scalar("SELECT label FROM stores WHERE id = 's-1'") shouldBe Some("Plateau") // untouched
    }

    "move coverage when the area changes, so the fan-out follows the store" in {
      onStore("s-1", StoreEvent.StoreDetailsUpdated(None, Some(Area("H2T")), None))
      scalar("SELECT area FROM store_coverage WHERE store_id = 's-1'") shouldBe Some("H2T")
    }

    "stop covering a deactivated store but KEEP its row" in {
      // Its price and purchase history still reference it; deleting would orphan facts.
      onStore("s-1", StoreEvent.StoreDeactivated)
      scalar("SELECT active FROM stores WHERE id = 's-1'") shouldBe Some("f")
      scalar("SELECT store_id FROM store_coverage WHERE store_id = 's-1'") shouldBe None
    }
  }

  "the price projection" should {

    "write history and current price for a regional observation" in {
      onStore(
        "s-20",
        StoreEvent.StoreRegistered(StoreId("s-20"), "IGA NDG", ChainId("iga"), Area("H4A"), None)
      )
      onPrice(
        "price|p-1|area:iga:H4A",
        1,
        PriceEvent.PriceObserved(
          ProductId("p-1"),
          PriceScope.Regional(ChainId("iga"), Area("H4A")),
          money,
          None,
          None,
          Confidence.Certain,
          Confidence.unsafe(0.4),
          now,
          PriceSource.Scrape("flipp")
        )
      )
      scalar(
        "SELECT scope_kind FROM price_history WHERE persistence_id = 'price|p-1|area:iga:H4A'"
      ) shouldBe Some("area")
      // sizeConfidence is contract, not decoration — it must reach the read model intact.
      scalar(
        "SELECT size_confidence FROM price_history WHERE persistence_id = 'price|p-1|area:iga:H4A'"
      ) shouldBe Some("0.4")

      val resolved =
        repo.currentPriceForStore("p-1", "s-20").futureValue.getOrElse(fail("should resolve"))
      resolved.isExact shouldBe false
    }

    "let an exact observation take precedence for that one store" in {
      onPrice(
        "price|p-1|store:s-20",
        1,
        PriceEvent.PriceObserved(
          ProductId("p-1"),
          PriceScope.Exact(StoreId("s-20")),
          Money.unsafe(BigDecimal("3.49")),
          None,
          None,
          Confidence.Certain,
          Confidence.Certain,
          now,
          PriceSource.Purchase(PurchaseId("pu-1"))
        )
      )
      val resolved =
        repo.currentPriceForStore("p-1", "s-20").futureValue.getOrElse(fail("should resolve"))
      resolved.isExact shouldBe true
      resolved.amount.compareTo(new java.math.BigDecimal("3.49")) shouldBe 0
      resolved.source shouldBe "Purchase"
    }

    "be safe to re-apply — at-least-once delivery means it WILL be" in {
      val e = PriceEvent.PriceObserved(
        ProductId("p-9"),
        PriceScope.Exact(StoreId("s-20")),
        money,
        None,
        None,
        Confidence.Certain,
        Confidence.Certain,
        now,
        PriceSource.Manual
      )
      onPrice("price|p-9|store:s-20", 5, e)
      onPrice("price|p-9|store:s-20", 5, e)
      scalar("SELECT count(*) FROM price_history WHERE product_id = 'p-9'") shouldBe Some("1")
    }
  }
}

package me.cference.ariadne.hermes

import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.*

import java.time.Instant

/**
 * The wire shape Demeter will build against.
 *
 * These assertions are the contract until the Lexicon agrees one, so they pin the fields whose
 * absence or wrong encoding would be silently wrong downstream rather than loudly broken.
 */
final class CatalogMessagesSpec extends AnyWordSpec with Matchers {

  private val topics = CatalogMessages.Topics()
  private val at = Instant.parse("2026-09-07T12:00:00Z")

  private def payloadOf(o: CatalogMessages.Outgoing): JsObject = o.payload.parseJson.asJsObject

  private def observed(scope: PriceScope, sizeConfidence: Confidence = Confidence.Certain) =
    PriceEvent.PriceObserved(
      ProductId("p-1"),
      scope,
      Money.unsafe(BigDecimal("4.99")),
      None,
      None,
      Confidence.Certain,
      sizeConfidence,
      at,
      PriceSource.Scrape("flipp", 77L)
    )

  private def priceMsg(scope: PriceScope, sc: Confidence = Confidence.Certain) =
    CatalogMessages.price(topics, "price|p-1", 1L, "", observed(scope, sc)).get

  "the message id" should {
    "be the journal's own coordinates, so a republish is recognisable" in {
      // At-least-once means the same event can publish twice; the broker collapses the
      // duplicate on this key. A minted id would make each retry a NEW message.
      CatalogMessages.messageId("price|p-1|area:iga:H2X", 4L) shouldBe "price|p-1|area:iga:H2X:4"
    }
  }

  "a price observation" should {

    "carry a TAGGED scope, so a flyer claim cannot be read as a store price" in {
      // §2.3.1. An area price speaks for every franchise of that chain in the region.
      // Read as one store's price it under-counts coverage; expanded to N stores it
      // fabricates N facts. Demeter cannot alert correctly without knowing which it has.
      payloadOf(priceMsg(PriceScope.Regional(ChainId("iga"), Area("H2X")))).fields("scope") shouldBe
        JsObject(
          "kind" -> JsString("area"),
          "chainId" -> JsString("iga"),
          "area" -> JsString("H2X")
        )

      payloadOf(priceMsg(PriceScope.Exact(StoreId("s-1")))).fields("scope") shouldBe
        JsObject("kind" -> JsString("exact"), "storeId" -> JsString("s-1"))
    }

    "carry sizeConfidence — it is contract, not decoration" in {
      // §2.3: Demeter's match confidence is the weakest link of its split confidence
      // and this one. Omitted, downstream judgment silently reads too high.
      val p = payloadOf(priceMsg(PriceScope.Exact(StoreId("s-1")), Confidence.unsafe(0.4)))
      p.fields("sizeConfidence") shouldBe JsNumber(0.4)
      p.fields("priceConfidence") shouldBe JsNumber(1.0)
    }

    "encode money as a decimal STRING, never a float" in {
      // 4.99 has no exact binary representation. A consumer comparing prices for a deal
      // would be comparing rounding artefacts.
      payloadOf(priceMsg(PriceScope.Exact(StoreId("s-1")))).fields("price") shouldBe
        JsObject("amount" -> JsString("4.99"), "currency" -> JsString("CAD"))
    }

    "name the archived response a scraped price came from" in {
      val source = payloadOf(priceMsg(PriceScope.Exact(StoreId("s-1")))).fields("source").asJsObject
      source.fields("kind") shouldBe JsString("scrape")
      source.fields("rawResponseId") shouldBe JsString("77")
    }

    "NOT publish a retraction" in {
      // The only consumer treats every message on this topic as a new price, so a
      // correction would arrive looking like an observation — worse than silence.
      CatalogMessages.price(
        topics,
        "price|p-1",
        2L,
        "",
        PriceEvent.PriceObservationRetracted(at, "wrong")
      ) shouldBe None
    }

    "go to the price topic" in {
      priceMsg(PriceScope.Exact(StoreId("s-1"))).topic shouldBe "product.price.observed"
    }
  }

  "a product registration" should {

    "publish with its status and origin" in {
      val e = ProductEvent.ProductRegistered(
        ProductId("p-1"),
        "Lactantia Butter",
        Some("Lactantia"),
        Some("dairy"),
        Some(Quantity.unsafe(BigDecimal(454), MeasureUnit.Gram)),
        Some(Gtin.unsafe("4006381333931")),
        Origin.Scrape("flipp", None),
        ProductStatus.Provisional
      )
      val out = CatalogMessages.product(topics, "product|p-1", 1L, "", e).get
      out.topic shouldBe "product.registered"
      val p = payloadOf(out)
      p.fields("status") shouldBe JsString("PROVISIONAL")
      p.fields("origin") shouldBe JsString("SCRAPE")
      p.fields("size") shouldBe JsObject(
        "amount" -> JsString("454"),
        "unit" -> JsString("Gram")
      )
    }

    "publish a merge as a re-point on the SAME topic" in {
      // §5: a consumer holding a product id learns to follow it. Whether this overloads
      // the topic is §10.1, open with the Lexicon and deliberately not decided here.
      val out = CatalogMessages
        .product(topics, "product|p-old", 3L, "", ProductEvent.ProductMerged(ProductId("p-new")))
        .get
      out.topic shouldBe "product.registered"
      val p = payloadOf(out)
      p.fields("productId") shouldBe JsString("p-old")
      p.fields("status") shouldBe JsString("MERGED_INTO")
      p.fields("mergedInto") shouldBe JsString("p-new")
    }

    "stay SILENT on internal bookkeeping" in {
      // Publishing everything the journal holds would make this a change-data-capture
      // feed rather than a contract. No consumer has asked for alias churn.
      CatalogMessages.product(
        topics,
        "product|p-1",
        2L,
        "",
        ProductEvent.ProductAliasAdded("beurre")
      ) shouldBe None
      CatalogMessages.product(
        topics,
        "product|p-1",
        3L,
        "",
        ProductEvent.ProductIdentifierAdded(Gtin.unsafe("4006381333931"))
      ) shouldBe None
    }
  }
}

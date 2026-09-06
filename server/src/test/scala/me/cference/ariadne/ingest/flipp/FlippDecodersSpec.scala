package me.cference.ariadne.ingest.flipp

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

/**
 * The decoders, ported from demeter-service. Each test here corresponds to a failure that actually
 * happened against live Flipp — these are not hypotheticals, and they are the reason the code was
 * ported rather than rewritten.
 *
 * Fixtures are built relative to the clock, never to literal dates: demeter's DailyRunSpec went red
 * at midnight on a hardcoded window, on a docs-only PR, taking five tests with it.
 */
final class FlippDecodersSpec extends AnyWordSpec with Matchers {

  private val now = Instant.now()
  private val from = now.minusSeconds(86400).toString
  private val to = now.plusSeconds(86400).toString

  private def parse(json: String) =
    FlippDecoders.parseJson("test", json.getBytes("UTF-8")).getOrElse(fail("fixture is not JSON"))

  // The per-flyer shape: `id`, `price` as a STRING, `discount` as an int, and NO merchant.
  private def perFlyerItem(price: String, name: String = "Salted Butter 454 g") =
    s"""{"id":1,"flyer_id":9,"name":"$name","price":$price,"valid_from":"$from","valid_to":"$to"}"""

  "priceValue" should {

    "read a numeric string, a number, and null alike" in {
      val items = s"""{"items":[${perFlyerItem("\"4.99\"")}]}"""
      FlippDecoders
        .decodeItems("test", parse(items))
        .map(_.items.head.currentPrice.map(_.cents)) shouldBe
        Right(Some(499L))

      val numeric = s"""{"items":[${perFlyerItem("4.99")}]}"""
      FlippDecoders
        .decodeItems("test", parse(numeric))
        .map(_.items.head.currentPrice.map(_.cents)) shouldBe
        Right(Some(499L))

      val nulled = s"""{"items":[${perFlyerItem("null")}]}"""
      FlippDecoders
        .decodeItems("test", parse(nulled))
        .map(_.items.head.currentPrice) shouldBe Right(None)
    }

    "treat an EMPTY STRING price as no price, not as a bad one" in {
      // The single highest-value line in the port. Treating "" as a bad price dropped
      // 142 of 485 items in one real flyer — ~14% of everything the first live run
      // fetched. The offer lives in the name or artwork instead.
      val items = s"""{"items":[${perFlyerItem("\"\"")}, ${perFlyerItem("\"  \"")}]}"""
      FlippDecoders.decodeItems("test", parse(items)) match {
        case Right(p) =>
          p.items should have size 2
          p.dropped shouldBe 0
          all(p.items.map(_.currentPrice)) shouldBe None
        case other => fail(s"empty-string prices must not drop items, got $other")
      }
    }

    "reject a non-numeric, non-null price at the ITEM level with its pointer" in {
      val items = s"""{"items":[${perFlyerItem("\"free-ish\"")}]}"""
      FlippDecoders.decodeItems("test", parse(items)).map(_.dropped) shouldBe Right(1)
    }

    "permit a zero price, as the feed's own model does" in {
      // FlippMoney allows zero; Ariadne's domain Money does not. That disagreement is
      // resolved at the mapper, deliberately and visibly, NOT by silently dropping the
      // item in the decoder.
      val items = s"""{"items":[${perFlyerItem("\"0.00\"")}]}"""
      FlippDecoders
        .decodeItems("test", parse(items))
        .map(_.items.head.currentPrice.map(_.cents)) shouldBe
        Right(Some(0L))
    }
  }

  "decodeItem" should {

    "decode the PER-FLYER shape, which has no merchant" in {
      // The spec's field table described the search shape, and requiring `merchant_id`
      // dropped every item of every real per-flyer response.
      val items = s"""{"items":[${perFlyerItem("\"4.99\"")}]}"""
      FlippDecoders.decodeItems("test", parse(items)) match {
        case Right(p) =>
          p.items.head.merchantId shouldBe FlippDecoders.UnresolvedMerchant
          p.items.head.sourceItemId shouldBe "1"
        case other => fail(s"unexpected: $other")
      }
    }

    "decode the SEARCH shape, which carries flyer_item_id and merchant_id" in {
      val item =
        s"""{"flyer_item_id":77,"flyer_id":9,"merchant_id":42,"name":"Butter",
            |"current_price":"4.99","valid_from":"$from","valid_to":"$to"}""".stripMargin
      FlippDecoders.decodeItems("test", parse(s"""{"items":[$item]}""")) match {
        case Right(p) =>
          p.items.head.sourceItemId shouldBe "77"
          p.items.head.merchantId shouldBe MerchantId(42)
        case other => fail(s"unexpected: $other")
      }
    }

    "reject an inverted validity window" in {
      val item =
        s"""{"id":1,"flyer_id":9,"name":"Butter","price":"1.00","valid_from":"$to","valid_to":"$from"}"""
      FlippDecoders
        .decodeItems("test", parse(s"""{"items":[$item]}"""))
        .map(_.dropped) shouldBe Right(1)
    }

    "keep `discount` as OPAQUE sale text, never as a percentage" in {
      // Its units are undocumented and were not verifiable from the response. Reading it
      // as a percentage would fabricate an original price and silently corrupt every
      // discount-gated judgment downstream.
      val item =
        s"""{"id":1,"flyer_id":9,"name":"Butter","price":"4.99","discount":25,
            |"valid_from":"$from","valid_to":"$to"}""".stripMargin
      FlippDecoders.decodeItems("test", parse(s"""{"items":[$item]}""")) match {
        case Right(p) =>
          p.items.head.saleStory shouldBe Some("discount 25")
          p.items.head.originalPrice shouldBe None // NOT fabricated from the discount
        case other => fail(s"unexpected: $other")
      }
    }

    "leave the bilingual name unsplit — that is normalisation's job" in {
      val items = s"""{"items":[${perFlyerItem("\"4.99\"", "Beurre salé | Salted butter")}]}"""
      FlippDecoders.decodeItems("test", parse(items)) match {
        case Right(p) =>
          p.items.head.rawName shouldBe "Beurre salé | Salted butter"
          p.items.head.name shouldBe me.cference.ariadne.text.BilingualText.empty
        case other => fail(s"unexpected: $other")
      }
    }
  }

  "the envelopes" should {

    "DROP AND COUNT bad items rather than failing the whole response" in {
      // One malformed item must not cost a whole flyer's worth of prices.
      val good = perFlyerItem("\"4.99\"")
      val bad =
        s"""{"id":2,"flyer_id":9,"price":"1.00","valid_from":"$from","valid_to":"$to"}""" // no name
      FlippDecoders.decodeItems("test", parse(s"""{"items":[$good,$bad,$good]}""")) match {
        case Right(p) =>
          p.items should have size 2
          p.dropped shouldBe 1
        case other => fail(s"unexpected: $other")
      }
    }

    "fail the response when the array itself is missing" in {
      FlippDecoders.decodeItems("test", parse("""{"pages":[]}""")).isLeft shouldBe true
    }

    "decode a flyers listing, and drop a flyer with a bad postal code" in {
      val ok =
        s"""{"id":9,"merchant_id":42,"merchant":"IGA","name":"Weekly","valid_from":"$from",
            |"valid_to":"$to","postal_code":"H2X 1Y4","locale":"en-ca"}""".stripMargin
      val bad =
        s"""{"id":10,"merchant_id":42,"merchant":"IGA","name":"Weekly","valid_from":"$from",
            |"valid_to":"$to","postal_code":"nope","locale":"en-ca"}""".stripMargin
      FlippDecoders.decodeListing("test", parse(s"""{"flyers":[$ok,$bad]}""")) match {
        case Right(p) =>
          p.flyers should have size 1
          p.dropped shouldBe 1
          p.flyers.head.postalCode.canonical shouldBe "H2X1Y4"
          p.merchants shouldBe List(Merchant(MerchantId(42), "IGA"))
        case other => fail(s"unexpected: $other")
      }
    }

    "ignore unknown fields — Flipp adds them without notice" in {
      val item =
        s"""{"id":1,"flyer_id":9,"name":"Butter","price":"4.99","valid_from":"$from",
            |"valid_to":"$to","some_new_field":{"nested":true}}""".stripMargin
      FlippDecoders
        .decodeItems("test", parse(s"""{"items":[$item]}"""))
        .map(_.items.size) shouldBe Right(1)
    }
  }

  "the merchant re-stamp (quirk #1)" should {

    "replace the unresolved sentinel with the flyer's merchant" in {
      // Miss this and every product collides on merchant 0, with correct row counts and
      // a corrupt corpus.
      val items = s"""{"items":[${perFlyerItem("\"4.99\"")}]}"""
      val decoded =
        FlippDecoders.decodeItems("test", parse(items)).getOrElse(fail("should decode")).items
      MerchantRestamp.hasUnresolved(decoded) shouldBe true

      val flyer = Flyer(
        FlyerId(9),
        MerchantId(42),
        "Weekly",
        now,
        now.plusSeconds(3600),
        PostalCode.unsafe("H2X1Y4"),
        me.cference.ariadne.text.Locale.EnCa
      )
      val stamped = MerchantRestamp(flyer, decoded)
      stamped.map(_.merchantId) shouldBe List(MerchantId(42))
      MerchantRestamp.hasUnresolved(stamped) shouldBe false
    }
  }
}

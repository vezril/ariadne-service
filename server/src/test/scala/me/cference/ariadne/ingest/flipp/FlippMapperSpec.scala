package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.domain.{Area, ChainId, Confidence, PriceScope}
import me.cference.ariadne.text.BilingualText
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

/**
 * The wire/domain boundary, tested for the cases where the two models disagree.
 *
 * Every refusal here is an item the feed considers perfectly fine. That is the point: this is the
 * layer that decides what Flipp's idea of an item is allowed to become a price FACT, and each
 * refusal is counted under its own reason so a category never hides inside a failure total.
 */
final class FlippMapperSpec extends AnyWordSpec with Matchers with OptionValues {

  private val chain = ChainId("iga")
  private val postal = PostalCode.unsafe("H2X1Y4")
  private val provenance = FlippMapper.Provenance("flipp", 77L)
  private val at = Instant.parse("2026-09-05T00:00:00Z")

  private def item(name: String, price: Option[Long]) =
    FlyerItem(
      sourceItemId = "i-1",
      flyerId = FlyerId(9),
      merchantId = MerchantId(42),
      name = BilingualText(None, None),
      rawName = name,
      currentPrice = price.map(c => FlippMoney.cents(c)),
      originalPrice = None,
      saleStory = None,
      validFrom = at,
      validTo = at.plusSeconds(86400)
    )

  private def map(i: FlyerItem) =
    FlippMapper.map(i, chain, postal, provenance, Confidence.Certain, Confidence.Certain)

  "a well-formed item" should {

    "become a REGIONAL observation carrying its provenance" in {
      val obs = map(item("Lactantia Butter 454g", Some(499))).toOption.value
      obs.price.amount shouldBe BigDecimal("4.99")
      // Never Exact: the feed carries no franchise at all (§2.3.1), so attributing one
      // would fabricate precision that was never observed.
      obs.scope shouldBe PriceScope.Regional(chain, Area("H2X"))
      obs.provenance shouldBe provenance
      obs.subject.name shouldBe "Lactantia Butter 454g"
    }
  }

  "an item the feed accepts but the domain will not" should {

    "refuse a genuine 0.00 under its OWN reason" in {
      // 3 rows in 35,088 measured: carrier handsets on postpaid contracts. The number
      // is true and using it as a price would be false.
      map(item("Phone on contract", Some(0))) shouldBe Left(SkipReason.ZeroPriced)
    }

    "count a priceless item separately — it is normal, not breakage" in {
      map(item("Butter", None)) shouldBe Left(SkipReason.NoPrice)
    }

    "refuse a nameless item BEFORE looking at its price" in {
      // A blank name is not resolvable by anything downstream: the matcher scores text.
      // Left unchecked it mints one provisional product that every nameless item in the
      // corpus falls into — which reads as a real product with a very busy price
      // history. Checked first, because the price is irrelevant to whether it can be
      // identified.
      map(item("   ", Some(499))) shouldBe Left(SkipReason.Nameless)
      map(item("", None)) shouldBe Left(SkipReason.Nameless)
    }
  }
}

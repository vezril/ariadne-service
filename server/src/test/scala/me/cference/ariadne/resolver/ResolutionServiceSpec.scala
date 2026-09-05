package me.cference.ariadne.resolver

import me.cference.ariadne.domain.{Gtin, ListingKey, StoreId}
import me.cference.ariadne.domain.resolution.MatchSubject
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The case id is what stops the review queue filling with one row per scrape of the same unresolved
 * listing, so it is worth pinning independently of the actor plumbing.
 */
final class ResolutionCaseIdSpec extends AnyWordSpec with Matchers {

  // The REAL implementation, not a copy of it — a duplicated derivation would keep
  // passing after the implementation changed.
  private def idFor(s: MatchSubject) = ResolutionService.caseIdFor(s).value

  "the case id" should {

    "be STABLE for the same subject, so a repeat scrape reuses one case" in {
      val s = MatchSubject(
        "Lactantia Butter",
        Some("Lactantia"),
        listing = Some(ListingKey(StoreId("s-1"), "ext-9"))
      )
      idFor(s) shouldBe idFor(s)
    }

    "key on the listing when there is one — that is the thing being resolved" in {
      val a = MatchSubject(
        "Lactantia Butter",
        Some("Lactantia"),
        listing = Some(ListingKey(StoreId("s-1"), "ext-9"))
      )
      // A later scrape of the SAME listing, with the retailer's name text changed.
      val b = MatchSubject(
        "Lactantia Butter Salted 454g",
        None,
        listing = Some(ListingKey(StoreId("s-1"), "ext-9"))
      )
      idFor(a) shouldBe idFor(b)
    }

    "distinguish different listings" in {
      val a = MatchSubject("Butter", listing = Some(ListingKey(StoreId("s-1"), "ext-9")))
      val b = MatchSubject("Butter", listing = Some(ListingKey(StoreId("s-1"), "ext-8")))
      idFor(a) should not be idFor(b)
    }

    "fall back to the GTIN, then to name+brand" in {
      val byGtin = MatchSubject("anything", gtin = Some(Gtin.unsafe("4006381333931")))
      idFor(byGtin) shouldBe idFor(
        MatchSubject("different text", gtin = Some(Gtin.unsafe("4006381333931")))
      )

      val byName = MatchSubject("  Lactantia Butter  ", Some("Lactantia"))
      idFor(byName) shouldBe idFor(MatchSubject("lactantia butter", Some("Lactantia")))
    }

    "not collide across subjects that are genuinely different" in {
      val ids = List(
        MatchSubject("Butter", Some("Lactantia")),
        MatchSubject("Butter", Some("Gay Lea")),
        MatchSubject("Milk", Some("Lactantia")),
        MatchSubject("x", gtin = Some(Gtin.unsafe("4006381333931"))),
        MatchSubject("y", listing = Some(ListingKey(StoreId("s-1"), "e-1")))
      ).map(idFor)
      ids.distinct should have size ids.size
    }
  }
}

/**
 * The provisional product id is the other derived key in this file, and the more dangerous one.
 *
 * The case id only affects how tidy the review queue is. This one decides whether a scrape that
 * meets an unknown product creates ONE provisional or a new product every time it runs — and the
 * failure is silent, because a catalog full of near-duplicate provisionals looks exactly like a
 * catalog that has seen a lot of products.
 */
final class ProvisionalIdSpec extends AnyWordSpec with Matchers {

  private def idFor(s: MatchSubject) = ResolutionService.provisionalIdFor(s).value

  "the provisional id" should {

    "be stable across runs of the same subject" in {
      val s = MatchSubject("Lactantia Salted Butter 454g", Some("Lactantia"))
      idFor(s) shouldBe idFor(s)
    }

    "survive the formatting drift a retailer's display text actually has" in {
      // The same item as the flyer renders it on two different days. Nothing here is a
      // different product, and a key that said otherwise would mint a second one.
      idFor(MatchSubject("Lactantia  Salted   Butter", Some("Lactantia"))) shouldBe
        idFor(MatchSubject("LACTANTIA SALTED BUTTER!", Some("lactantia")))
    }

    "prefer the GTIN, which is identity rather than description" in {
      val gtin = Some(Gtin.unsafe("4006381333931"))
      idFor(MatchSubject("Butter", gtin = gtin)) shouldBe
        idFor(MatchSubject("Beurre salé 454 g", Some("Lactantia"), gtin = gtin))
    }

    "keep genuinely different products apart" in {
      idFor(MatchSubject("Lactantia Butter", Some("Lactantia"))) should not be
        idFor(MatchSubject("Gay Lea Butter", Some("Gay Lea")))
    }

    "be recognisable as provisional in the journal" in {
      // Operators read persistence ids. A provisional that looks like any other product
      // id makes "how much of the catalog is auto-created?" an unanswerable question.
      idFor(MatchSubject("Butter")) should startWith("prov-")
    }
  }
}

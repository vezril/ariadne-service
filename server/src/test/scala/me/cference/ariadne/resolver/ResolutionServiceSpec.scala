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

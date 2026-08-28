package me.cference.ariadne.matching

import me.cference.ariadne.domain.{MeasureUnit, Quantity}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The fuzzy fallback of §6.3 — the code that decides whether two listings are the same product when
 * no GTIN says so. Auto-link happens at >= 0.92, so what these tests really pin is which pairs are
 * allowed anywhere near that line.
 */
final class ScorerSpec extends AnyFunSuite with Matchers {

  private def subj(name: String, brand: Option[String] = None) = MatchInput.from(name, brand)
  private def score(a: MatchInput, b: MatchInput, c: MatchConfig = MatchConfig()) =
    Scorer.score(a, b, c).confidence.toDouble

  test("identical names score at the top") {
    score(subj("Lactantia Butter 454 g"), subj("Lactantia Butter 454 g")) shouldBe 1.0
  }

  test("the same product written differently still scores high") {
    val a = subj("Lactantia Salted Butter 454g", Some("Lactantia"))
    val b = subj("Lactantia Butter, Salted 454 g", Some("Lactantia"))
    score(a, b) should be > 0.85
  }

  test("SIZE CONFLICT is a strong negative, not a small deduction") {
    // §6.7: a 454 g and a 250 g of the same brand are DIFFERENT products. If this
    // scored as a near miss, the resolver would happily auto-link two pack sizes
    // and every unit price downstream would be wrong.
    val a = subj("Lactantia Butter 454 g", Some("Lactantia"))
    val b = subj("Lactantia Butter 250 g", Some("Lactantia"))
    val s = Scorer.score(a, b)
    s.confidence.toDouble should be < 0.60 // below even the review band
    s.notes should contain("size conflict")
  }

  test("incomparable dimensions are treated as a conflict, not as unknown") {
    val a = subj("Cream 200 ml")
    val b = subj("Cheese 200 g")
    Scorer.score(a, b).notes should contain("size conflict")
  }

  test("BRAND CONFLICT between two known brands is a strong negative") {
    // Two butters of the same size from different brands are not the same product,
    // however similar the words are.
    val a = subj("Butter 454 g", Some("Lactantia"))
    val b = subj("Butter 454 g", Some("Gay Lea"))
    val s = Scorer.score(a, b)
    s.confidence.toDouble should be < 0.60
    s.notes should contain("brand conflict")
  }

  test("an UNKNOWN brand is not evidence against a match") {
    // Absent signals must drop out of the weighting rather than score zero —
    // otherwise every unbranded listing is permanently unmatchable.
    val known = subj("Salted Butter 454 g", Some("Lactantia"))
    val unknown = subj("Salted Butter 454 g", None)
    val s = Scorer.score(known, unknown)
    s.brand shouldBe None
    s.confidence.toDouble should be > 0.90
  }

  test("the minFuzzyLength guard: short names get token credit only") {
    // Demeter's production lesson — 14 of 83 butter alerts were false positives that
    // scored identically to the wanted match. Length separates them where no
    // threshold could.
    // Demeter's actual case: minFuzzyLength = 7 needs BOTH sides long enough, so
    // "cookie" (6) cannot fuzzy-match "cookies" (7) — which is what stopped
    // "Cedar Brand Butter Cookies" firing on a butter watch.
    val a = subj("cookie")
    val b = subj("cookies")
    val guarded = Scorer.score(a, b, MatchConfig())
    val unguarded = Scorer.score(a, b, MatchConfig(minFuzzyLength = 0))
    guarded.confidence.toDouble should be < unguarded.confidence.toDouble
    guarded.notes should contain("fuzzy suppressed: below minFuzzyLength")
  }

  test("the guard does NOT punish long names that legitimately fuzz") {
    // At 7, `yogourt` still matches — that is the point of the threshold's value.
    val a = subj("Yogourt Grec Vanille")
    val b = subj("Yogourt Grec Vanille 2%")
    Scorer.score(a, b).notes should not contain "fuzzy suppressed: below minFuzzyLength"
    score(a, b) should be > 0.80
  }

  test("size is removed from the name before comparing") {
    // Otherwise two unrelated 454 g products share tokens for the wrong reason.
    val input = MatchInput.from("Lactantia Butter 454 g", Some("Lactantia"))
    input.tokens should not contain "454"
    input.size shouldBe Some(Quantity.unsafe(BigDecimal(454), MeasureUnit.Gram))
  }

  test("the brand is not counted twice when it also appears in the name") {
    val input = MatchInput.from("Lactantia Butter 454 g", Some("Lactantia"))
    input.tokens should not contain "lactantia"
    input.brand shouldBe Some("lactantia")
  }

  test("unrelated products score low enough to never auto-link") {
    val a = subj("Fresh Atlantic Salmon Fillet 340 g")
    val b = subj("Lactantia Salted Butter 454 g", Some("Lactantia"))
    score(a, b) should be < 0.60
  }

  test("every score carries its breakdown, so a link is always explainable") {
    // §6.5: "why is this listing on this product" must be answerable and reversible.
    val s =
      Scorer.score(subj("Butter 454 g", Some("Lactantia")), subj("Butter 250 g", Some("Gay Lea")))
    s.name should be >= 0.0
    s.brand shouldBe Some(0.0)
    s.size shouldBe Some(0.0)
    s.notes should contain allOf ("brand conflict", "size conflict")
  }

  test("scoring is symmetric") {
    val a = subj("Lactantia Salted Butter 454 g", Some("Lactantia"))
    val b = subj("Butter Salted 454g", Some("Lactantia"))
    score(a, b) shouldBe score(b, a)
  }
}

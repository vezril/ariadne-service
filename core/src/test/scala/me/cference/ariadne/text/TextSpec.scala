package me.cference.ariadne.text

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TextSpec extends AnyFunSuite with Matchers {

  import TextNormalizer.normalize

  test("accents and ligatures fold, so bœuf and boeuf are the same token") {
    // Unicode gives œ/æ no decomposition, so without the explicit fold "bœuf"
    // never matches "boeuf" and a whole class of French names silently misses.
    normalize("Bœuf haché").tokens shouldBe List("boeuf", "hache")
    normalize("boeuf hache").tokens shouldBe List("boeuf", "hache")
    normalize("Crème Glacée").tokens shouldBe List("creme", "glacee")
  }

  test("digit/letter boundaries split, so 4L and 4 L normalise alike") {
    // Otherwise the same pack size reads as two different products.
    normalize("4L").tokens shouldBe List("4", "l")
    normalize("4 L").tokens shouldBe List("4", "l")
    normalize("750ml").tokens shouldBe normalize("750 mL").tokens
  }

  test("normalisation is idempotent — running it twice changes nothing") {
    val once = normalize("Lactantia Beurre Salé 454 g")
    normalize(once.joined) shouldBe once
  }

  test("stopwords drop in both languages, and punctuation becomes separation") {
    normalize("Bread & Butter").tokens shouldBe List("bread", "butter")
    normalize("Fromage de chevre").tokens shouldBe List("fromage", "chevre")
  }

  test("elision leaves a bare one-letter token — known, inherited, NOT fixed here") {
    // "d'orange" -> ["d", "orange"]: the apostrophe becomes a separator and bare
    // "d" is not in the stopword list (which has de/du/des). This is Demeter's
    // tuned behaviour and changing the shared normaliser unilaterally is exactly
    // what DESIGN §10.5 forbids — the whole point of one owned copy is that its
    // tuning does not drift per-consumer.
    //
    // Pinned here so the behaviour is deliberate rather than accidental. The
    // scorer compensates instead: one-character tokens carry almost no weight in
    // token-set overlap, and the minimum-length guard stops them fuzzing at all.
    normalize("Jus d'orange").tokens shouldBe List("jus", "d", "orange")
  }

  test("a French-only name is detected with High confidence") {
    val r = BilingualSplitter.splitBilingual("Beurre salé")
    r.text.fr shouldBe Some("Beurre salé")
    r.text.en shouldBe None
    r.confidence shouldBe SplitConfidence.High
  }

  test("a jammed FR | EN pair splits into both languages") {
    val r = BilingualSplitter.splitBilingual("Beurre salé | Salted butter")
    r.text.fr shouldBe Some("Beurre salé")
    r.text.en shouldBe Some("Salted butter")
    r.confidence shouldBe SplitConfidence.High
  }

  test("an EN | FR pair is assigned correctly, not by position") {
    // The Quebec convention is FR first, but the feed does not honour it reliably,
    // so detection must beat position or half the pairs land backwards.
    val r = BilingualSplitter.splitBilingual("Salted butter | Beurre salé")
    r.text.en shouldBe Some("Salted butter")
    r.text.fr shouldBe Some("Beurre salé")
  }

  test("an undetectable name degrades to BOTH forms rather than guessing") {
    // Safe for matching: a name offered in both languages can still match, whereas
    // a confidently wrong split cannot. Honesty beats a coin flip.
    val r = BilingualSplitter.splitBilingual("Zorblex 9000")
    r.text.fr shouldBe Some("Zorblex 9000")
    r.text.en shouldBe Some("Zorblex 9000")
    r.confidence shouldBe SplitConfidence.Low
    r.text.forms should have size 1 // deduplicated
  }

  test("an unspaced dash is a hyphenated word, NOT a language split") {
    // "Coca-Cola" must not become two language forms.
    val r = BilingualSplitter.splitBilingual("Coca-Cola")
    r.text.forms should have size 1
    r.text.anyForm.map(_.contains("-")) shouldBe Some(true)
  }

  test("three or more segments admits it is out of its depth") {
    val r = BilingualSplitter.splitBilingual("Lait | Milk | 2%")
    r.confidence shouldBe SplitConfidence.Low
  }

  test("the raw string is never mutated — only partitioned") {
    val raw = "Beurre salé | Salted butter"
    val r = BilingualSplitter.splitBilingual(raw)
    r.text.forms.foreach(f => raw should include(f))
  }

  test("SplitConfidence.min takes the weakest link and is not the domain Confidence") {
    SplitConfidence.High.min(SplitConfidence.Low) shouldBe SplitConfidence.Low
    SplitConfidence.Medium.min(SplitConfidence.High) shouldBe SplitConfidence.Medium
  }
}

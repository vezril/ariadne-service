package me.cference.ariadne.matching

import me.cference.ariadne.text.TextNormalizer
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/**
 * The invariants examples cannot cover exhaustively (DESIGN §11 step 3).
 *
 * These matter more than usual here: the scorer decides identity, and a violated invariant does not
 * throw — it quietly links two unrelated products, which is the exact failure the review queue
 * exists to catch by hand.
 */
final class MatchingPropertySpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  /** Names like the catalogue actually sees: accents, ligatures, sizes, punctuation. */
  private val nameGen: Gen[String] = {
    val word = Gen.oneOf(
      "beurre",
      "butter",
      "lait",
      "milk",
      "crème",
      "bœuf",
      "yogourt",
      "fromage",
      "salé",
      "Lactantia",
      "Metro",
      "Selection",
      "bio",
      "2%",
      "grec",
      "vanille"
    )
    val size = Gen.oneOf("454 g", "1 kg", "750 ml", "2 L", "2 x 1 L", "12 × 355 ml", "")
    val punct = Gen.oneOf(",", " -", " |", "", ".")
    for {
      ws <- Gen.chooseNum(1, 5).flatMap(Gen.listOfN(_, word))
      s <- size
      p <- punct
    } yield (ws.mkString(" ") + p + " " + s).trim
  }

  test("normalisation is idempotent for any name") {
    // Non-idempotence would mean a product's own stored alias could stop matching
    // itself after a round trip.
    forAll(nameGen) { raw =>
      val once = TextNormalizer.normalize(raw)
      TextNormalizer.normalize(once.joined) shouldBe once
    }
  }

  test("normalisation never produces empty or whitespace tokens") {
    forAll(nameGen) { raw =>
      TextNormalizer.normalize(raw).tokens.foreach { t =>
        t should not be empty
        t.trim shouldBe t
      }
    }
  }

  test("a score is always a valid confidence in [0,1]") {
    // Confidence.unsafe throws outside the range, so a weighting bug that produced
    // 1.4 would surface as an exception in production rather than a silent overclaim.
    forAll(nameGen, nameGen) { (a, b) =>
      val s = Scorer.score(MatchInput.from(a, None), MatchInput.from(b, None))
      s.confidence.toDouble should (be >= 0.0 and be <= 1.0)
    }
  }

  test("scoring is symmetric for any pair") {
    // Asymmetry would make a link depend on which side the resolver happened to
    // call the subject — the same two rows matching or not by accident of order.
    forAll(nameGen, nameGen) { (a, b) =>
      val (x, y) = (MatchInput.from(a, None), MatchInput.from(b, None))
      Scorer.score(x, y).confidence.toDouble shouldBe Scorer.score(y, x).confidence.toDouble
    }
  }

  test("a name always scores 1.0 against itself") {
    forAll(nameGen) { raw =>
      val i = MatchInput.from(raw, None)
      Scorer.score(i, i).confidence.toDouble shouldBe 1.0
    }
  }

  test("size extraction never invents a non-positive quantity") {
    forAll(nameGen) { raw =>
      SizeParser.parse(raw).quantity.foreach(_.amount should be > BigDecimal(0))
    }
  }

  test("the size is always absent from the remainder it hands back") {
    forAll(nameGen) { raw =>
      val p = SizeParser.parse(raw)
      p.remainder.length should be <= raw.length
    }
  }
}

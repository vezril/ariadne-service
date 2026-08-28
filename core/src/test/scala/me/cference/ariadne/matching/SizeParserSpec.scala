package me.cference.ariadne.matching

import me.cference.ariadne.domain.{MeasureUnit, Quantity}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class SizeParserSpec extends AnyFunSuite with Matchers {

  private def q(a: String, u: MeasureUnit) = Some(Quantity.unsafe(BigDecimal(a), u))

  test("parses the common shelf formats") {
    SizeParser.parse("Lactantia Butter 454 g").quantity shouldBe q("454", MeasureUnit.Gram)
    SizeParser.parse("Coca-Cola 2L").quantity shouldBe q("2", MeasureUnit.Litre)
    SizeParser.parse("Yogourt 750ml").quantity shouldBe q("750", MeasureUnit.Millilitre)
  }

  test("comma decimals parse — French listings write 1,5 L") {
    SizeParser.parse("Jus d'orange 1,5 L").quantity shouldBe q("1.5", MeasureUnit.Litre)
  }

  test("a multipack's size is the TOTAL, not the unit size") {
    // "2 x 1 L" also matches the simple pattern as "1 L". Taking that would halve
    // the pack and make the unit price look twice as good as it is.
    SizeParser.parse("Lait 2 x 1 L").quantity shouldBe q("2", MeasureUnit.Litre)
    SizeParser.parse("Canette 12 × 355 ml").quantity shouldBe q("4260", MeasureUnit.Millilitre)
  }

  test("the LAST size wins — names lead with brand and trail with size") {
    // "Pepsi 355 ml" inside a promo string like "2 for 5 355 ml" must not read the
    // leading promo number as a pack size.
    SizeParser.parse("Selection 500 g Cheddar 200 g").quantity shouldBe q("200", MeasureUnit.Gram)
  }

  test("the size is REMOVED from the remainder") {
    // Leaving it in lets two unrelated 454 g products look alike for the wrong reason.
    val p = SizeParser.parse("Lactantia Butter 454 g")
    p.remainder shouldBe "Lactantia Butter"
    p.remainder should not include "454"
  }

  test("a name with no size yields None and an untouched remainder") {
    val p = SizeParser.parse("Fresh Atlantic Salmon")
    p.quantity shouldBe None
    p.remainder shouldBe "Fresh Atlantic Salmon"
  }

  test("a bare number without a unit is not a size") {
    SizeParser.parse("Pack of 12").quantity should not be q("12", MeasureUnit.Gram)
    SizeParser.parse("Product 2000").quantity shouldBe None
  }

  test("unit aliases normalise to the same Quantity") {
    val forms = List("1 kg", "1kg", "1 kilo")
    forms.map(f => SizeParser.parse(s"Rice $f").quantity).distinct should have size 1
  }

  test("mass and volume stay in their own dimensions") {
    val g = SizeParser.parse("Cheese 200 g").quantity.getOrElse(fail("should parse"))
    val ml = SizeParser.parse("Cream 200 ml").quantity.getOrElse(fail("should parse"))
    g.isCloseTo(ml).isLeft shouldBe true // incomparable, not merely unequal
  }
}

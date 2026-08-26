package me.cference.ariadne.domain

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class MoneyAndQuantitySpec extends AnyFunSuite with Matchers {

  test("Money refuses zero and negative amounts") {
    // A non-positive price is not a fact, it is a parse failure — storing it
    // would poison price history with something no shelf ever showed.
    Money.cad(BigDecimal(0)).isLeft shouldBe true
    Money.cad(BigDecimal("-1.99")).isLeft shouldBe true
    Money.cad(BigDecimal("0.01")).isRight shouldBe true
  }

  test("Money rounds to two places once, at construction") {
    Money.cad(BigDecimal("1.005")).map(_.amount) shouldBe Right(BigDecimal("1.01"))
    Money.cad(BigDecimal("2.344")).map(_.amount) shouldBe Right(BigDecimal("2.34"))
  }

  test("Money addition refuses a currency mismatch rather than guessing a rate") {
    val cad = Money.unsafe(BigDecimal("5.00"), Currency.CAD)
    val usd = Money.unsafe(BigDecimal("5.00"), Currency.USD)
    (cad + usd).isLeft shouldBe true
    (cad + cad).map(_.amount) shouldBe Right(BigDecimal("10.00"))
  }

  test("Money.sum totals a list and reports None for empty") {
    val lines = List(Money.unsafe(BigDecimal("1.25")), Money.unsafe(BigDecimal("2.50")))
    Money.sum(lines).map(_.map(_.amount)) shouldBe Right(Some(BigDecimal("3.75")))
    Money.sum(Nil) shouldBe Right(None)
  }

  test("Quantity refuses non-positive sizes") {
    Quantity(BigDecimal(0), MeasureUnit.Gram).isLeft shouldBe true
    Quantity(BigDecimal(-5), MeasureUnit.Litre).isLeft shouldBe true
  }

  test("Quantity compares across units within a dimension") {
    val sevenFiftyMl = Quantity.unsafe(BigDecimal(750), MeasureUnit.Millilitre)
    val pointSevenFiveL = Quantity.unsafe(BigDecimal("0.75"), MeasureUnit.Litre)
    sevenFiftyMl.isCloseTo(pointSevenFiveL) shouldBe Right(true)
  }

  test("750 mL and 4 L are the same dimension but NOT close — a negative match signal") {
    // §6.3: incompatible size is a strong negative. It must read as "not close",
    // not as an error, because both are volumes and the scorer can compare them.
    val small = Quantity.unsafe(BigDecimal(750), MeasureUnit.Millilitre)
    val big = Quantity.unsafe(BigDecimal(4), MeasureUnit.Litre)
    small.isCloseTo(big) shouldBe Right(false)
  }

  test("mass and volume are INCOMPARABLE, not merely unequal") {
    // The distinction matters: the scorer treats "different size" and
    // "cannot be compared" differently, so this must not collapse to false.
    val kg = Quantity.unsafe(BigDecimal(1), MeasureUnit.Kilogram)
    val l = Quantity.unsafe(BigDecimal(1), MeasureUnit.Litre)
    kg.isCloseTo(l).isLeft shouldBe true
  }

  test("UnitPrice normalises mass per 100 g") {
    // $4.99 for 250 g -> $1.996 per 100 g
    val up = UnitPrice.from(
      Money.unsafe(BigDecimal("4.99")),
      Quantity.unsafe(BigDecimal(250), MeasureUnit.Gram)
    )
    up.amount shouldBe BigDecimal("1.9960")
    up.per.unit shouldBe MeasureUnit.Gram
  }

  test("UnitPrice normalises volume per litre, making pack sizes comparable") {
    // $3.00 / 750 mL = $4.00 per L; $4.50 / 2 L = $2.25 per L — the 2 L is cheaper.
    val small = UnitPrice.from(
      Money.unsafe(BigDecimal("3.00")),
      Quantity.unsafe(BigDecimal(750), MeasureUnit.Millilitre)
    )
    val large = UnitPrice.from(
      Money.unsafe(BigDecimal("4.50")),
      Quantity.unsafe(BigDecimal(2), MeasureUnit.Litre)
    )
    small.amount shouldBe BigDecimal("4.0000")
    large.amount shouldBe BigDecimal("2.2500")
    (large.amount < small.amount) shouldBe true
  }

  test("Confidence is clamped to [0,1] and min takes the weakest link") {
    Confidence(1.5).isLeft shouldBe true
    Confidence(-0.1).isLeft shouldBe true
    Confidence(Double.NaN).isLeft shouldBe true
    // §2.3: an ambiguous size must drag the combined confidence DOWN, which is
    // exactly why sizeConfidence rides the contract to Demeter.
    Confidence.unsafe(0.9).min(Confidence.unsafe(0.4)).toDouble shouldBe 0.4
  }
}

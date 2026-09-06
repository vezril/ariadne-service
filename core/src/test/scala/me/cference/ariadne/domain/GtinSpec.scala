package me.cference.ariadne.domain

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * GTIN is the one key trusted for AUTOMATIC identity (§6.1), so a false accept here silently welds
 * two different products together. The check digit is the only thing standing between a typo and
 * that outcome.
 */
final class GtinSpec extends AnyFunSuite with Matchers {

  // Each verified by hand against the mod-10 weighting before being trusted here.
  private val validEan13 = "4006381333931"
  private val validUpcA = "036000291452"
  private val validGtin8 = "96385074"

  test("accepts a valid EAN-13 and normalises it to 14 digits") {
    Gtin.parse(validEan13).map(_.value) shouldBe Right("04006381333931")
  }

  test("accepts a valid UPC-A (12) and a valid GTIN-8") {
    Gtin.parse(validUpcA).map(_.value) shouldBe Right("00036000291452")
    Gtin.parse(validGtin8).map(_.value) shouldBe Right("00000096385074")
  }

  test("the same code at different lengths normalises to the same 14 digits") {
    // This is the whole point of normalising: one product scanned as UPC-A and
    // as a zero-padded EAN-13 must compare EQUAL, or the strong key is not strong.
    val asUpcA = Gtin.parse(validUpcA).map(_.value)
    val asEan13 = Gtin.parse("0" + validUpcA).map(_.value)
    asUpcA shouldBe asEan13
  }

  test("rejects a wrong check digit") {
    // Last digit bumped 1 -> 2; everything else identical.
    Gtin.parse("4006381333932").left.map(_.message) match {
      case Left(msg) => msg should include("check digit")
      case Right(g) => fail(s"expected rejection, got ${g.value}")
    }
  }

  test("rejects transposed digits, the typo the check digit exists to catch") {
    // 4006381333931 with two adjacent middle digits swapped.
    Gtin.parse("4006383133931").isLeft shouldBe true
  }

  test("rejects non-digits, blanks, and lengths that are not 8/12/13/14") {
    Gtin.parse("40063813339A1").isLeft shouldBe true
    Gtin.parse("").isLeft shouldBe true
    Gtin.parse("   ").isLeft shouldBe true
    Gtin.parse("400638133").isLeft shouldBe true // 9 digits
  }

  test("trims surrounding whitespace before validating") {
    Gtin.parse(s"  $validEan13  ").map(_.value) shouldBe Right("04006381333931")
  }

  test("check digit is whatever rounds the weighted sum up to a multiple of ten") {
    // Property: for any valid GTIN, recomputing the check over its data digits
    // reproduces the stated one.
    List(validEan13, validUpcA, validGtin8).foreach { raw =>
      val padded = Gtin.parse(raw).map(_.value).getOrElse(fail(s"$raw should parse"))
      Gtin.checkDigit(padded.dropRight(1)) shouldBe padded.last.asDigit
    }
  }
}

package me.cference.ariadne.migrate

import org.scalatest.LoneElement
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The export reader (`migration-demeter.md` step 2).
 *
 * These tests are mostly about REFUSING things. A migration reads a file once, and the failure that
 * costs is the quiet one: a renamed column read as empty strings, or a confidence that parsed as
 * 0.0. 21,680 rows of silently-empty `price_confidence` is a corpus that looks complete and is not,
 * and the verification step ("history counts match between the two stores") would then be comparing
 * against a number nobody chose.
 */
final class DemeterCsvSpec extends AnyWordSpec with Matchers with LoneElement {

  private val header = DemeterCsv.ObservationColumns.mkString(",")

  private def obsRow(
      productKey: String = "dk-1",
      merchantId: String = "2001",
      effectiveCents: String = "499",
      priceConfidence: String = "0.9",
      matchConfidence: String = "0.8",
      rawName: String = "LACTANTIA BUTTER 454G"
  ) = List(
    productKey,
    merchantId,
    "77",
    "2026-09-01T12:00:00Z",
    rawName,
    "Lactantia Butter",
    "Beurre Lactantia",
    effectiveCents,
    "unit",
    "599",
    "454",
    "g",
    "1",
    "110",
    "per_100g",
    "SAVE $1",
    "2026-09-01",
    "2026-09-07",
    priceConfidence,
    matchConfidence,
    "5"
  ).mkString(",")

  "the observation reader" should {

    "parse a well-formed export, preserving both confidences verbatim" in {
      val Right(parsed) = DemeterCsv.observations(s"$header\n${obsRow()}"): @unchecked
      parsed.ok shouldBe true
      val r = parsed.rows.loneElement
      r.productKey shouldBe "dk-1"
      r.merchantId shouldBe "2001"
      r.effectiveCents shouldBe 499L
      // Verbatim, not re-derived: these are Demeter's judgment and we cannot reconstruct them.
      r.priceConfidence shouldBe 0.9
      r.matchConfidence shouldBe 0.8
      r.rawResponseId shouldBe Some(5L)
    }

    "REFUSE a header that does not match the migration contract" in {
      // The failure this prevents: a renamed or reordered column read as a column of empty
      // strings. Nothing would error, and the corpus would look complete.
      val wrong = DemeterCsv.ObservationColumns
        .map {
          case "price_confidence" => "confidence"
          case other => other
        }
        .mkString(",")
      val result = DemeterCsv.observations(s"$wrong\n${obsRow()}")
      result.isLeft shouldBe true
      result.left.toOption.get should include("header does not match")
    }

    "treat Postgres NULL (\\N) as absent, and an empty field as absent" in {
      val withNulls = obsRow().split(',').toList.updated(2, "\\N").mkString(",")
      val Right(parsed) = DemeterCsv.observations(s"$header\n$withNulls"): @unchecked
      parsed.rows.loneElement.flyerId shouldBe None
    }

    "refuse a confidence outside [0,1] rather than clamp it" in {
      // Clamping would quietly invent judgment Demeter did not express.
      val Right(parsed) =
        DemeterCsv.observations(s"$header\n${obsRow(priceConfidence = "1.4")}"): @unchecked
      parsed.ok shouldBe false
      parsed.errors.loneElement.reason should include("outside [0,1]")
    }

    "refuse a missing required field rather than default it" in {
      val Right(parsed) =
        DemeterCsv.observations(s"$header\n${obsRow(effectiveCents = "\\N")}"): @unchecked
      parsed.ok shouldBe false
      parsed.errors.loneElement.reason should include("effective_cents is required")
    }

    "COLLECT every bad row, so a broken export is diagnosed in one pass" in {
      val csv = List(
        header,
        obsRow(productKey = "good-1"),
        obsRow(productKey = "bad-1", priceConfidence = "nope"),
        obsRow(productKey = "good-2"),
        obsRow(productKey = "bad-2", effectiveCents = "")
      ).mkString("\n")
      val Right(parsed) = DemeterCsv.observations(csv): @unchecked
      parsed.rows.map(_.productKey) shouldBe List("good-1", "good-2")
      parsed.errors.map(_.line) shouldBe List(3, 5)
      parsed.ok shouldBe false
    }

    "report the line number as the reader sees it, header included" in {
      val csv = s"$header\n${obsRow()}\n${obsRow(priceConfidence = "x")}"
      val Right(parsed) = DemeterCsv.observations(csv): @unchecked
      // Line 1 is the header, so the second data row is line 3 — the number someone will
      // use to open the file and look.
      parsed.errors.loneElement.line shouldBe 3
    }

    "flag a short row instead of padding it" in {
      val Right(parsed) = DemeterCsv.observations(s"$header\ndk-1,2001,77"): @unchecked
      parsed.errors.loneElement.reason should include("expected 21 fields, found 3")
    }
  }

  "the CSV splitter" should {

    "honour quoted commas, because a flyer product name contains anything" in {
      DemeterCsv.split("""a,"b,c",d""") shouldBe List(List("a", "b,c", "d"))
    }

    "honour doubled quotes" in {
      DemeterCsv.split("a,\"say \"\"hi\"\"\",c") shouldBe List(List("a", "say \"hi\"", "c"))
    }

    "honour a newline inside quotes rather than splitting the row" in {
      DemeterCsv.split("a,\"line1\nline2\",c") shouldBe List(List("a", "line1\nline2", "c"))
    }

    "tolerate CRLF" in {
      DemeterCsv.split("a,b\r\nc,d") shouldBe List(List("a", "b"), List("c", "d"))
    }

    "drop a trailing newline without inventing an empty row" in {
      DemeterCsv.split("a,b\n") shouldBe List(List("a", "b"))
    }
  }

  "the product reader" should {

    "be LENIENT about extra columns, since the contract never specified them" in {
      // migration-demeter.md says only "products ~18.7k, same treatment". Being strict
      // about a header nobody wrote down would reject a valid export.
      val csv = "product_key,raw_name,brand,size_qty,size_unit,gtin,something_else\n" +
        "dk-1,LACTANTIA BUTTER,Lactantia,454,g,4006381333931,whatever"
      val Right(parsed) = DemeterCsv.products(csv): @unchecked
      parsed.ok shouldBe true
      val r = parsed.rows.loneElement
      r.productKey shouldBe "dk-1"
      r.brand shouldBe Some("Lactantia")
      r.sizeQty shouldBe Some(BigDecimal(454))
      r.gtin shouldBe Some("4006381333931")
    }

    "still refuse a missing product_key column" in {
      DemeterCsv.products("raw_name\nbutter").isLeft shouldBe true
    }

    "fall back through the display names for a product with no raw_name" in {
      val csv = "product_key,raw_name,display_name_en\ndk-1,\\N,Lactantia Butter"
      val Right(parsed) = DemeterCsv.products(csv): @unchecked
      parsed.rows.loneElement.rawName shouldBe "Lactantia Butter"
    }

    "refuse a row with no usable name at all" in {
      // It could not be resolved against anything, and mapping it would put an
      // unidentifiable row into the table Demeter keeps for cutover.
      val csv = "product_key,raw_name,display_name_en,display_name_fr\ndk-1,\\N,\\N,\\N"
      val Right(parsed) = DemeterCsv.products(csv): @unchecked
      parsed.ok shouldBe false
      parsed.errors.loneElement.reason should include("no raw_name")
    }
  }
}

package me.cference.ariadne.migrate

import scala.util.Try

/**
 * Strict readers for Demeter's `COPY`→CSV exports (`migration-demeter.md` step 2).
 *
 * STRICT on purpose, in two ways that matter for a one-shot migration:
 *
 *   1. **An unexpected header is a hard failure, not a shrug.** A column that moved or got renamed
 *      would otherwise read as a column of empty strings, and 21,680 rows of silently-empty
 *      `price_confidence` is a corpus that looks complete and is not. The header is checked against
 *      the contract in `migration-demeter.md` before any row is parsed. 2. **Row errors are
 *      COLLECTED, not thrown.** The caller gets every bad row at once, so a malformed export is
 *      diagnosed in one pass rather than one row per re-run. Nothing should be written until the
 *      whole file parses — a half-loaded corpus is worse than none, because the verification step
 *      ("history counts match between the two stores") would then be comparing against a number
 *      nobody chose.
 *
 * Parsing is hand-rolled rather than a CSV library because the input is `COPY`-generated: fields
 * are comma-separated, quoted only when they contain a comma, quote or newline, and `\N` means
 * NULL. That is a small, known grammar, and taking a dependency to read it would be the larger
 * risk. Embedded newlines inside quotes ARE handled, because product names from a flyer can contain
 * anything.
 */
object DemeterCsv {

  /** `\N` is Postgres `COPY`'s NULL sentinel; an empty field is a genuine empty string. */
  private val Null = "\\N"

  final case class RowError(line: Int, reason: String, raw: String)

  final case class Parsed[A](rows: List[A], errors: List[RowError]) {
    def ok: Boolean = errors.isEmpty
  }

  /**
   * One row of Demeter's `price_observation` export.
   *
   * Field names mirror the export's column names rather than Ariadne's vocabulary, deliberately:
   * this type is a faithful picture of THEIR table, and the translation into our domain happens in
   * one named place (`DemeterBackfill`) instead of being smeared across a parser.
   *
   * Both confidences are preserved verbatim per the migration contract — `price_confidence` and
   * `match_confidence` are theirs, and re-deriving either would discard judgment we cannot
   * reconstruct.
   */
  final case class ObservationRow(
      productKey: String,
      merchantId: String,
      flyerId: Option[String],
      observedAt: String,
      rawName: String,
      displayNameEn: Option[String],
      displayNameFr: Option[String],
      effectiveCents: Long,
      priceBasis: Option[String],
      originalCents: Option[Long],
      sizeQty: Option[BigDecimal],
      sizeUnit: Option[String],
      packCount: Option[Int],
      unitCents: Option[Long],
      unitBasis: Option[String],
      saleText: Option[String],
      validFrom: Option[String],
      validTo: Option[String],
      priceConfidence: Double,
      matchConfidence: Double,
      rawResponseId: Option[Long]
  )

  /** The header contract from `migration-demeter.md`, in order. */
  val ObservationColumns: List[String] = List(
    "product_key",
    "merchant_id",
    "flyer_id",
    "observed_at",
    "raw_name",
    "display_name_en",
    "display_name_fr",
    "effective_cents",
    "price_basis",
    "original_cents",
    "size_qty",
    "size_unit",
    "pack_count",
    "unit_cents",
    "unit_basis",
    "sale_text",
    "valid_from",
    "valid_to",
    "price_confidence",
    "match_confidence",
    "raw_response_id"
  )

  def observations(csv: String): Either[String, Parsed[ObservationRow]] =
    parse(csv, ObservationColumns) { (f, line, raw) =>
      for {
        productKey <- required(f, "product_key")
        merchantId <- required(f, "merchant_id")
        observedAt <- required(f, "observed_at")
        rawName <- required(f, "raw_name")
        effective <- requiredLong(f, "effective_cents")
        priceConf <- requiredDouble(f, "price_confidence")
        matchConf <- requiredDouble(f, "match_confidence")
      } yield ObservationRow(
        productKey = productKey,
        merchantId = merchantId,
        flyerId = opt(f, "flyer_id"),
        observedAt = observedAt,
        rawName = rawName,
        displayNameEn = opt(f, "display_name_en"),
        displayNameFr = opt(f, "display_name_fr"),
        effectiveCents = effective,
        priceBasis = opt(f, "price_basis"),
        originalCents = optLong(f, "original_cents"),
        sizeQty = optDecimal(f, "size_qty"),
        sizeUnit = opt(f, "size_unit"),
        packCount = optLong(f, "pack_count").map(_.toInt),
        unitCents = optLong(f, "unit_cents"),
        unitBasis = opt(f, "unit_basis"),
        saleText = opt(f, "sale_text"),
        validFrom = opt(f, "valid_from"),
        validTo = opt(f, "valid_to"),
        priceConfidence = priceConf,
        matchConfidence = matchConf,
        rawResponseId = optLong(f, "raw_response_id")
      )
    }

  /**
   * One row of Demeter's `products` export.
   *
   * The column set is NOT specified in `migration-demeter.md` — it says only "products ~18.7k, same
   * treatment". So this reader takes the columns it needs and IGNORES the rest rather than
   * asserting a full header: being strict about a contract nobody wrote down would reject a valid
   * export. `product_key` and a name are the two it cannot proceed without.
   */
  final case class ProductRow(
      productKey: String,
      rawName: String,
      displayNameEn: Option[String],
      displayNameFr: Option[String],
      brand: Option[String],
      sizeQty: Option[BigDecimal],
      sizeUnit: Option[String],
      gtin: Option[String]
  )

  val ProductRequiredColumns: List[String] = List("product_key")

  def products(csv: String): Either[String, Parsed[ProductRow]] =
    parseLenient(csv, ProductRequiredColumns) { (f, line, raw) =>
      for {
        productKey <- required(f, "product_key")
        // Either a raw name or a display name will do — a product with neither cannot be
        // resolved against anything, and silently mapping it would put an unidentifiable
        // row into the id mapping Demeter keeps for cutover.
        name <- opt(f, "raw_name")
          .orElse(opt(f, "display_name_en"))
          .orElse(opt(f, "display_name_fr"))
          .toRight("no raw_name, display_name_en or display_name_fr")
      } yield ProductRow(
        productKey = productKey,
        rawName = name,
        displayNameEn = opt(f, "display_name_en"),
        displayNameFr = opt(f, "display_name_fr"),
        brand = opt(f, "brand"),
        sizeQty = optDecimal(f, "size_qty"),
        sizeUnit = opt(f, "size_unit"),
        gtin = opt(f, "gtin")
      )
    }

  // ---------------------------------------------------------------- internals

  private def parse[A](csv: String, expected: List[String])(
      row: (Map[String, String], Int, String) => Either[String, A]
  ): Either[String, Parsed[A]] =
    split(csv) match {
      case Nil => Left("empty file — not even a header")
      case header :: rest =>
        val cols = header.map(_.trim)
        if cols != expected then
          Left(
            s"header does not match the migration contract.\n  expected: ${expected.mkString(",")}" +
              s"\n  found:    ${cols.mkString(",")}"
          )
        else Right(rows(cols, rest)(row))
    }

  private def parseLenient[A](csv: String, requiredCols: List[String])(
      row: (Map[String, String], Int, String) => Either[String, A]
  ): Either[String, Parsed[A]] =
    split(csv) match {
      case Nil => Left("empty file — not even a header")
      case header :: rest =>
        val cols = header.map(_.trim)
        val missing = requiredCols.filterNot(cols.contains)
        if missing.nonEmpty then
          Left(s"header is missing required column(s): ${missing.mkString(", ")}")
        else Right(rows(cols, rest)(row))
    }

  private def rows[A](cols: List[String], lines: List[List[String]])(
      row: (Map[String, String], Int, String) => Either[String, A]
  ): Parsed[A] = {
    val (good, bad) = lines.zipWithIndex.foldLeft((List.empty[A], List.empty[RowError])) {
      case ((acc, errs), (fields, i)) =>
        val line = i + 2 // 1-based, and the header is line 1
        val raw = fields.mkString(",")
        if fields.sizeIs != cols.size then
          (acc, RowError(line, s"expected ${cols.size} fields, found ${fields.size}", raw) :: errs)
        else
          row(cols.zip(fields).toMap, line, raw) match {
            case Right(a) => (a :: acc, errs)
            case Left(why) => (acc, RowError(line, why, raw) :: errs)
          }
    }
    Parsed(good.reverse, bad.reverse)
  }

  private def opt(f: Map[String, String], key: String): Option[String] =
    f.get(key).map(_.trim).filter(v => v.nonEmpty && v != Null)

  private def required(f: Map[String, String], key: String): Either[String, String] =
    opt(f, key).toRight(s"$key is required and was empty or NULL")

  private def requiredLong(f: Map[String, String], key: String): Either[String, Long] =
    required(f, key).flatMap(v =>
      Try(v.toLong).toEither.left.map(_ => s"$key is not an integer: '$v'")
    )

  private def requiredDouble(f: Map[String, String], key: String): Either[String, Double] =
    required(f, key).flatMap(v =>
      Try(v.toDouble).toEither.left
        .map(_ => s"$key is not a number: '$v'")
        .flatMap(d =>
          // A confidence outside [0,1] is not a value this domain can hold, and clamping it
          // would quietly invent judgment Demeter did not express.
          if d >= 0.0 && d <= 1.0 then Right(d) else Left(s"$key is outside [0,1]: $d")
        )
    )

  private def optLong(f: Map[String, String], key: String): Option[Long] =
    opt(f, key).flatMap(v => Try(v.toLong).toOption)

  private def optDecimal(f: Map[String, String], key: String): Option[BigDecimal] =
    opt(f, key).flatMap(v => Try(BigDecimal(v)).toOption)

  /**
   * Split `COPY`-shaped CSV into rows of fields, honouring quotes (including embedded commas,
   * doubled quotes and newlines). Blank trailing lines are dropped; a blank line in the middle is a
   * row of one empty field and will fail the field-count check, which is the right outcome.
   */
  private[migrate] def split(csv: String): List[List[String]] = {
    val out = List.newBuilder[List[String]]
    val row = List.newBuilder[String]
    val field = new StringBuilder
    var inQuotes = false
    var i = 0
    var sawAny = false

    def endField(): Unit = { row += field.toString; field.clear() }
    def endRow(): Unit = { endField(); out += row.result(); row.clear() }

    while i < csv.length do {
      val c = csv.charAt(i)
      if inQuotes then
        if c == '"' then
          if i + 1 < csv.length && csv.charAt(i + 1) == '"' then { field += '"'; i += 1 }
          else inQuotes = false
        else field += c
      else
        c match {
          case '"' => inQuotes = true; sawAny = true
          case ',' => endField(); sawAny = true
          case '\r' => () // tolerate CRLF
          case '\n' => if sawAny || field.nonEmpty then { endRow(); sawAny = false }
          case other => field += other; sawAny = true
        }
      i += 1
    }
    if sawAny || field.nonEmpty then endRow()
    out.result()
  }
}

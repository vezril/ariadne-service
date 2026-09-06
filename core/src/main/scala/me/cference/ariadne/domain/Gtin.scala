package me.cference.ariadne.domain

/**
 * A GTIN — the ONLY key trusted for automatic identity (§6.1).
 *
 * Accepts GTIN-8, -12 (UPC-A), -13 (EAN-13) and -14, validates the mod-10 check digit, and
 * normalises to 14 digits by left-padding with zeros. The normalisation matters: the same product
 * scanned as a UPC-A and an EAN-13 must compare equal, so the *stored* form is always 14.
 *
 * A product may legitimately hold several GTINs (pack variants) — but note §6.7: GTINs are
 * size-specific, so two pack sizes are two products, never one.
 */
opaque type Gtin = String

object Gtin {

  private val ValidLengths = Set(8, 12, 13, 14)

  /** Parse and validate. Returns the normalised 14-digit form. */
  def parse(raw: String): Either[DomainError, Gtin] = {
    val trimmed = Option(raw).map(_.trim).getOrElse("")
    if trimmed.isEmpty then Left(DomainError.InvalidGtin(raw, "empty"))
    else if !trimmed.forall(_.isDigit) then Left(DomainError.InvalidGtin(raw, "must be all digits"))
    else if !ValidLengths.contains(trimmed.length) then
      Left(DomainError.InvalidGtin(raw, s"length ${trimmed.length} is not one of 8, 12, 13, 14"))
    else {
      val padded = padTo14(trimmed)
      val stated = padded.last.asDigit
      val computed = checkDigit(padded.dropRight(1))
      if stated != computed then
        Left(DomainError.InvalidGtin(raw, s"check digit $stated should be $computed"))
      else Right(padded)
    }
  }

  def unsafe(raw: String): Gtin =
    parse(raw).fold(e => throw new IllegalArgumentException(e.message), identity)

  private def padTo14(digits: String): String =
    if digits.length >= 14 then digits else ("0" * (14 - digits.length)) + digits

  /**
   * Mod-10: weight the data digits 3,1,3,1… from the right, sum, then the check digit is whatever
   * rounds that sum up to the next multiple of ten.
   */
  private[domain] def checkDigit(dataDigits: String): Int = {
    val sum = dataDigits.reverse.zipWithIndex.map { case (ch, i) =>
      ch.asDigit * (if i % 2 == 0 then 3 else 1)
    }.sum
    (10 - (sum % 10)) % 10
  }

  extension (g: Gtin) {

    /** The canonical 14-digit form — what gets stored and indexed. */
    def value: String = g
  }
}

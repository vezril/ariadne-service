package me.cference.ariadne.matching

import me.cference.ariadne.domain.{MeasureUnit, Quantity}

import scala.util.matching.Regex

/**
 * Pulls a pack size out of a product name, and hands back the name with the size REMOVED.
 *
 * Both halves matter. The size is identity (§6.7 — a 454 g and a 250 g of the same brand are
 * different products), so it is scored separately and strictly. The remainder is what the name
 * similarity should actually compare: leaving "454 g" in the token set lets two unrelated 454 g
 * products look alike for the wrong reason.
 *
 * Lives in `matching`, NOT in the `text` island, because it depends on the domain's Quantity —
 * DESIGN §10.5's island rule keeps `text` free of domain types.
 */
final case class ParsedSize(quantity: Option[Quantity], remainder: String)

object SizeParser {

  private val UnitAliases: Map[String, MeasureUnit] = Map(
    "g" -> MeasureUnit.Gram,
    "gr" -> MeasureUnit.Gram,
    "gram" -> MeasureUnit.Gram,
    "grams" -> MeasureUnit.Gram,
    "grammes" -> MeasureUnit.Gram,
    "kg" -> MeasureUnit.Kilogram,
    "kilo" -> MeasureUnit.Kilogram,
    "kilos" -> MeasureUnit.Kilogram,
    "ml" -> MeasureUnit.Millilitre,
    "millilitre" -> MeasureUnit.Millilitre,
    "l" -> MeasureUnit.Litre,
    "lt" -> MeasureUnit.Litre,
    "litre" -> MeasureUnit.Litre,
    "litres" -> MeasureUnit.Litre,
    "liter" -> MeasureUnit.Litre,
    "ea" -> MeasureUnit.Each,
    "each" -> MeasureUnit.Each,
    "ct" -> MeasureUnit.Each,
    "count" -> MeasureUnit.Each,
    "pk" -> MeasureUnit.Each,
    "pack" -> MeasureUnit.Each,
    "un" -> MeasureUnit.Each,
    "unites" -> MeasureUnit.Each
  )

  private val unitAlt = UnitAliases.keys.toList.sortBy(-_.length).mkString("|")
  private val num = """\d+(?:[.,]\d+)?"""

  /** "2 x 1 L", "2x1L", "12 × 355 ml" — a multipack's size is the TOTAL. */
  private val MultiPack: Regex = raw"""(?i)\b($num)\s*[x×]\s*($num)\s*($unitAlt)\b""".r

  /** "750 ml", "454g", "1,5 L" (comma decimals are normal in French listings). */
  private val Simple: Regex = raw"""(?i)\b($num)\s*($unitAlt)\b""".r

  def parse(name: String): ParsedSize =
    // Multipack first: "2 x 1 L" also matches Simple as "1 L", and the total is the truth.
    MultiPack.findFirstMatchIn(name) match {
      case Some(m) =>
        val total = for {
          count <- decimal(m.group(1))
          each <- decimal(m.group(2))
          unit <- UnitAliases.get(m.group(3).toLowerCase)
          q <- Quantity(count * each, unit).toOption
        } yield q
        ParsedSize(total, strip(name, m.matched))
      case None =>
        // Last match, not first: names lead with the brand and trail with the size
        // ("Coca-Cola 2 L"), and a stray leading number is usually not a pack size.
        Simple.findAllMatchIn(name).toList.lastOption match {
          case Some(m) =>
            val q = for {
              amount <- decimal(m.group(1))
              unit <- UnitAliases.get(m.group(2).toLowerCase)
              q <- Quantity(amount, unit).toOption
            } yield q
            ParsedSize(q, strip(name, m.matched))
          case None => ParsedSize(None, name)
        }
    }

  private def decimal(s: String): Option[BigDecimal] =
    scala.util.Try(BigDecimal(s.replace(',', '.'))).toOption

  private def strip(name: String, matched: String): String =
    name.replace(matched, " ").replaceAll("\\s+", " ").trim
}

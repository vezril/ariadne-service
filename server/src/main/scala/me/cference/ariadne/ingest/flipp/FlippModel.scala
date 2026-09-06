package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.text.{BilingualText, Locale}

import java.time.Instant

/**
 * The Flipp WIRE MODEL — what the feed says, before Ariadne has an opinion about it.
 *
 * Ported from demeter-service `modules/foundations`. Kept as its own layer rather than decoding
 * straight into the domain, for two reasons:
 *
 *   - the decoders port verbatim only if their output types come with them; decoding directly into
 *     `ProductId`/`Money`/`PriceScope` would be a rewrite of exactly the code whose leniency rules
 *     were tuned against a live upstream;
 *   - the wire/domain boundary is where the interesting mismatches live (see `FlippMapper`), and a
 *     boundary you can point at is one you can test.
 */
final case class MerchantId(value: Int) extends AnyVal
final case class FlyerId(value: Long) extends AnyVal
final case class Merchant(id: MerchantId, name: String)

/** Canadian postal code, split as Flipp uses it: FSA is the flyer-coverage region. */
final case class PostalCode private (fsa: String, ldu: String) {
  def canonical: String = s"$fsa$ldu"
}

object PostalCode {
  private val Shape = """([A-Za-z]\d[A-Za-z])\s*(\d[A-Za-z]\d)""".r

  def parse(raw: String): Either[String, PostalCode] =
    raw.trim.toUpperCase match {
      case Shape(fsa, ldu) => Right(new PostalCode(fsa, ldu))
      case other => Left(s"not a postal code: $other")
    }

  def unsafe(raw: String): PostalCode =
    parse(raw).fold(e => throw new IllegalArgumentException(e), identity)
}

/**
 * A price exactly as the feed expressed it, in CENTS.
 *
 * Ported rather than mapped straight onto the domain's `Money`, because the two disagree about zero
 * and that disagreement must be visible at the boundary rather than buried in a decoder — see
 * `FlippMapper`. Rejects negative; permits zero, as Demeter's does.
 */
final case class FlippMoney private (cents: Long) {
  def format: String = f"${cents / 100}%d.${cents % 100}%02d"
}

object FlippMoney {
  // Canonical decimal only: optional sign, digits, optional fraction. Comma decimals,
  // currency symbols and grouping are the normaliser's job, and are rejected here.
  private val DecimalShape = """(-?)(\d+)(?:\.(\d+))?""".r

  def cents(n: Long): FlippMoney = new FlippMoney(n)

  def fromDecimal(s: String): Either[String, FlippMoney] =
    s match {
      case DecimalShape(sign, whole, frac) =>
        if sign == "-" then Left(s"negative price: $s")
        else if frac != null && frac.length > 2 then Left(s"too many decimal places: $s")
        else
          scala.util
            .Try {
              val fracCents = Option(frac).map(_.padTo(2, '0').toLong).getOrElse(0L)
              new FlippMoney(Math.addExact(Math.multiplyExact(whole.toLong, 100L), fracCents))
            }
            .toEither
            .left
            .map(_ => s"not a number: $s")
      case other => Left(s"not a number: $other")
    }
}

/**
 * A flyer. Note what it does NOT carry: any identifier for an individual store.
 *
 * `merchantId` + `postalCode` is the finest grain the feed offers, which is why a scraped price is
 * a `Regional(chain, area)` fact in Ariadne rather than a store one (DESIGN §2.3.1).
 */
final case class Flyer(
    id: FlyerId,
    merchantId: MerchantId,
    name: String,
    validFrom: Instant,
    validTo: Instant,
    postalCode: PostalCode,
    locale: Locale
)

final case class FlyerItem(
    sourceItemId: String,
    flyerId: FlyerId,
    merchantId: MerchantId,
    name: BilingualText,
    rawName: String,
    currentPrice: Option[FlippMoney],
    originalPrice: Option[FlippMoney],
    saleStory: Option[String],
    validFrom: Instant,
    validTo: Instant
)

/** A decode failure, with enough context to find the offending payload in the archive. */
final case class DecodeError(source: String, pointer: String, reason: String) {
  def message: String = s"$source at $pointer: $reason"
}

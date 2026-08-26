package me.cference.ariadne.domain

/**
 * Identifiers and the small shared value types every aggregate leans on.
 *
 * All ids are opaque-ish wrappers over String (ULIDs in practice). They are deliberately dumb: the
 * catalog owns identity, and identity is a name for a thing, not a place to hang behaviour.
 */
final case class ProductId(value: String) extends AnyVal
final case class StoreId(value: String) extends AnyVal
final case class PurchaseId(value: String) extends AnyVal

/**
 * A retailer's own stable id for a listing — the second strong key (§6.2).
 *
 * Once a listing resolves, the link is remembered and every later scrape of that listing
 * short-circuits the matcher entirely.
 */
final case class ListingKey(storeId: StoreId, externalId: String)

/**
 * Adopted from an incoming edge when present, minted when absent (§8). Journalled on every event
 * and echoed on every publish.
 */
final case class CorrelationId(value: String) extends AnyVal

/**
 * The matcher revision that produced a link (§6.6).
 *
 * Recorded on every `ListingLinked` so a resolver change migrates history deliberately rather than
 * silently orphaning it — Demeter's `Version="v1"` discipline, carried over.
 */
final case class MatcherVersion(value: String) extends AnyVal

/**
 * A certainty in [0, 1].
 *
 * NOTE: distinct from the text package's `SplitConfidence` (Low|Medium|High), which is Demeter's
 * bilingual-split notion. Same word, different concept — DESIGN §10.5 says explicitly not to unify
 * them.
 */
opaque type Confidence = Double

object Confidence {
  val Certain: Confidence = 1.0

  def apply(d: Double): Either[DomainError, Confidence] =
    if d.isNaN then Left(DomainError.InvalidConfidence("NaN"))
    else if d < 0.0 || d > 1.0 then Left(DomainError.InvalidConfidence(d.toString))
    else Right(d)

  /** For literals known good at the call site (thresholds, test fixtures). */
  def unsafe(d: Double): Confidence =
    apply(d).fold(e => throw new IllegalArgumentException(e.message), identity)

  extension (c: Confidence) {
    def toDouble: Double = c

    /**
     * The weakest link — Demeter's `split.confidence.min(sizeConfidence)` coupling (§2.3),
     * preserved so downstream judgment cannot read too high.
     */
    def min(that: Confidence): Confidence = if c <= that then c else that
  }
}

/** How a listing came to be linked to a product (§6.4). */
enum MatchMethod {
  case Gtin
  case Listing
  case Fuzzy
  case Human
}

/** Where a product came from (§2.1). */
enum Origin {
  case Manual
  case Scrape(listing: ListingKey)
  case Migration(source: String)
}

/**
 * A flyer's own claim about a promotion — a FACT, not a judgment. Whether it is a *good deal* is
 * Demeter's call; the line stays.
 */
final case class PromoFlag(description: String, percentOff: Option[BigDecimal])

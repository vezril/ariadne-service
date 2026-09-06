package me.cference.ariadne.matching

import me.cference.ariadne.domain.{Confidence, Quantity}
import me.cference.ariadne.text.TextNormalizer

/**
 * Tuning knobs. Config, not code — DESIGN §6.4 says thresholds get tuned against the review queue's
 * accept rate, and against the Demeter backfill corpus once it lands.
 */
final case class MatchConfig(
    nameWeight: Double = 0.60,
    brandWeight: Double = 0.25,
    sizeWeight: Double = 0.15,
    sizeTolerance: BigDecimal = BigDecimal("0.02"),
    brandConflictFactor: Double = 0.35,
    sizeConflictFactor: Double = 0.25,
    /**
     * Both sides must be at least this long before fuzzy similarity counts for anything.
     *
     * Inherited from Demeter's matcher, where it was tuned against a real failure: 14 of 83 butter
     * alerts in one production run were false positives that scored IDENTICALLY to the wanted
     * match, so no threshold could separate them. Length could — at 7, "yogourt" still matches and
     * "butter" stops fuzzing entirely. The cost is that short plurals need their own alias, which
     * is explicit and cheap. 0 disables the rule.
     */
    minFuzzyLength: Int = 7
)

/** What the subject and the candidate look like after normalisation — the scorer's real inputs. */
final case class MatchInput(
    tokens: List[String],
    normalized: String,
    brand: Option[String],
    size: Option[Quantity]
)

object MatchInput {

  /**
   * Normalise a raw listing/product name into scoreable parts.
   *
   * Order matters: the size comes out FIRST so it is scored as identity rather than smuggled into
   * the name comparison, where "454 g" would make two unrelated products look alike.
   */
  def from(
      rawName: String,
      brand: Option[String],
      knownSize: Option[Quantity] = None
  ): MatchInput = {
    val parsed = SizeParser.parse(rawName)
    val norm = TextNormalizer.normalize(parsed.remainder)
    val brandNorm = brand.map(b => TextNormalizer.normalize(b).joined).filter(_.nonEmpty)
    // The brand is usually also present in the name; removing it stops it being counted twice.
    val tokens = brandNorm.fold(norm.tokens)(b => norm.tokens.filterNot(b.split(' ').toSet))
    MatchInput(tokens, tokens.mkString(" "), brandNorm, knownSize.orElse(parsed.quantity))
  }
}

/** The score plus why — "why is this listing on this product" must always be answerable (§6.5). */
final case class Scored(
    confidence: Confidence,
    name: Double,
    brand: Option[Double],
    size: Option[Double],
    notes: List[String]
)

/**
 * The fuzzy fallback of §6.3. GTIN and listing-key matches never reach here — those are exact and
 * decided upstream (§6.1, §6.2); this only runs when identity has to be inferred.
 */
object Scorer {

  def score(
      subject: MatchInput,
      candidate: MatchInput,
      config: MatchConfig = MatchConfig()
  ): Scored = {
    // --- name -------------------------------------------------------------
    val exactTokens = subject.tokens.nonEmpty && subject.tokens == candidate.tokens
    val tooShortToFuzz =
      config.minFuzzyLength > 0 &&
        (subject.normalized.length < config.minFuzzyLength || candidate.normalized.length < config.minFuzzyLength)

    val nameScore =
      if exactTokens then 1.0
      else if tooShortToFuzz then {
        // Short strings get token-set credit only. Trigram similarity between short
        // words is nearly meaningless and is where the butter/yogourt class of false
        // positive comes from.
        Similarity.tokenSet(subject.tokens, candidate.tokens)
      } else {
        0.5 * Similarity.trigram(subject.normalized, candidate.normalized) +
          0.5 * Similarity.tokenSet(subject.tokens, candidate.tokens)
      }

    // --- brand ------------------------------------------------------------
    val (brandScore, brandConflict) = (subject.brand, candidate.brand) match {
      case (Some(a), Some(b)) if a == b => (Some(1.0), false)
      case (Some(_), Some(_)) => (Some(0.0), true) // two KNOWN, DIFFERENT brands
      case _ => (None, false) // unknown on either side says nothing
    }
    // --- size -------------------------------------------------------------
    // §6.7: size is part of identity. An incompatible size is a strong NEGATIVE, not a
    // small deduction — a 454 g and a 250 g are different products, not a near miss.
    val (sizeScore, sizeConflict) = (subject.size, candidate.size) match {
      case (Some(a), Some(b)) =>
        a.isCloseTo(b, config.sizeTolerance) match {
          case Right(true) => (Some(1.0), false)
          case Right(false) => (Some(0.0), true)
          case Left(_) => (Some(0.0), true) // incomparable dimensions: certainly not the same pack
        }
      case _ => (None, false)
    }
    // --- combine ----------------------------------------------------------
    // Absent signals drop out of both numerator and denominator rather than scoring
    // zero: not knowing a brand is not evidence against a match.
    val parts = List(
      Some(config.nameWeight -> nameScore),
      brandScore.map(config.brandWeight -> _),
      sizeScore.map(config.sizeWeight -> _)
    ).flatten
    val weightSum = parts.map(_._1).sum
    val base = if weightSum <= 0 then 0.0 else parts.map { case (w, s) => w * s }.sum / weightSum

    val penalised = base *
      (if brandConflict then config.brandConflictFactor else 1.0) *
      (if sizeConflict then config.sizeConflictFactor else 1.0)

    val notes = List(
      Option.when(tooShortToFuzz && !exactTokens)("fuzzy suppressed: below minFuzzyLength"),
      Option.when(brandConflict)("brand conflict"),
      Option.when(sizeConflict)("size conflict")
    ).flatten

    val clamped = math.max(0.0, math.min(1.0, penalised))
    Scored(Confidence.unsafe(clamped), nameScore, brandScore, sizeScore, notes)
  }
}

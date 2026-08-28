package me.cference.ariadne.text

/**
 * The supporting types of the text island.
 *
 * THE ISLAND RULE (DESIGN §10.5): this package imports NOTHING from `me.cference.ariadne.domain`.
 * Ariadne owns the shared text library embedded rather than as a separately published artifact, and
 * that decision is only reversible while the package stays self-contained — the moment `ProductId`
 * or `Quantity` leaks in, extracting it for a third consumer stops being a copy-paste and the
 * choice silently becomes permanent.
 *
 * Ported from demeter-service `modules/normalization` + `modules/foundations`, which is where this
 * logic was written and tuned against real Flipp data.
 */

/** Closed on purpose: only the two locales Flipp serves. */
enum Locale(val queryValue: String) {
  case FrCa extends Locale("fr-ca")
  case EnCa extends Locale("en-ca")
}

/** A dumb container for the two language forms of a name. Never fabricates a translation. */
final case class BilingualText(fr: Option[String], en: Option[String]) {

  /** Preferred language, else the other, else none. */
  def primary(preferred: Locale): Option[String] = preferred match {
    case Locale.FrCa => fr.orElse(en)
    case Locale.EnCa => en.orElse(fr)
  }

  def anyForm: Option[String] = en.orElse(fr)

  /** Every present form, deduplicated — the matcher's input. */
  def forms: List[String] = (fr.toList ++ en.toList).distinct
}

object BilingualText {
  val empty: BilingualText = BilingualText(None, None)
  def frOnly(s: String): BilingualText = BilingualText(Some(s), None)
  def enOnly(s: String): BilingualText = BilingualText(None, Some(s))
}

/**
 * How sure the bilingual split is.
 *
 * DELIBERATELY NOT the domain's `Confidence` (a match score in [0,1]) — DESIGN §10.5 says in as
 * many words not to unify them. They answer different questions: this one asks "did we cut the
 * string in the right place", the other asks "is this the right product". Merging them would let a
 * splitting doubt masquerade as a matching doubt.
 */
enum SplitConfidence(private val rank: Int) {
  case Low extends SplitConfidence(0)
  case Medium extends SplitConfidence(1)
  case High extends SplitConfidence(2)

  /** The weakest link. */
  def min(that: SplitConfidence): SplitConfidence = if rank <= that.rank then this else that
}

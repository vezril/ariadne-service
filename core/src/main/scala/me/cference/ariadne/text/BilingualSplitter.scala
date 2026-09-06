package me.cference.ariadne.text

/**
 * Split Flipp's jammed-together bilingual names into a BilingualText.
 *
 * Deliberately cheap detection (diacritics + small lexicons) and honest about uncertainty: when
 * unsure it degrades to "both forms" at Low confidence, which is SAFE for matching — a name offered
 * in both languages can still match, whereas a confidently wrong split cannot. Never fabricates a
 * translation and never mutates the raw string.
 *
 * Ported from demeter-service `BilingualSplitter`.
 */
final case class SplitResult(text: BilingualText, confidence: SplitConfidence)

object BilingualSplitter {

  /** Spaced separators only — an unspaced dash is a hyphenated word, not a language boundary. */
  private val Separators = List("\\|", "\\n", "\\s/\\s", "\\s-\\s")

  private val FrenchDiacritics = "àâäçéèêëîïôöùûüÿ".toSet

  private val FrenchWords: Set[String] = Set(
    "lait",
    "beurre",
    "fromage",
    "oeuf",
    "oeufs",
    "pain",
    "jus",
    "poulet",
    "boeuf",
    "porc",
    "jambon",
    "yogourt",
    "legumes",
    "fruits",
    "pomme",
    "pommes",
    "arachide",
    "arachides",
    "croquant",
    "creme",
    "glacee",
    "sucre",
    "farine",
    "gratuit",
    "rabais",
    "prix",
    "moitie",
    "chacun",
    "paquet",
    "boite",
    "surgele",
    "frais",
    "fume",
    "filtre",
    "finement",
    "saveur",
    "biologique",
    "poisson",
    "saumon",
    "riz",
    "cafe",
    "the",
    "eau",
    "de",
    "du",
    "des",
    "au",
    "aux",
    "avec",
    "et",
    "pour",
    "sans",
    "sur"
  )

  private val EnglishWords: Set[String] = Set(
    "milk",
    "butter",
    "cheese",
    "egg",
    "eggs",
    "bread",
    "juice",
    "chicken",
    "beef",
    "pork",
    "ham",
    "yogurt",
    "vegetables",
    "fruit",
    "apple",
    "apples",
    "peanut",
    "crunchy",
    "cream",
    "frozen",
    "sugar",
    "flour",
    "free",
    "price",
    "half",
    "each",
    "pack",
    "box",
    "fresh",
    "smoked",
    "filtered",
    "finely",
    "flavour",
    "organic",
    "fish",
    "salmon",
    "rice",
    "coffee",
    "tea",
    "water",
    "the",
    "with",
    "and",
    "for",
    "of",
    "shelf",
    "rack",
    "resin",
    "tool",
    "tools",
    "set"
  )

  def splitBilingual(raw: String): SplitResult = {
    val segments = Separators
      .foldLeft(List(raw))((segs, sep) => segs.flatMap(_.split(sep).toList))
      .map(_.trim)
      .filter(_.nonEmpty)

    segments match {
      case Nil => SplitResult(BilingualText.empty, SplitConfidence.Low)
      case single :: Nil => detectSingle(single)
      case first :: rest =>
        // Three or more segments is rare and means the heuristic is out of its depth: take the
        // outer two as the language pair and say so with Low confidence rather than guess well.
        val second = rest.last
        val degraded = rest.size > 1
        val (bt, conf) = assignPair(first, second)
        SplitResult(bt, if degraded then SplitConfidence.Low else conf)
    }
  }

  /** (frenchScore, englishScore) — diacritics weigh double, lexicon hits single. */
  private def scores(s: String): (Int, Int) = {
    val lower = s.toLowerCase
    val tokens = TextNormalizer.normalize(s, stopwords = Set.empty).tokens
    val elision = if lower.contains("d'") || lower.contains("l'") then 1 else 0
    val fr = lower.count(FrenchDiacritics) * 2 + tokens.count(FrenchWords) + elision
    val en = tokens.count(EnglishWords)
    (fr, en)
  }

  private def detectSingle(s: String): SplitResult = {
    val (fr, en) = scores(s)
    if fr > en then SplitResult(BilingualText(Some(s), None), SplitConfidence.High)
    else if en > fr then SplitResult(BilingualText(None, Some(s)), SplitConfidence.High)
    // Ambiguous: offer both forms. Safe for matching, and honest.
    else SplitResult(BilingualText(Some(s), Some(s)), SplitConfidence.Low)
  }

  private def assignPair(a: String, b: String): (BilingualText, SplitConfidence) = {
    val (frA, enA) = scores(a)
    val (frB, enB) = scores(b)
    if frA > enA && enB >= frB then (BilingualText(Some(a), Some(b)), SplitConfidence.High)
    else if enA > frA && frB >= enB then (BilingualText(Some(b), Some(a)), SplitConfidence.High)
    // Undetectable: fall back to the observed Quebec convention (FR | EN), medium confidence.
    else (BilingualText(Some(a), Some(b)), SplitConfidence.Medium)
  }
}

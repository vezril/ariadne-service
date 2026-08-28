package me.cference.ariadne.text

import java.text.Normalizer

/**
 * The shared matching-text normaliser. Steps are ordered and pinned; normalisation is idempotent.
 *
 * Ported from demeter-service `TextNormalizer` — same steps, same stopwords, because the tuning
 * behind them came from real Flipp data and a rewrite would quietly lose it.
 */
final case class NormalizedText(tokens: List[String]) {
  def joined: String = tokens.mkString(" ")
}

object TextNormalizer {

  /** Both languages; the catalogue is Quebec-bilingual so neither list is optional. */
  val DefaultStopwords: Set[String] =
    Set("de", "du", "des", "le", "la", "les", "the", "a", "an", "and", "et", "avec", "with", "&")

  def normalize(text: String, stopwords: Set[String] = DefaultStopwords): NormalizedText = {
    // 1. NFKD + strip combining marks (é -> e). Ligatures are folded explicitly: Unicode gives
    //    œ/æ no decomposition, but French product names need oe/ae or "bœuf" never matches "boeuf".
    val folded = Normalizer
      .normalize(
        text.replace("œ", "oe").replace("Œ", "OE").replace("æ", "ae").replace("Æ", "AE"),
        Normalizer.Form.NFKD
      )
      .replaceAll("\\p{M}+", "")
    // 2. lowercase
    val lower = folded.toLowerCase
    // 3. punctuation and symbols become spaces; digit<->letter boundaries split so "4L" and "4 L"
    //    normalise alike — otherwise the same pack size reads as two different products.
    val spaced = lower
      .replaceAll("[^\\p{Alnum}]+", " ")
      .replaceAll("(?<=\\d)(?=\\p{Alpha})", " ")
      .replaceAll("(?<=\\p{Alpha})(?=\\d)", " ")
    // 4. collapse whitespace and trim; 5. drop stopwords
    val tokens = spaced.trim.split("\\s+").toList.filter(_.nonEmpty).filterNot(stopwords)
    NormalizedText(tokens)
  }
}

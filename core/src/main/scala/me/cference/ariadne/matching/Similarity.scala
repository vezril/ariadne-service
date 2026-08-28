package me.cference.ariadne.matching

/**
 * String similarity primitives. Pure, total, and cheap — this runs on a shortlist, not the whole
 * catalogue (candidate retrieval is the match-index projection's job, §6.4).
 */
object Similarity {

  /**
   * Dice coefficient over character trigrams: 2|A∩B| / (|A|+|B|). Robust to word order and to the
   * small spelling drift retailer listings are full of.
   */
  def trigram(a: String, b: String): Double = {
    val (ta, tb) = (trigrams(a), trigrams(b))
    if ta.isEmpty && tb.isEmpty then 1.0
    else if ta.isEmpty || tb.isEmpty then 0.0
    else 2.0 * ta.intersect(tb).size / (ta.size + tb.size).toDouble
  }

  /**
   * Jaccard over token sets: order-independent, and it rewards sharing whole words rather than
   * incidental letter runs. Complements trigram, which is the opposite bias.
   */
  def tokenSet(a: List[String], b: List[String]): Double = {
    val (sa, sb) = (a.toSet, b.toSet)
    if sa.isEmpty && sb.isEmpty then 1.0
    else if sa.isEmpty || sb.isEmpty then 0.0
    else sa.intersect(sb).size / sa.union(sb).size.toDouble
  }

  private def trigrams(s: String): Set[String] = {
    val padded = s"  ${s.trim}  "
    if padded.length < 3 then Set(padded.trim).filter(_.nonEmpty)
    else padded.sliding(3).toSet
  }
}

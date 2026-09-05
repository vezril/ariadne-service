package me.cference.ariadne.resolver

import me.cference.ariadne.matching.Similarity
import me.cference.ariadne.projection.ReadModelRepository
import me.cference.ariadne.projection.ReadModelRepository.StoreRow
import me.cference.ariadne.text.TextNormalizer

import scala.concurrent.{ExecutionContext, Future}

/** One franchise the typed text might mean, with why it scored (§7.1). */
final case class StoreCandidate(store: StoreRow, score: Double, why: String)

/**
 * Resolving a receipt's typed store text to a franchise (§7.1, the gap in §7.2).
 *
 * **The thing that makes this different from product resolution:** a receipt says "Metro", and
 * "Metro" is a CHAIN, not a franchise. §2.2 made the individual franchise the Store, so the common
 * case is that the text identifies several stores correctly and none of them uniquely. Auto-picking
 * one would attribute a purchase to a specific franchise on no evidence at all — the same class of
 * error §2.3.1 refuses for prices, arriving through the back door of a receipt.
 *
 * So this NEVER picks. It ranks and hands back candidates, and reports `unique` only when the text
 * genuinely narrows to one. The caller shows a picker, which is what §7.2 said the surface is for.
 *
 * Scoring is done here rather than in SQL because there are a handful of stores, not a catalogue —
 * §7.1's own reasoning. That stops being true somewhere in the hundreds, at which point this wants
 * the same pg_trgm treatment `match_index` gets; the repository call is already bounded by `limit`
 * so the change is contained.
 */
final class StoreResolver(
    repo: ReadModelRepository,
    config: StoreResolverConfig = StoreResolverConfig()
)(using ec: ExecutionContext) {

  def resolve(
      text: String,
      area: Option[String] = None
  ): Future[List[StoreCandidate]] = {
    val normalized = TextNormalizer.normalize(text)
    if normalized.joined.isEmpty then Future.successful(Nil)
    else
      repo.listStores(area = area, activeOnly = true, limit = config.scanLimit).map { stores =>
        stores
          .map(score(normalized.joined, normalized.tokens, _))
          .filter(_.score >= config.floor)
          .sortBy(c => (-c.score, c.store.id))
          .take(config.topK)
      }
  }

  private def score(query: String, queryTokens: List[String], store: StoreRow): StoreCandidate = {
    val name = TextNormalizer.normalize(store.name)
    val chain = TextNormalizer.normalize(store.chainId)

    // The chain is scored SEPARATELY from the name, because a receipt names the banner
    // and a franchise's name usually contains the banner plus a location. Blending them
    // into one string would let a long location suffix dilute an exact banner match.
    val byName = math.max(
      Similarity.trigram(query, name.joined),
      Similarity.tokenSet(queryTokens, name.tokens)
    )
    val byChain = math.max(
      Similarity.trigram(query, chain.joined),
      Similarity.tokenSet(queryTokens, chain.tokens)
    )

    if byChain >= config.chainExact && byChain >= byName then
      // The honest reading of "Metro" on a receipt: it names the banner, and every
      // franchise of that banner is equally consistent with it.
      StoreCandidate(store, byChain, s"chain '${store.chainId}' matches the text")
    else if byName >= byChain then StoreCandidate(store, byName, "store name matches the text")
    else StoreCandidate(store, byChain, s"chain '${store.chainId}' matches the text")
  }
}

/**
 * Thresholds are config for the same reason the product resolver's are (§6.4): they are guesses
 * until they have been run against real receipts, and tuning them should not be a deploy.
 */
final case class StoreResolverConfig(
    /** Below this a candidate is noise rather than a weak answer. */
    floor: Double = 0.45,
    /** At or above this, the text is read as naming the banner rather than the franchise. */
    chainExact: Double = 0.95,
    topK: Int = 10,
    /**
     * How many franchises to score in memory. A ceiling, not a page: if it is ever reached the
     * scoring is silently incomplete, which is why it sits far above any plausible number of stores
     * Calvin shops at.
     */
    scanLimit: Int = 500
)

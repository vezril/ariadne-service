package me.cference.ariadne.resolver

import me.cference.ariadne.domain.{
  Confidence,
  Gtin,
  ListingKey,
  MatchMethod,
  MatcherVersion,
  MeasureUnit,
  Quantity
}
import me.cference.ariadne.matching.{MatchConfig, MatchInput, Scorer}
import me.cference.ariadne.projection.ReadModelRepository
import me.cference.ariadne.projection.ReadModelRepository.MatchCandidateRow

import scala.concurrent.{ExecutionContext, Future}

/** What we are trying to identify — a scraped listing, or a Dionysus ingredient. */
final case class MatchSubject(
    name: String,
    brand: Option[String] = None,
    gtin: Option[Gtin] = None,
    listing: Option[ListingKey] = None
)

final case class ScoredCandidate(productId: String, score: Confidence, notes: List[String])

/** The three answers the resolver can give (§6.4). */
enum ResolutionOutcome {
  case Matched(productId: String, confidence: Confidence, method: MatchMethod)
  case Ambiguous(candidates: List[ScoredCandidate])
  case NoMatch
}

/**
 * Thresholds are CONFIG, not code — DESIGN §6.4 says to tune them against the review queue's accept
 * rate and the Demeter backfill corpus. Hard-coding them would make that tuning a deploy.
 */
final case class ResolverConfig(
    autoLink: Double = 0.92,
    review: Double = 0.60,
    topK: Int = 10,
    matcherVersion: MatcherVersion = MatcherVersion("v1"),
    matching: MatchConfig = MatchConfig()
)

/**
 * Hot spot #1 — the thread through the labyrinth (§6).
 *
 * Strong keys first, fuzzy only as a fallback:
 *
 *   1. GTIN — the ONLY key trusted for automatic identity. A hit is identity, not a guess. 2.
 *      Listing key — a listing already resolved short-circuits the matcher entirely, which is what
 *      makes the pipeline cheap in steady state: fuzzy matching runs once per NEW listing, not once
 *      per observation. 3. Trigram top-K from the index, then the PURE scorer recomputes exactly on
 *      the shortlist.
 *
 * The split in step 3 is deliberate. Postgres narrows; the domain decides. The confidence written
 * onto a link has to be the number the scorer computed, because that number is what makes an
 * auto-link auditable and reversible (§6.5) — and pg_trgm's similarity is a different function from
 * the scorer's blend.
 */
final class Resolver(repo: ReadModelRepository, config: ResolverConfig = ResolverConfig())(using
    ec: ExecutionContext
) {

  def resolve(subject: MatchSubject): Future[ResolutionOutcome] =
    byStrongKey(subject).flatMap {
      case Some(outcome) => Future.successful(outcome)
      case None => byFuzzy(subject)
    }

  private def byStrongKey(subject: MatchSubject): Future[Option[ResolutionOutcome]] = {
    val byGtin = subject.gtin match {
      case Some(g) => repo.findProductByGtin(g.value).map(_.map(_ -> MatchMethod.Gtin))
      case None => Future.successful(None)
    }
    byGtin.flatMap {
      case Some((id, method)) =>
        canonical(id).map(c => Some(ResolutionOutcome.Matched(c, Confidence.Certain, method)))
      case None =>
        subject.listing match {
          case Some(k) =>
            repo.findProductByListing(k.storeId.value, k.externalId).flatMap {
              case Some(id) =>
                canonical(id).map(c =>
                  Some(ResolutionOutcome.Matched(c, Confidence.Certain, MatchMethod.Listing))
                )
              case None => Future.successful(None)
            }
          case None => Future.successful(None)
        }
    }
  }

  /** Always answer with the canonical id: ids never die, they forward (§6.5). */
  private def canonical(id: String): Future[String] =
    repo.resolveCanonical(id).map(_.getOrElse(id))

  private def byFuzzy(subject: MatchSubject): Future[ResolutionOutcome] = {
    val input = MatchInput.from(subject.name, subject.brand)
    repo.trigramCandidates(input.normalized, config.topK).map { rows =>
      val scored = rows
        .map { row =>
          val candidate = MatchInput(
            tokens = row.normalizedName.split(' ').toList.filter(_.nonEmpty),
            normalized = row.normalizedName,
            brand = row.brandNorm,
            size = quantityOf(row)
          )
          val s = Scorer.score(input, candidate, config.matching)
          ScoredCandidate(row.productId, s.confidence, s.notes)
        }
        .sortBy(-_.score.toDouble)

      scored.headOption match {
        case Some(best) if best.score.toDouble >= config.autoLink =>
          ResolutionOutcome.Matched(best.productId, best.score, MatchMethod.Fuzzy)
        case Some(best) if best.score.toDouble >= config.review =>
          // Ambiguous, so a human decides. Only candidates that actually reach the
          // review floor are offered — padding the list with near-zero scores would
          // make the review UI look like a lottery.
          ResolutionOutcome.Ambiguous(scored.filter(_.score.toDouble >= config.review))
        case _ => ResolutionOutcome.NoMatch
      }
    }
  }

  private def quantityOf(row: MatchCandidateRow): Option[Quantity] =
    for {
      amount <- row.sizeAmount
      unit <- row.sizeUnit.flatMap(u => scala.util.Try(MeasureUnit.valueOf(u)).toOption)
      q <- Quantity(amount, unit).toOption
    } yield q
}

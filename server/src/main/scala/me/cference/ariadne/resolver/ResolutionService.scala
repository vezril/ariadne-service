package me.cference.ariadne.resolver

import me.cference.ariadne.domain.{CorrelationId, Origin, ProductId}
import me.cference.ariadne.domain.product.{ProductCommand, ProductState}
import me.cference.ariadne.domain.resolution.{MatchSubject, ResolutionCommand, ResolutionId}
import me.cference.ariadne.persistence.{ProductEntity, ResolutionCaseEntity, Sharding}
import me.cference.ariadne.text.TextNormalizer
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

/**
 * Closes the loop between the matcher and the review queue.
 *
 * An `Ambiguous` result is not an answer — it is a question for a human, and until now it had
 * nowhere to go. This opens a ResolutionCase for it, which is what puts the subject in front of
 * ariadne-ui (§6.5).
 *
 * Note what is NOT done here: nothing is written against a guessed identity. A `Matched` result
 * links; an `Ambiguous` one only asks. That distinction is the whole reason the case aggregate
 * exists.
 */
final class ResolutionService(
    resolver: Resolver,
    system: ActorSystem[?]
)(using ec: ExecutionContext, timeout: Timeout) {

  /**
   * Resolve, and open a review case when the answer is ambiguous.
   *
   * The case id is derived from the subject rather than random, so the same unresolved listing seen
   * on a later scrape lands on the SAME case instead of filling the queue with duplicates of one
   * question. Proposing onto an existing case is refused by the aggregate, and that refusal is
   * expected here rather than an error.
   */
  def resolveAndReview(
      subject: MatchSubject,
      correlationId: CorrelationId
  ): Future[ResolutionOutcome] =
    resolver.resolve(subject).flatMap[ResolutionOutcome] {
      case ambiguous @ ResolutionOutcome.Ambiguous(candidates) =>
        val id = ResolutionService.caseIdFor(subject)
        val ref = Sharding.resolution(system, id.value)
        ref
          .askWithStatus[Done](
            ResolutionCaseEntity.Execute(
              ResolutionCommand.Propose(id, subject, candidates, correlationId),
              _
            )
          )
          .map(_ => ambiguous)
          // Already proposed: the same listing came round again on a later scrape. That
          // is the design working, not a failure — the queue must not grow one row per
          // scrape of the same unresolved thing.
          .recover { case _: StatusReply.ErrorMessage => ambiguous }
      case other => Future.successful(other)
    }

  /**
   * Path A of the §6.4 table, end to end — what a SCRAPED listing does with each outcome.
   *
   * `resolveAndReview` above answers the question; this one acts on the answer, and the three
   * branches are deliberately not symmetric:
   *
   *   - **Matched** — observe against the product. 1. **Ambiguous** (0.60–0.92) — a human has to
   *     decide, so nothing is observed. The case is opened and the price is skipped rather than
   *     attributed to a guess. 2. **NoMatch** (< 0.60) — this is NOT a review case. The catalog
   *     simply does not know this product yet, and a review queue full of "here is a thing you have
   *     never seen" is a queue nobody works. §6.4 says auto-create a Provisional product and let
   *     prices flow immediately; the provisional surfaces in a low-priority naming/merging lane
   *     instead of blocking the fact.
   *
   * The distinction that matters: ambiguity is a QUESTION (two products might be this), absence is
   * an ANSWER (none of them are). Only the question needs a human.
   */
  def resolveForScrape(
      subject: MatchSubject,
      scraper: String,
      correlationId: CorrelationId
  ): Future[Option[ProductId]] =
    resolveAndReview(subject, correlationId).flatMap {
      case ResolutionOutcome.Matched(productId, _, _) => Future.successful(Some(productId))
      case ResolutionOutcome.Ambiguous(_) => Future.successful(None)
      case ResolutionOutcome.NoMatch => provisional(subject, scraper, correlationId).map(Some(_))
    }

  /**
   * Mint (or re-find) the provisional product for a subject the catalog has never seen.
   *
   * The id is DERIVED from the normalised subject, not random, and that is load-bearing. Two things
   * would otherwise duplicate: a run that meets the same name twice, and — because the read model
   * that feeds the matcher is eventually consistent — the very next scrape, which can easily reach
   * the resolver before the product it just created is visible to it. A random id turns both into a
   * fresh product every time, which is precisely the catalog-of-duplicates outcome the resolver
   * exists to prevent.
   *
   * A second register under the same id is refused by the aggregate. That refusal is the SUCCESS
   * case here: it means the product already exists, which is exactly what was wanted.
   */
  private def provisional(
      subject: MatchSubject,
      scraper: String,
      correlationId: CorrelationId
  ): Future[ProductId] = {
    val id = ResolutionService.provisionalIdFor(subject)
    Sharding
      .product(system, id)
      .askWithStatus[Done](
        ProductEntity.Execute(
          ProductCommand.RegisterProduct(
            id = id,
            name = subject.name.trim,
            brand = subject.brand,
            category = None,
            size = None,
            gtin = subject.gtin,
            // No ListingKey: Flipp item ids change weekly (§2.6 quirk #4), so recording
            // one would be recording a key that stops matching within days.
            origin = Origin.Scrape(scraper, subject.listing),
            correlationId = correlationId
          ),
          _
        )
      )
      .map(_ => id)
      // A refusal is EXPECTED here — it normally means "already registered", which is
      // the outcome wanted. But the reply carries only a message string, so the refusal
      // cannot be told apart from a genuine one by its type. Rather than assume, ask:
      // if the product really does exist, the id is good; if it does not, the register
      // failed for some other reason and returning the id anyway would attribute prices
      // to a product that was never created.
      .recoverWith { case _: StatusReply.ErrorMessage =>
        Sharding
          .product(system, id)
          .ask[ProductState](ProductEntity.GetState(_))
          .map {
            case _: ProductState.Existing => id
            case ProductState.Empty =>
              throw new IllegalStateException(
                s"provisional product ${id.value} was refused AND does not exist — " +
                  "refusing to attribute prices to it"
              )
          }
      }
  }
}

object ResolutionService {

  /**
   * A stable id for "this subject, unresolved".
   *
   * Deliberately DERIVED, not random: identity of the QUESTION is what keeps repeat scrapes
   * collapsing onto one case instead of filling the review queue with one row per scrape of the
   * same unresolved listing.
   *
   * The key prefers the listing, because that is the thing actually being resolved — the retailer's
   * display text drifts between scrapes while its listing id does not.
   */
  def caseIdFor(subject: MatchSubject): ResolutionId = {
    val key = subject.listing
      .map(l => s"listing:${l.storeId.value}:${l.externalId}")
      .orElse(subject.gtin.map(g => s"gtin:${g.value}"))
      .getOrElse(s"name:${subject.name.trim.toLowerCase}:${subject.brand.getOrElse("")}")
    ResolutionId(java.util.UUID.nameUUIDFromBytes(key.getBytes("UTF-8")).toString)
  }

  /**
   * A stable id for "the provisional product for this subject".
   *
   * Keyed on the NORMALISED name rather than the raw one, so the same product advertised as
   * "Lactantia Butter 454g" and "LACTANTIA BUTTER, 454 G" collapses onto one provisional instead of
   * two. This is the same normaliser the matcher scores with, so the key agrees with the thing that
   * will later decide these are the same product.
   *
   * GTIN wins when present — it is identity, and two listings sharing one are the same product
   * whatever their display text says.
   */
  def provisionalIdFor(subject: MatchSubject): ProductId = {
    val key = subject.gtin
      .map(g => s"gtin:${g.value}")
      .getOrElse {
        val name = TextNormalizer.normalize(subject.name).joined
        s"name:$name:${subject.brand.map(b => TextNormalizer.normalize(b).joined).getOrElse("")}"
      }
    ProductId(s"prov-${java.util.UUID.nameUUIDFromBytes(key.getBytes("UTF-8"))}")
  }
}

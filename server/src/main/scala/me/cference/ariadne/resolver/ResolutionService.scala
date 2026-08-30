package me.cference.ariadne.resolver

import me.cference.ariadne.domain.CorrelationId
import me.cference.ariadne.domain.resolution.{MatchSubject, ResolutionCommand, ResolutionId}
import me.cference.ariadne.persistence.{ResolutionCaseEntity, Sharding}
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
}

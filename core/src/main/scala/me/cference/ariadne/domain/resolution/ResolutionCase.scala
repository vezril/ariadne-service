package me.cference.ariadne.domain
package resolution

import java.time.Instant

final case class ResolutionId(value: String) extends AnyVal

/** What we are trying to identify — a scraped listing, or a Dionysus ingredient. */
final case class MatchSubject(
    name: String,
    brand: Option[String] = None,
    gtin: Option[Gtin] = None,
    listing: Option[ListingKey] = None
)

/** One candidate the matcher offered, with the score the DOMAIN computed (§6.4). */
final case class ScoredCandidate(productId: ProductId, score: Confidence, notes: List[String])

/**
 * A price observation held BACK because identity is not settled (§6.4).
 *
 * The whole reason this exists: an ambiguous match must not record price facts against a guessed
 * identity. Parking keeps the observation — losing it would mean losing a real market fact — while
 * refusing to attribute it until a human says who it belongs to.
 */
final case class ParkedObservation(
    price: Money,
    observedAt: Instant,
    scope: PriceScope,
    priceConfidence: Confidence,
    sizeConfidence: Confidence
)

/** How a case ended. */
sealed trait ResolutionOutcome
object ResolutionOutcome {
  final case class Confirmed(productId: ProductId) extends ResolutionOutcome
  final case class NewProduct(productId: ProductId) extends ResolutionOutcome
  final case class MergedProducts(winner: ProductId, loser: ProductId) extends ResolutionOutcome
  final case class SplitOff(listing: ListingKey, newProduct: ProductId) extends ResolutionOutcome
  case object Rejected extends ResolutionOutcome
}

sealed trait ResolutionState
object ResolutionState {
  case object Empty extends ResolutionState
  final case class Pending(
      id: ResolutionId,
      subject: MatchSubject,
      candidates: List[ScoredCandidate],
      parked: List[ParkedObservation]
  ) extends ResolutionState
  final case class Resolved(
      id: ResolutionId,
      subject: MatchSubject,
      outcome: ResolutionOutcome,
      released: List[ParkedObservation]
  ) extends ResolutionState
}

sealed trait ResolutionCommand extends CborSerializable
object ResolutionCommand {
  final case class Propose(
      id: ResolutionId,
      subject: MatchSubject,
      candidates: List[ScoredCandidate],
      correlationId: CorrelationId
  ) extends ResolutionCommand
  final case class ParkObservation(observation: ParkedObservation, correlationId: CorrelationId)
      extends ResolutionCommand
  final case class Confirm(productId: ProductId, correlationId: CorrelationId)
      extends ResolutionCommand
  final case class Reject(newProductId: ProductId, correlationId: CorrelationId)
      extends ResolutionCommand
  final case class RequestMerge(winner: ProductId, loser: ProductId, correlationId: CorrelationId)
      extends ResolutionCommand
  final case class RequestSplit(
      listing: ListingKey,
      newProductId: ProductId,
      correlationId: CorrelationId
  ) extends ResolutionCommand
}

sealed trait ResolutionEvent extends CborSerializable
object ResolutionEvent {
  final case class ResolutionProposed(
      id: ResolutionId,
      subject: MatchSubject,
      candidates: List[ScoredCandidate]
  ) extends ResolutionEvent
  final case class ObservationParked(observation: ParkedObservation) extends ResolutionEvent
  final case class ResolutionConfirmed(productId: ProductId, released: List[ParkedObservation])
      extends ResolutionEvent
  final case class ResolutionRejected(newProductId: ProductId, released: List[ParkedObservation])
      extends ResolutionEvent
  final case class MergeRequested(winner: ProductId, loser: ProductId) extends ResolutionEvent
  final case class SplitRequested(listing: ListingKey, newProductId: ProductId)
      extends ResolutionEvent
}

/**
 * The workflow state of an ambiguous identity match (§2.5, §6.5).
 *
 * This aggregate exists so that human-review state stays OUT of Product. Product owns facts; a
 * half-made decision about what something IS is not a fact, and putting it there would be the
 * God-object trap in a different costume.
 *
 * Every terminal verb releases the parked observations. The releasing is this aggregate's answer;
 * actually appending them to the price streams is a process manager's job over this journal (§3) —
 * at-least-once and idempotent, rather than a fragile in-band side effect.
 */
object ResolutionCase {

  def decide(
      state: ResolutionState,
      cmd: ResolutionCommand
  ): Either[DomainError, List[ResolutionEvent]] =
    (state, cmd) match {
      case (ResolutionState.Empty, c: ResolutionCommand.Propose) =>
        if c.subject.name.isBlank then Left(DomainError.EmptyName)
        else Right(List(ResolutionEvent.ResolutionProposed(c.id, c.subject, c.candidates)))

      case (ResolutionState.Empty, _) => Left(DomainError.NotRegistered)

      case (_: ResolutionState.Pending, _: ResolutionCommand.Propose) =>
        Left(DomainError.AlreadyRegistered)

      case (s: ResolutionState.Pending, c) => decidePending(s, c)

      // A decided case is closed. Re-deciding it would silently re-release the parked
      // observations and double-record them, so a change of mind is a NEW case, not an
      // edit of this one — same stance as voiding a purchase rather than editing it.
      case (_: ResolutionState.Resolved, _) => Left(DomainError.AlreadyResolved)
    }

  private def decidePending(
      s: ResolutionState.Pending,
      cmd: ResolutionCommand
  ): Either[DomainError, List[ResolutionEvent]] =
    cmd match {
      case _: ResolutionCommand.Propose => Left(DomainError.AlreadyRegistered)

      case c: ResolutionCommand.ParkObservation =>
        Right(List(ResolutionEvent.ObservationParked(c.observation)))

      case c: ResolutionCommand.Confirm =>
        // Confirm means "this one, of the ones offered". Picking something that was
        // never a candidate is not a confirmation — it is a different decision, and
        // Reject is the verb for it.
        if !s.candidates.exists(_.productId == c.productId) then
          Left(DomainError.NotACandidate(c.productId))
        else Right(List(ResolutionEvent.ResolutionConfirmed(c.productId, s.parked)))

      case c: ResolutionCommand.Reject =>
        Right(List(ResolutionEvent.ResolutionRejected(c.newProductId, s.parked)))

      case c: ResolutionCommand.RequestMerge =>
        if c.winner == c.loser then Left(DomainError.CannotMergeIntoSelf)
        else Right(List(ResolutionEvent.MergeRequested(c.winner, c.loser)))

      case c: ResolutionCommand.RequestSplit =>
        Right(List(ResolutionEvent.SplitRequested(c.listing, c.newProductId)))
    }

  def evolve(state: ResolutionState, event: ResolutionEvent): ResolutionState =
    (state, event) match {
      case (ResolutionState.Empty, e: ResolutionEvent.ResolutionProposed) =>
        ResolutionState.Pending(e.id, e.subject, e.candidates, Nil)
      case (ResolutionState.Empty, _) => ResolutionState.Empty

      case (s: ResolutionState.Pending, e) =>
        e match {
          case _: ResolutionEvent.ResolutionProposed => s
          case e: ResolutionEvent.ObservationParked => s.copy(parked = s.parked :+ e.observation)
          case e: ResolutionEvent.ResolutionConfirmed =>
            ResolutionState.Resolved(
              s.id,
              s.subject,
              ResolutionOutcome.Confirmed(e.productId),
              e.released
            )
          case e: ResolutionEvent.ResolutionRejected =>
            ResolutionState.Resolved(
              s.id,
              s.subject,
              ResolutionOutcome.NewProduct(e.newProductId),
              e.released
            )
          case e: ResolutionEvent.MergeRequested =>
            ResolutionState.Resolved(
              s.id,
              s.subject,
              ResolutionOutcome.MergedProducts(e.winner, e.loser),
              s.parked
            )
          case e: ResolutionEvent.SplitRequested =>
            ResolutionState.Resolved(
              s.id,
              s.subject,
              ResolutionOutcome.SplitOff(e.listing, e.newProductId),
              s.parked
            )
        }

      case (s: ResolutionState.Resolved, _) => s
    }

  def replay(events: List[ResolutionEvent]): ResolutionState =
    events.foldLeft[ResolutionState](ResolutionState.Empty)(evolve)
}

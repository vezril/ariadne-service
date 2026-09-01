package me.cference.ariadne.domain
package purchase

import java.time.Instant

/**
 * How a purchase got into the catalog (§7). All three paths converge on the same `RecordPurchase`
 * command, so receipt scan and bank import are later ADAPTERS, not later models.
 */
sealed trait PurchaseSource

object PurchaseSource {
  case object Manual extends PurchaseSource
  final case class Receipt(blobId: String) extends PurchaseSource
  final case class BankImport(reference: String) extends PurchaseSource
}

final case class PurchaseLine(
    productId: ProductId,
    quantity: BigDecimal,
    pricePaid: Money,
    lineTotal: Money
)

enum PurchaseState {
  case Empty
  case Recorded(
      id: PurchaseId,
      storeId: StoreId,
      purchasedAt: Instant,
      lines: List[PurchaseLine],
      total: Money,
      source: PurchaseSource,
      voided: Boolean
  )
}

sealed trait PurchaseCommand extends CborSerializable

object PurchaseCommand {
  final case class RecordPurchase(
      id: PurchaseId,
      storeId: StoreId,
      purchasedAt: Instant,
      lines: List[PurchaseLine],
      total: Money,
      source: PurchaseSource,
      correlationId: CorrelationId
  ) extends PurchaseCommand
  final case class VoidPurchase(reason: String, correlationId: CorrelationId)
      extends PurchaseCommand
}

sealed trait PurchaseEvent extends CborSerializable

object PurchaseEvent {
  final case class PurchaseRecorded(
      id: PurchaseId,
      storeId: StoreId,
      purchasedAt: Instant,
      lines: List[PurchaseLine],
      total: Money,
      source: PurchaseSource
  ) extends PurchaseEvent
  final case class PurchaseVoided(reason: String) extends PurchaseEvent
}

/**
 * An immutable fact: one `PurchaseRecorded` per receipt.
 *
 * A mistake is voided and re-recorded — both events persist, because the audit trail IS the point.
 * Prices actually paid are the highest-quality price facts the catalog ever gets, so a purchase
 * feeds price history via a process manager (§2.4/§3), not via a fragile in-band side effect.
 */
object Purchase {

  def decide(
      state: PurchaseState,
      cmd: PurchaseCommand,
      now: Instant
  ): Either[DomainError, List[PurchaseEvent]] =
    (state, cmd) match {
      case (PurchaseState.Empty, c: PurchaseCommand.RecordPurchase) =>
        if c.lines.isEmpty then Left(DomainError.EmptyPurchase)
        else if c.purchasedAt.isAfter(now) then
          Left(DomainError.PurchaseInFuture(c.purchasedAt.toString))
        else
          // The stated total must agree with the lines. A receipt whose parts do
          // not sum to its whole is a parse failure, and recording it would put a
          // number into budgeting that no till ever printed.
          Money.sum(c.lines.map(_.lineTotal)).flatMap {
            case Some(computed) if computed != c.total =>
              Left(DomainError.PurchaseTotalMismatch(c.total.toString, computed.toString))
            case _ =>
              Right(
                List(
                  PurchaseEvent
                    .PurchaseRecorded(c.id, c.storeId, c.purchasedAt, c.lines, c.total, c.source)
                )
              )
          }

      case (PurchaseState.Empty, _) => Left(DomainError.NotRegistered)

      case (s: PurchaseState.Recorded, c: PurchaseCommand.RecordPurchase) =>
        // Re-recording the SAME purchase is a no-op success, not an error.
        //
        // Ariadne being unreachable cannot block someone typing a receipt: a dropped
        // receipt is lost user data, and unlike a missing price it can never be
        // reconstructed — the flyer feed will never contain what Calvin actually paid. So
        // the caller buffers locally and retries, which means a retry of an already-
        // recorded purchase MUST look like success or the outbox can never drain: an
        // error is indistinguishable from a genuine failure and the row retries forever.
        //
        // A DIFFERENT purchase arriving under an id that is already taken is still a real
        // error — that is an id collision, not a retry, and silently accepting it would
        // overwrite one receipt with another.
        if sameAs(s, c) then Right(Nil)
        else Left(DomainError.AlreadyRegistered)

      case (s: PurchaseState.Recorded, c: PurchaseCommand.VoidPurchase) =>
        if s.voided then Left(DomainError.AlreadyVoided)
        else Right(List(PurchaseEvent.PurchaseVoided(c.reason)))
    }

  /** Content equality, so a retry is recognised without trusting the caller to say so. */
  private def sameAs(s: PurchaseState.Recorded, c: PurchaseCommand.RecordPurchase): Boolean =
    s.storeId == c.storeId &&
      s.purchasedAt == c.purchasedAt &&
      s.total == c.total &&
      s.source == c.source &&
      s.lines == c.lines

  def evolve(state: PurchaseState, event: PurchaseEvent): PurchaseState =
    (state, event) match {
      case (PurchaseState.Empty, e: PurchaseEvent.PurchaseRecorded) =>
        PurchaseState.Recorded(
          e.id,
          e.storeId,
          e.purchasedAt,
          e.lines,
          e.total,
          e.source,
          voided = false
        )
      case (PurchaseState.Empty, _) => PurchaseState.Empty
      case (s: PurchaseState.Recorded, _: PurchaseEvent.PurchaseRecorded) => s
      case (s: PurchaseState.Recorded, _: PurchaseEvent.PurchaseVoided) => s.copy(voided = true)
    }

  def replay(events: List[PurchaseEvent]): PurchaseState =
    events.foldLeft[PurchaseState](PurchaseState.Empty)(evolve)
}

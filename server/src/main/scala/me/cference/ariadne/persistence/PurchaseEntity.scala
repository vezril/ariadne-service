package me.cference.ariadne.persistence

import me.cference.ariadne.domain.purchase.{Purchase, PurchaseCommand, PurchaseEvent, PurchaseState}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

import java.time.Instant

/**
 * An immutable purchase fact (§2.4). A mistake is voided and re-recorded; both events persist,
 * because the audit trail is the point.
 */
object PurchaseEntity {

  val EntityPrefix = "purchase"

  val TypeKey: org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command] =
    org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command](EntityPrefix)

  sealed trait Command extends me.cference.ariadne.domain.CborSerializable
  final case class Execute(
      command: PurchaseCommand,
      now: Instant,
      replyTo: ActorRef[StatusReply[Done]]
  ) extends Command
  final case class GetState(replyTo: ActorRef[PurchaseState]) extends Command

  def persistenceId(id: String): PersistenceId = PersistenceId.ofUniqueId(s"$EntityPrefix|$id")

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, PurchaseEvent, PurchaseState](
      persistenceId = persistenceId(id),
      emptyState = PurchaseState.Empty,
      commandHandler = handle,
      eventHandler = Purchase.evolve
    ).withTagger(_ => Set(Tags.Purchase))

  private def handle(state: PurchaseState, command: Command): Effect[PurchaseEvent, PurchaseState] =
    command match {
      case Execute(domain, now, replyTo) =>
        Purchase.decide(state, domain, now) match {
          case Right(events) => Effect.persist(events).thenReply(replyTo)(_ => StatusReply.ack())
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(error.message))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

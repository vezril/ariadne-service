package me.cference.ariadne.persistence

import me.cference.ariadne.domain.store.{Store, StoreCommand, StoreEvent, StoreState}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

/** The franchise reference aggregate (§2.2). Also the source of the store-coverage projection. */
object StoreEntity {

  val EntityPrefix = "store"

  sealed trait Command
  final case class Execute(command: StoreCommand, replyTo: ActorRef[StatusReply[Done]])
      extends Command
  final case class GetState(replyTo: ActorRef[StoreState]) extends Command

  def persistenceId(id: String): PersistenceId = PersistenceId.ofUniqueId(s"$EntityPrefix|$id")

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, StoreEvent, StoreState](
      persistenceId = persistenceId(id),
      emptyState = StoreState.Empty,
      commandHandler = handle,
      eventHandler = Store.evolve
    ).withTagger(_ => Set(Tags.Store))

  private def handle(state: StoreState, command: Command): Effect[StoreEvent, StoreState] =
    command match {
      case Execute(domain, replyTo) =>
        Store.decide(state, domain) match {
          case Right(events) => Effect.persist(events).thenReply(replyTo)(_ => StatusReply.ack())
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(DomainException(error)))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

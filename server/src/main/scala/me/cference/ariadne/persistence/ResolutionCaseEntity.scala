package me.cference.ariadne.persistence

import me.cference.ariadne.domain.resolution.{
  ResolutionCase,
  ResolutionCommand,
  ResolutionEvent,
  ResolutionState
}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

/**
 * The review workflow for an ambiguous match (§2.5, §6.5).
 *
 * Kept as its own aggregate so that half-made decisions about what something IS never land in
 * Product. Product owns facts; "a human is still thinking about this" is not one, and storing it
 * there would be the God-object trap in a different costume.
 */
object ResolutionCaseEntity {

  val EntityPrefix = "resolution"

  val TypeKey: org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command] =
    org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command](EntityPrefix)

  sealed trait Command extends me.cference.ariadne.domain.CborSerializable
  final case class Execute(command: ResolutionCommand, replyTo: ActorRef[StatusReply[Done]])
      extends Command
  final case class GetState(replyTo: ActorRef[ResolutionState]) extends Command

  def persistenceId(id: String): PersistenceId = PersistenceId.ofUniqueId(s"$EntityPrefix|$id")

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, ResolutionEvent, ResolutionState](
      persistenceId = persistenceId(id),
      emptyState = ResolutionState.Empty,
      commandHandler = handle,
      eventHandler = ResolutionCase.evolve
    ).withTagger(_ => Set(Tags.Resolution))

  private def handle(
      state: ResolutionState,
      command: Command
  ): Effect[ResolutionEvent, ResolutionState] =
    command match {
      case Execute(domain, replyTo) =>
        ResolutionCase.decide(state, domain) match {
          case Right(events) => Effect.persist(events).thenReply(replyTo)(_ => StatusReply.ack())
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(error.message))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

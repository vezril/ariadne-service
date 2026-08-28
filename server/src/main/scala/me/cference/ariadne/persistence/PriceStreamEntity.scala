package me.cference.ariadne.persistence

import me.cference.ariadne.domain.{PriceScope, ProductId}
import me.cference.ariadne.domain.price.{
  PriceCommand,
  PriceEvent,
  PriceObservation,
  PriceStreamState
}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

import java.time.Instant

/**
 * One append-only price stream per product x scope (§2.3.1).
 *
 * The persistence id is `price|{productId}|{scope}` — NOT `price|{productId}|{storeId}`. The
 * difference is the whole point of §2.3.1: a flyer observation is scoped to a chain and region and
 * gets its own stream, rather than being fanned onto every member store at write time, which would
 * fabricate one fact per store.
 *
 * `now` is passed in rather than read from a clock so the domain stays pure; the entity is the
 * boundary where real time enters.
 */
object PriceStreamEntity {

  val EntityPrefix = "price"

  sealed trait Command
  final case class Execute(
      command: PriceCommand,
      now: Instant,
      replyTo: ActorRef[StatusReply[Done]]
  ) extends Command
  final case class GetState(replyTo: ActorRef[PriceStreamState]) extends Command

  def persistenceId(productId: ProductId, scope: PriceScope): PersistenceId =
    PersistenceId.ofUniqueId(s"$EntityPrefix|${productId.value}|${scope.key}")

  def apply(productId: ProductId, scope: PriceScope): Behavior[Command] =
    EventSourcedBehavior[Command, PriceEvent, PriceStreamState](
      persistenceId = persistenceId(productId, scope),
      emptyState = PriceStreamState.Empty,
      commandHandler = handle,
      eventHandler = PriceObservation.evolve
    ).withTagger(_ => Set(Tags.Price))

  private def handle(
      state: PriceStreamState,
      command: Command
  ): Effect[PriceEvent, PriceStreamState] =
    command match {
      case Execute(domain, now, replyTo) =>
        PriceObservation.decide(state, domain, now) match {
          case Right(events) => Effect.persist(events).thenReply(replyTo)(_ => StatusReply.ack())
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(DomainException(error)))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

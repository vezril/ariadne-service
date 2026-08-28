package me.cference.ariadne.persistence

import me.cference.ariadne.domain.DomainError
import me.cference.ariadne.domain.product.{Product, ProductCommand, ProductEvent, ProductState}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

/**
 * Wraps the pure `Product.decide`/`evolve` in an `EventSourcedBehavior`.
 *
 * The entity adds persistence and nothing else: no rules live here, so the domain stays testable
 * without a journal and the runtime stays substitutable without touching the domain.
 */
object ProductEntity {

  val EntityPrefix = "product"

  sealed trait Command
  final case class Execute(command: ProductCommand, replyTo: ActorRef[StatusReply[Done]])
      extends Command
  final case class GetState(replyTo: ActorRef[ProductState]) extends Command

  def persistenceId(id: String): PersistenceId = PersistenceId.ofUniqueId(s"$EntityPrefix|$id")

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, ProductEvent, ProductState](
      persistenceId = persistenceId(id),
      emptyState = ProductState.Empty,
      commandHandler = handle,
      eventHandler = Product.evolve
    ).withTagger(_ => Set(Tags.Product))

  private def handle(state: ProductState, command: Command): Effect[ProductEvent, ProductState] =
    command match {
      case Execute(domain, replyTo) =>
        Product.decide(state, domain) match {
          // An empty event list is a legitimate no-op (a repeated scrape), not a failure —
          // it must still acknowledge, or every duplicate would surface as an error.
          case Right(events) => Effect.persist(events).thenReply(replyTo)(_ => StatusReply.ack())
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(DomainException(error)))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

/** Carries a `DomainError` across the ask boundary without flattening it to a string. */
final case class DomainException(error: DomainError) extends RuntimeException(error.message)

/** Projection tags (DESIGN §3) — the axis each read model subscribes to. */
object Tags {
  val Product = "product"
  val Store = "store"
  val Price = "price"
  val Purchase = "purchase"
}

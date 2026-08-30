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

  val TypeKey: org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command] =
    org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey[Command](EntityPrefix)

  sealed trait Command extends me.cference.ariadne.domain.CborSerializable
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
          case Left(error) => Effect.reply(replyTo)(StatusReply.error(error.message))
        }
      case GetState(replyTo) => Effect.reply(replyTo)(state)
    }
}

/**
 * A rejection reply carries the error's MESSAGE, not the error object.
 *
 * Found by turning on command verification once sharding landed: replies cross node boundaries too,
 * and a `RuntimeException` wrapping a `DomainError` is not serializable — Java serialization is
 * disabled, deliberately. `StatusReply.error(String)` is serializable natively.
 *
 * What this trades: a caller gets a sentence, not a typed error it can branch on. Nothing needs
 * that yet — `decide` still returns the full `DomainError` to everything inside the service, and
 * only the reply is flattened. When the REST surface needs to map failures onto status codes, the
 * fix is a serializable reply ADT rather than reviving an exception, and this is the note saying so
 * was a choice.
 */

/** Projection tags (DESIGN §3) — the axis each read model subscribes to. */
object Tags {
  val Product = "product"
  val Store = "store"
  val Price = "price"
  val Purchase = "purchase"
  val Resolution = "resolution"
}

package me.cference.ariadne.projection

import me.cference.ariadne.domain.{Confidence, PriceScope, ProductId}
import me.cference.ariadne.domain.price.{PriceCommand, PriceEvent, PriceSource}
import me.cference.ariadne.domain.purchase.PurchaseEvent
import me.cference.ariadne.domain.product.ProductEvent
import me.cference.ariadne.domain.store.StoreEvent
import me.cference.ariadne.domain.resolution.ResolutionEvent
import me.cference.ariadne.hermes.{CatalogMessages, CatalogPublisher}
import me.cference.ariadne.persistence.{
  PriceStreamEntity,
  ProductEntity,
  PurchaseEntity,
  ResolutionCaseEntity,
  StoreEntity
}
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardedDaemonProcessSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.persistence.query.typed.EventEnvelope
import org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import org.apache.pekko.projection.eventsourced.scaladsl.EventSourcedProvider
import org.apache.pekko.projection.r2dbc.scaladsl.{R2dbcHandler, R2dbcProjection, R2dbcSession}
import org.apache.pekko.projection.scaladsl.SourceProvider
import org.apache.pekko.projection.{Projection, ProjectionBehavior, ProjectionId}
import org.apache.pekko.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.immutable

/**
 * Wires each read model to the journal (DESIGN §3).
 *
 * One projection per entity type, each split into slice ranges and run under `ShardedDaemonProcess`
 * so instances are supervised and restart-safe — and would distribute across nodes if this ever
 * became a multi-node cluster.
 *
 * OFFSETS COMMIT AFTER the handler succeeds, which is what makes delivery at-least-once rather than
 * at-most-once: a crash between applying and committing replays the event. Every statement the
 * repository runs is idempotent precisely so that replay is invisible.
 */
object AriadneProjections {

  private def ranges(n: Int)(using system: ActorSystem[?]): immutable.Seq[Range] =
    EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, n)

  private def provider[E](entityType: String, r: Range)(using
      system: ActorSystem[?]
  ): SourceProvider[Offset, EventEnvelope[E]] =
    EventSourcedProvider
      .eventsBySlices[E](system, R2dbcReadJournal.Identifier, entityType, r.min, r.max)

  /** Adapts a plain (persistenceId, seqNr, event) function into an R2dbc handler. */
  final private class Handler[E](f: (String, Long, E) => Future[Unit])(using ec: ExecutionContext)
      extends R2dbcHandler[EventEnvelope[E]] {
    def process(session: R2dbcSession, envelope: EventEnvelope[E]): Future[Done] =
      f(envelope.persistenceId, envelope.sequenceNr, envelope.event).map(_ => Done)
  }

  def productProjection(repo: ReadModelRepository, r: Range)(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Projection[EventEnvelope[ProductEvent]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId("product-catalog", s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[ProductEvent](ProductEntity.EntityPrefix, r),
      handler =
        () => new Handler[ProductEvent]((pid, _, e) => ProjectionHandlers.product(repo)(pid, e))
    )

  def storeProjection(repo: ReadModelRepository, r: Range)(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Projection[EventEnvelope[StoreEvent]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId("store-coverage", s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[StoreEvent](StoreEntity.EntityPrefix, r),
      handler = () => new Handler[StoreEvent]((pid, _, e) => ProjectionHandlers.store(repo)(pid, e))
    )

  def priceProjection(repo: ReadModelRepository, r: Range)(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Projection[EventEnvelope[PriceEvent]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId("price-history", s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[PriceEvent](PriceStreamEntity.EntityPrefix, r),
      // The price handler needs the sequence number: it is half of the
      // (persistence_id, seq_nr) key that makes re-delivery a no-op.
      handler =
        () => new Handler[PriceEvent]((pid, seq, e) => ProjectionHandlers.price(repo)(pid, seq, e))
    )

  /**
   * The Hermes publishers (§5) — an event-sourced outbox, not a dual write.
   *
   * One projection per entity type, because `eventsBySlices` subscribes by entity type; and each
   * carries its OWN `ProjectionId`, which is the part that matters operationally. Publisher offsets
   * must never share a projection id with a read-model offset: Hermes going down would then stall
   * the read models too, and the REST surface would go stale because a broker was unreachable.
   * Separate offsets mean a Hermes outage lags exactly one thing.
   *
   * The offset advances only AFTER a successful publish, which is what makes this at-least-once
   * rather than at-most-once — a crash in that window republishes, and the broker collapses the
   * duplicate on the idempotency key.
   */
  def hermesProjection[E](
      name: String,
      entityType: String,
      publisher: CatalogPublisher,
      map: (String, Long, E) => Option[CatalogMessages.Outgoing],
      r: Range
  )(using system: ActorSystem[?], ec: ExecutionContext): Projection[EventEnvelope[E]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId(name, s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[E](entityType, r),
      handler = () =>
        new Handler[E]((pid, seq, e) =>
          // An event with no message is a deliberate silence (internal bookkeeping no
          // consumer asked for), and must still advance the offset — treating it as
          // unpublished would wedge the projection on the first one.
          map(pid, seq, e).fold(Future.unit)(publisher.publish)
        )
    )

  def resolutionProjection(repo: ReadModelRepository, r: Range)(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Projection[EventEnvelope[ResolutionEvent]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId("review-queue", s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[ResolutionEvent](ResolutionCaseEntity.EntityPrefix, r),
      handler = () =>
        new Handler[ResolutionEvent]((pid, _, e) => ProjectionHandlers.resolution(repo)(pid, e))
    )

  /**
   * The price-append process manager (§2.4): *whenever PurchaseRecorded then observe each line's
   * actual price paid*.
   *
   * A price actually paid is the best price fact the catalog ever gets — a flyer states an
   * intention, a receipt states what a till charged — so the policy is standing, not optional.
   *
   * A PROJECTION rather than an in-band side effect inside the purchase aggregate, which is the
   * whole reason §2.4 specifies it this way. Writing price streams from the purchase's own command
   * handler would make recording a receipt fail when an unrelated price entity is unreachable, and
   * a dropped receipt is unrecoverable user data (§7.1). Here the purchase is already committed
   * before any price is appended, so the worst case is lag.
   *
   * At-least-once is safe without bookkeeping of its own: a replayed `PurchaseRecorded` reissues
   * identical `ObservePrice` commands, and `PriceObservation.decide` drops a same-price,
   * same-source, same-day repeat as a no-op. The source is `Purchase(id)` — unique per receipt — so
   * that dedup cannot collapse a receipt into an unrelated scrape carrying the same number.
   *
   * Scope is `Exact(storeId)` and never regional: a receipt is the one price fact that genuinely
   * names an individual franchise, which is exactly what §2.3.1 says a flyer cannot do.
   *
   * Lines are appended in ORDER rather than in parallel. Two lines for the same product in one
   * receipt land on the same entity, and concurrent asks there would race the dedup check — the
   * second could read a state that does not yet include the first.
   */
  def purchasePriceProjection(
      observe: (ProductId, PriceScope, PriceCommand) => Future[Unit],
      r: Range
  )(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Projection[EventEnvelope[PurchaseEvent]] =
    R2dbcProjection.exactlyOnce(
      projectionId = ProjectionId("purchase-price-append", s"${r.min}-${r.max}"),
      settings = None,
      sourceProvider = provider[PurchaseEvent](PurchaseEntity.EntityPrefix, r),
      handler = () => new Handler[PurchaseEvent]((_, _, e) => appendPrices(observe)(e))
    )

  /**
   * The policy itself, separated from the projection that drives it so it can be tested without a
   * journal — the `ProjectionHandlers` pattern. What the handler decides is the interesting part;
   * that R2dbcProjection delivers events is Pekko's problem, not this policy's.
   */
  def appendPrices(
      observe: (ProductId, PriceScope, PriceCommand) => Future[Unit]
  )(event: PurchaseEvent)(using ec: ExecutionContext): Future[Unit] =
    event match {
      case p: PurchaseEvent.PurchaseRecorded =>
        val scope = PriceScope.Exact(p.storeId)
        p.lines.foldLeft(Future.unit) { (prior, line) =>
          prior.flatMap(_ =>
            observe(
              line.productId,
              scope,
              PriceCommand.ObservePrice(
                productId = line.productId,
                scope = scope,
                price = line.pricePaid,
                observedAt = p.purchasedAt,
                source = PriceSource.Purchase(p.id),
                unitPrice = None,
                promo = None,
                // The price is what a till charged, so nothing about it is uncertain.
                // Size confidence is Certain for a different reason: the line names a
                // ProductId outright, so no size was PARSED here and there is no parsing
                // risk for Demeter to inherit (§2.3).
                priceConfidence = Confidence.Certain,
                sizeConfidence = Confidence.Certain,
                correlationId = p.correlationId
              )
            )
          )
        }
      // A void does NOT retract the price facts the purchase produced. The price paid was
      // still paid; voiding corrects the PURCHASE record (a mistyped total, the wrong
      // store), and retracting history on that basis would delete a fact Demeter may
      // already have scored against. A genuinely wrong price is retracted explicitly
      // through RetractObservation.
      case _: PurchaseEvent.PurchaseVoided => Future.unit
    }

  /**
   * Every projection this service runs, in one list.
   *
   * The list is the point. `resolutionProjection` was defined and never started — `init` had three
   * hand-written `ShardedDaemonProcess.init` blocks and the review queue was not one of them, so
   * cases were journaled correctly and never projected, and `GET /api/v1/resolutions` answered
   * empty forever. It shipped in v0.1.0. Every test passed, because they all invoke the HANDLERS
   * directly; nothing asserted the set of daemons that actually start.
   *
   * Defining and starting are now the same act. A projection that is not in this list does not
   * exist, rather than existing and quietly doing nothing.
   */
  def definitions(
      repo: ReadModelRepository,
      hermes: Option[(CatalogPublisher, CatalogMessages.Topics)] = None,
      observe: Option[(ProductId, PriceScope, PriceCommand) => Future[Unit]] = None
  )(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): List[ProjectionDef] = {
    val readModels = List(
      ProjectionDef("product-catalog", r => ProjectionBehavior(productProjection(repo, r))),
      ProjectionDef("store-coverage", r => ProjectionBehavior(storeProjection(repo, r))),
      ProjectionDef("price-history", r => ProjectionBehavior(priceProjection(repo, r))),
      ProjectionDef("review-queue", r => ProjectionBehavior(resolutionProjection(repo, r)))
    )
    // The process manager needs to SEND commands, so it takes the sharding ask rather
    // than the repository. Optional only so tests can build the list without a cluster;
    // in the running service it is always present, and §2.4's policy is not optional.
    val processManagers = observe.toList.map { ask =>
      ProjectionDef(
        "purchase-price-append",
        r => ProjectionBehavior(purchasePriceProjection(ask, r))
      )
    }
    // The publishers are present only when a broker is configured. Read models are what
    // the service IS; publishing is what it tells other services, and the first must not
    // depend on the second.
    //
    // §8: the journaled correlationId rides every published message, so a
    // scrape -> resolve -> observe -> deal-alert chain is traceable across services. The
    // id is read off the event itself rather than threaded in from the caller — all three
    // event traits declare `def correlationId`, so there is no event that can reach a
    // publisher without one, and the projection cannot accidentally publish a different
    // id than the journal recorded.
    val publishers = hermes.toList.flatMap { case (client, topics) =>
      List(
        ProjectionDef(
          "hermes-product",
          r =>
            ProjectionBehavior(
              hermesProjection[ProductEvent](
                "hermes-product",
                ProductEntity.EntityPrefix,
                client,
                (
                    pid,
                    seq,
                    e
                ) => CatalogMessages.product(topics, pid, seq, e.correlationId.value, e),
                r
              )
            )
        ),
        ProjectionDef(
          "hermes-price",
          r =>
            ProjectionBehavior(
              hermesProjection[PriceEvent](
                "hermes-price",
                PriceStreamEntity.EntityPrefix,
                client,
                (pid, seq, e) => CatalogMessages.price(topics, pid, seq, e.correlationId.value, e),
                r
              )
            )
        ),
        ProjectionDef(
          "hermes-purchase",
          r =>
            ProjectionBehavior(
              hermesProjection[PurchaseEvent](
                "hermes-purchase",
                PurchaseEntity.EntityPrefix,
                client,
                (pid, seq, e) =>
                  CatalogMessages.purchase(topics, pid, seq, e.correlationId.value, e),
                r
              )
            )
        )
      )
    }
    readModels ::: processManagers ::: publishers
  }

  /** Start every projection under ShardedDaemonProcess. Call once at boot. */
  def init(
      repo: ReadModelRepository,
      hermes: Option[(CatalogPublisher, CatalogMessages.Topics)] = None,
      observe: Option[(ProductId, PriceScope, PriceCommand) => Future[Unit]] = None,
      instances: Int = 4
  )(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Unit = {
    val rs = ranges(instances)
    definitions(repo, hermes, observe).foreach { d =>
      ShardedDaemonProcess(system).init(
        d.name,
        rs.size,
        i => d.behavior(rs(i)),
        ShardedDaemonProcessSettings(system),
        Some(ProjectionBehavior.Stop)
      )
    }
  }

  /**
   * One projection, named and startable.
   *
   * The behaviour is built per slice range and its event type is erased here deliberately: the four
   * projections fold different events, and the only thing `init` needs from them is a `Behavior` it
   * can supervise. Erasing at this boundary is what lets them live in one list at all.
   */
  final case class ProjectionDef(
      name: String,
      behavior: Range => Behavior[ProjectionBehavior.Command]
  )
}

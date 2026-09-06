package me.cference.ariadne.projection

import me.cference.ariadne.domain.price.PriceEvent
import me.cference.ariadne.domain.product.ProductEvent
import me.cference.ariadne.domain.store.StoreEvent
import me.cference.ariadne.domain.resolution.ResolutionEvent
import me.cference.ariadne.persistence.{
  PriceStreamEntity,
  ProductEntity,
  ResolutionCaseEntity,
  StoreEntity
}
import org.apache.pekko.actor.typed.ActorSystem
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

  /** Start every projection under ShardedDaemonProcess. Call once at boot. */
  def init(repo: ReadModelRepository, instances: Int = 4)(using
      system: ActorSystem[?],
      ec: ExecutionContext
  ): Unit = {
    val rs = ranges(instances)
    ShardedDaemonProcess(system).init(
      "product-catalog",
      rs.size,
      i => ProjectionBehavior(productProjection(repo, rs(i))),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop)
    )
    ShardedDaemonProcess(system).init(
      "store-coverage",
      rs.size,
      i => ProjectionBehavior(storeProjection(repo, rs(i))),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop)
    )
    ShardedDaemonProcess(system).init(
      "price-history",
      rs.size,
      i => ProjectionBehavior(priceProjection(repo, rs(i))),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop)
    )
  }
}

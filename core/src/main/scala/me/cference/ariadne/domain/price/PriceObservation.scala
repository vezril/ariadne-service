package me.cference.ariadne.domain
package price

import java.time.{Instant, ZoneId}

/**
 * Where a price fact came from. Ordered by how much we trust it: a price actually PAID beats a
 * flyer's claim about a price.
 */
sealed trait PriceSource

object PriceSource {
  final case class Scrape(scraper: String) extends PriceSource
  final case class Purchase(purchaseId: PurchaseId) extends PriceSource
  case object Manual extends PriceSource
  final case class Backfill(origin: String) extends PriceSource
}

/**
 * The stream's state is only what validation needs — this aggregate IS its event stream, so there
 * is nothing to fold up beyond the dedup window.
 */
final case class LastObservation(at: Instant, price: Money, source: PriceSource)

enum PriceStreamState {
  case Empty
  case Open(productId: ProductId, scope: PriceScope, last: Option[LastObservation], count: Long)
}

enum PriceCommand {
  case ObservePrice(
      productId: ProductId,
      scope: PriceScope,
      price: Money,
      observedAt: Instant,
      source: PriceSource,
      unitPrice: Option[UnitPrice],
      promo: Option[PromoFlag],
      priceConfidence: Confidence,
      sizeConfidence: Confidence,
      correlationId: CorrelationId
  )
  case RetractObservation(observedAt: Instant, reason: String, correlationId: CorrelationId)
}

sealed trait PriceEvent extends CborSerializable

object PriceEvent {
  final case class PriceObserved(
      productId: ProductId,
      scope: PriceScope,
      price: Money,
      unitPrice: Option[UnitPrice],
      promo: Option[PromoFlag],
      priceConfidence: Confidence,
      sizeConfidence: Confidence,
      observedAt: Instant,
      source: PriceSource
  ) extends PriceEvent
  final case class PriceObservationRetracted(observedAt: Instant, reason: String) extends PriceEvent
}

/**
 * An append-only stream of price facts for one product x store pair.
 *
 * There is no update and no delete. A wrong observation is corrected by a later
 * `PriceObservationRetracted`, never by mutation — because Demeter scores against this history, and
 * history that silently rewrites itself is not history.
 *
 * Both confidences ride every event and are CONTRACT-REQUIRED (§2.3). Demeter computes
 * `matchConfidence = split.confidence.min(sizeConfidence)`; size parsing lives here while match
 * judgment lives there, so dropping sizeConfidence would make Demeter's confidence silently read
 * too high.
 */
object PriceObservation {

  /** Canadian retail; the dedup window is a *calendar* day, so it needs a zone. */
  val DefaultZone: ZoneId = ZoneId.of("America/Toronto")

  def decide(
      state: PriceStreamState,
      cmd: PriceCommand,
      now: Instant,
      zone: ZoneId = DefaultZone
  ): Either[DomainError, List[PriceEvent]] =
    cmd match {
      case c: PriceCommand.ObservePrice =>
        if c.observedAt.isAfter(now) then
          Left(DomainError.ObservationInFuture(c.observedAt.toString))
        else if isDuplicate(state, c, zone) then Right(Nil)
        else
          Right(
            List(
              PriceEvent.PriceObserved(
                c.productId,
                c.scope,
                c.price,
                c.unitPrice,
                c.promo,
                c.priceConfidence,
                c.sizeConfidence,
                c.observedAt,
                c.source
              )
            )
          )

      case c: PriceCommand.RetractObservation =>
        state match {
          case PriceStreamState.Empty => Left(DomainError.NotRegistered)
          case _: PriceStreamState.Open =>
            Right(List(PriceEvent.PriceObservationRetracted(c.observedAt, c.reason)))
        }
    }

  /**
   * Same price, same source, same calendar day => nothing new was learned.
   *
   * Scrapes repeat within a day; without this the history fills with duplicates that would skew any
   * rolling statistic computed over it.
   */
  private def isDuplicate(
      state: PriceStreamState,
      c: PriceCommand.ObservePrice,
      zone: ZoneId
  ): Boolean =
    state match {
      case PriceStreamState.Empty => false
      case PriceStreamState.Open(_, scope, last, _) =>
        // Scope is part of the identity of a price fact, not a detail of it. A
        // receipt at one store and a flyer covering that store's chain+region can
        // carry the same number on the same day and still be two DIFFERENT facts —
        // and the receipt is the better one. Deduping across scopes would silently
        // drop it. In practice each stream is per-scope (entity id
        // `price|{productId}|{scope}`), but `decide` is pure and must not depend on
        // the caller having routed correctly.
        scope == c.scope && last.exists { l =>
          l.price == c.price &&
          l.source == c.source &&
          l.at.atZone(zone).toLocalDate == c.observedAt.atZone(zone).toLocalDate
        }
    }

  def evolve(state: PriceStreamState, event: PriceEvent): PriceStreamState =
    (state, event) match {
      case (PriceStreamState.Empty, e: PriceEvent.PriceObserved) =>
        PriceStreamState.Open(
          e.productId,
          e.scope,
          Some(LastObservation(e.observedAt, e.price, e.source)),
          1L
        )

      case (PriceStreamState.Empty, _) => PriceStreamState.Empty

      case (s: PriceStreamState.Open, e: PriceEvent.PriceObserved) =>
        // Out-of-order replay (a backfill carrying original timestamps) must not
        // let an older fact overwrite the dedup anchor.
        val next =
          if s.last.forall(l => !e.observedAt.isBefore(l.at)) then
            Some(LastObservation(e.observedAt, e.price, e.source))
          else s.last
        s.copy(last = next, count = s.count + 1)

      case (s: PriceStreamState.Open, _: PriceEvent.PriceObservationRetracted) =>
        s.copy(count = math.max(0L, s.count - 1))
    }

  def replay(events: List[PriceEvent]): PriceStreamState =
    events.foldLeft[PriceStreamState](PriceStreamState.Empty)(evolve)
}

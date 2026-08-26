package me.cference.ariadne.domain
package store

/**
 * A retailer/banner, optionally down to a location.
 *
 * Deliberately boring. Stores are also where scraper source config attaches (which Flipp merchant
 * maps to which StoreId) — but that is config, not domain state, and does not live here.
 *
 * §10.3 is open on granularity: v1 is chain-level with an optional location, revisited when
 * per-location pricing starts to hurt.
 */
enum StoreState {
  case Empty
  case Existing(
      id: StoreId,
      name: String,
      chain: Option[String],
      location: Option[String],
      active: Boolean
  )
}

enum StoreCommand {
  case RegisterStore(
      id: StoreId,
      name: String,
      chain: Option[String],
      location: Option[String],
      correlationId: CorrelationId
  )
  case UpdateStoreDetails(
      name: Option[String],
      chain: Option[String],
      location: Option[String],
      correlationId: CorrelationId
  )
  case DeactivateStore(correlationId: CorrelationId)
}

enum StoreEvent {
  case StoreRegistered(id: StoreId, name: String, chain: Option[String], location: Option[String])
  case StoreDetailsUpdated(name: Option[String], chain: Option[String], location: Option[String])
  case StoreDeactivated
}

object Store {

  def decide(state: StoreState, cmd: StoreCommand): Either[DomainError, List[StoreEvent]] =
    (state, cmd) match {
      case (StoreState.Empty, c: StoreCommand.RegisterStore) =>
        if c.name.isBlank then Left(DomainError.EmptyStoreName)
        else Right(List(StoreEvent.StoreRegistered(c.id, c.name.trim, c.chain, c.location)))

      case (StoreState.Empty, _) => Left(DomainError.NotRegistered)
      case (_: StoreState.Existing, _: StoreCommand.RegisterStore) =>
        Left(DomainError.AlreadyRegistered)

      case (s: StoreState.Existing, c: StoreCommand.UpdateStoreDetails) =>
        if c.name.exists(_.isBlank) then Left(DomainError.EmptyStoreName)
        else {
          val next = StoreEvent.StoreDetailsUpdated(c.name.map(_.trim), c.chain, c.location)
          // No-op when nothing actually changes — avoids a stream of empty deltas.
          val changed =
            c.name.map(_.trim).exists(_ != s.name) || c.chain != s.chain || c.location != s.location
          if changed then Right(List(next)) else Right(Nil)
        }

      case (s: StoreState.Existing, _: StoreCommand.DeactivateStore) =>
        if !s.active then Right(Nil) else Right(List(StoreEvent.StoreDeactivated))
    }

  def evolve(state: StoreState, event: StoreEvent): StoreState =
    (state, event) match {
      case (StoreState.Empty, e: StoreEvent.StoreRegistered) =>
        StoreState.Existing(e.id, e.name, e.chain, e.location, active = true)
      case (StoreState.Empty, _) => StoreState.Empty
      case (s: StoreState.Existing, e) =>
        e match {
          case _: StoreEvent.StoreRegistered => s
          case e: StoreEvent.StoreDetailsUpdated =>
            s.copy(
              name = e.name.getOrElse(s.name),
              chain = e.chain.orElse(s.chain),
              location = e.location.orElse(s.location)
            )
          case StoreEvent.StoreDeactivated => s.copy(active = false)
        }
    }

  def replay(events: List[StoreEvent]): StoreState =
    events.foldLeft[StoreState](StoreState.Empty)(evolve)
}

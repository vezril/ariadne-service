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
      chain: ChainId,
      area: Area,
      label: Option[String],
      active: Boolean
  )
}

sealed trait StoreCommand extends CborSerializable

object StoreCommand {
  final case class RegisterStore(
      id: StoreId,
      name: String,
      chain: ChainId,
      area: Area,
      label: Option[String],
      correlationId: CorrelationId
  ) extends StoreCommand
  final case class UpdateStoreDetails(
      name: Option[String],
      area: Option[Area],
      label: Option[String],
      correlationId: CorrelationId
  ) extends StoreCommand
  final case class DeactivateStore(correlationId: CorrelationId) extends StoreCommand
}

sealed trait StoreEvent extends CborSerializable

object StoreEvent {
  final case class StoreRegistered(
      id: StoreId,
      name: String,
      chain: ChainId,
      area: Area,
      label: Option[String]
  ) extends StoreEvent
  final case class StoreDetailsUpdated(
      name: Option[String],
      area: Option[Area],
      label: Option[String]
  ) extends StoreEvent
  case object StoreDeactivated extends StoreEvent
}

object Store {

  def decide(state: StoreState, cmd: StoreCommand): Either[DomainError, List[StoreEvent]] =
    (state, cmd) match {
      case (StoreState.Empty, c: StoreCommand.RegisterStore) =>
        if c.name.isBlank then Left(DomainError.EmptyStoreName)
        else Right(List(StoreEvent.StoreRegistered(c.id, c.name.trim, c.chain, c.area, c.label)))

      case (StoreState.Empty, _) => Left(DomainError.NotRegistered)
      case (_: StoreState.Existing, _: StoreCommand.RegisterStore) =>
        Left(DomainError.AlreadyRegistered)

      case (s: StoreState.Existing, c: StoreCommand.UpdateStoreDetails) =>
        if c.name.exists(_.isBlank) then Left(DomainError.EmptyStoreName)
        else {
          // No-op when nothing actually changes — avoids a stream of empty deltas.
          // `chain` is deliberately absent: a franchise does not change banner, and
          // allowing it would silently re-point every area observation that spoke
          // for this store.
          val changed =
            c.name.map(_.trim).exists(_ != s.name) ||
              c.area.exists(_ != s.area) ||
              (c.label.isDefined && c.label != s.label)
          if changed then
            Right(List(StoreEvent.StoreDetailsUpdated(c.name.map(_.trim), c.area, c.label)))
          else Right(Nil)
        }

      case (s: StoreState.Existing, _: StoreCommand.DeactivateStore) =>
        if !s.active then Right(Nil) else Right(List(StoreEvent.StoreDeactivated))
    }

  def evolve(state: StoreState, event: StoreEvent): StoreState =
    (state, event) match {
      case (StoreState.Empty, e: StoreEvent.StoreRegistered) =>
        StoreState.Existing(e.id, e.name, e.chain, e.area, e.label, active = true)
      case (StoreState.Empty, _) => StoreState.Empty
      case (s: StoreState.Existing, e) =>
        e match {
          case _: StoreEvent.StoreRegistered => s
          case e: StoreEvent.StoreDetailsUpdated =>
            s.copy(
              name = e.name.getOrElse(s.name),
              area = e.area.getOrElse(s.area),
              label = e.label.orElse(s.label)
            )
          case StoreEvent.StoreDeactivated => s.copy(active = false)
        }
    }

  def replay(events: List[StoreEvent]): StoreState =
    events.foldLeft[StoreState](StoreState.Empty)(evolve)
}

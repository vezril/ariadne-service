package me.cference.ariadne.domain
package product

/**
 * A reference to the ResolutionCase that justified a link or merge (§2.5). Every automatic decision
 * is auditable back to the case that made it.
 */
final case class ResolutionRef(value: String) extends AnyVal

/**
 * What a product is in the catalog's eyes.
 *
 * `MergedInto` is a tombstone, not a deletion: ids never die, they forward. Dionysus and Demeter
 * hold ProductIds, so a merged id must keep resolving forever (§6.5) — which is why merging writes
 * a redirect rather than removing a row.
 */
sealed trait ProductStatus

object ProductStatus {
  case object Provisional extends ProductStatus
  case object Active extends ProductStatus
  final case class MergedInto(canonical: ProductId) extends ProductStatus
  case object Deprecated extends ProductStatus
}

enum ProductState {
  case Empty
  case Existing(
      id: ProductId,
      name: String,
      brand: Option[String],
      category: Option[String],
      size: Option[Quantity],
      gtins: Set[Gtin],
      aliases: Set[String],
      listings: Set[ListingKey],
      status: ProductStatus
  )
}

enum ProductCommand {
  case RegisterProduct(
      id: ProductId,
      name: String,
      brand: Option[String],
      category: Option[String],
      size: Option[Quantity],
      gtin: Option[Gtin],
      origin: Origin,
      correlationId: CorrelationId
  )
  case AddIdentifier(gtin: Gtin, correlationId: CorrelationId)
  case AddAlias(alias: String, correlationId: CorrelationId)
  case LinkListing(
      key: ListingKey,
      confidence: Confidence,
      how: MatchMethod,
      matcher: MatcherVersion,
      resolution: Option[ResolutionRef],
      correlationId: CorrelationId
  )
  case MergeInto(
      canonical: ProductId,
      resolution: Option[ResolutionRef],
      correlationId: CorrelationId
  )
  case Absorb(
      loser: ProductId,
      gtins: Set[Gtin],
      aliases: Set[String],
      listings: Set[ListingKey],
      correlationId: CorrelationId
  )
  case Deprecate(reason: String, correlationId: CorrelationId)
}

sealed trait ProductEvent extends CborSerializable

object ProductEvent {
  final case class ProductRegistered(
      id: ProductId,
      name: String,
      brand: Option[String],
      category: Option[String],
      size: Option[Quantity],
      gtin: Option[Gtin],
      origin: Origin,
      status: ProductStatus
  ) extends ProductEvent
  final case class ProductIdentifierAdded(gtin: Gtin) extends ProductEvent
  final case class ProductAliasAdded(alias: String) extends ProductEvent
  final case class ListingLinked(
      key: ListingKey,
      confidence: Confidence,
      how: MatchMethod,
      matcher: MatcherVersion
  ) extends ProductEvent
  final case class ProductMerged(into: ProductId) extends ProductEvent
  final case class ProductAbsorbed(
      loser: ProductId,
      gtins: Set[Gtin],
      aliases: Set[String],
      listings: Set[ListingKey]
  ) extends ProductEvent
  final case class ProductDeprecated(reason: String) extends ProductEvent
}

/**
 * Identity + market description. No nutrition, no deal fields — ever (rule 1).
 *
 * Note what is NOT enforced here: GTIN uniqueness. That is a cross-entity invariant, checked at
 * resolve time against the match index (§6.4) and repaired by merge if a race slips one through.
 * The EventStorming stance is facts first, repair explicitly — not a distributed lock on the write
 * path.
 */
object Product {

  def decide(state: ProductState, cmd: ProductCommand): Either[DomainError, List[ProductEvent]] =
    (state, cmd) match {
      case (ProductState.Empty, c: ProductCommand.RegisterProduct) =>
        if c.name.isBlank then Left(DomainError.EmptyName)
        else {
          // Provenance decides trust: a scrape yields an unreviewed Provisional
          // product that surfaces in the review queue; a human or a migration
          // yields an Active one.
          val status = c.origin match {
            case Origin.Scrape(_) => ProductStatus.Provisional
            case _ => ProductStatus.Active
          }
          Right(
            List(
              ProductEvent.ProductRegistered(
                c.id,
                c.name.trim,
                c.brand,
                c.category,
                c.size,
                c.gtin,
                c.origin,
                status
              )
            )
          )
        }

      case (ProductState.Empty, _) => Left(DomainError.NotRegistered)

      case (_: ProductState.Existing, _: ProductCommand.RegisterProduct) =>
        Left(DomainError.AlreadyRegistered)

      case (s: ProductState.Existing, c) =>
        s.status match {
          // A tombstone accepts no writes. Callers must follow the redirect and
          // address the canonical id instead, or they would append facts to a
          // product that no longer exists as a distinct thing.
          case ProductStatus.MergedInto(canonical) =>
            Left(DomainError.ProductIsTombstone(canonical))
          case _ => decideOnLive(s, c)
        }
    }

  private def decideOnLive(
      s: ProductState.Existing,
      cmd: ProductCommand
  ): Either[DomainError, List[ProductEvent]] =
    cmd match {
      case _: ProductCommand.RegisterProduct => Left(DomainError.AlreadyRegistered)

      case c: ProductCommand.AddIdentifier =>
        // Idempotent: re-adding a known GTIN is a no-op, not an error. Scrapes
        // repeat, and a repeat must not fail a run.
        if s.gtins.contains(c.gtin) then Right(Nil)
        else Right(List(ProductEvent.ProductIdentifierAdded(c.gtin)))

      case c: ProductCommand.AddAlias =>
        val alias = c.alias.trim
        if alias.isEmpty then Left(DomainError.EmptyName)
        else if s.aliases.contains(alias) then Right(Nil)
        else Right(List(ProductEvent.ProductAliasAdded(alias)))

      case c: ProductCommand.LinkListing =>
        if s.listings.contains(c.key) then Right(Nil)
        else Right(List(ProductEvent.ListingLinked(c.key, c.confidence, c.how, c.matcher)))

      case c: ProductCommand.MergeInto =>
        if c.canonical == s.id then Left(DomainError.CannotMergeIntoSelf)
        else Right(List(ProductEvent.ProductMerged(c.canonical)))

      case c: ProductCommand.Absorb =>
        if c.loser == s.id then Left(DomainError.CannotMergeIntoSelf)
        else Right(List(ProductEvent.ProductAbsorbed(c.loser, c.gtins, c.aliases, c.listings)))

      case c: ProductCommand.Deprecate =>
        if s.status == ProductStatus.Deprecated then Left(DomainError.AlreadyDeprecated)
        else Right(List(ProductEvent.ProductDeprecated(c.reason)))
    }

  def evolve(state: ProductState, event: ProductEvent): ProductState =
    (state, event) match {
      case (ProductState.Empty, e: ProductEvent.ProductRegistered) =>
        ProductState.Existing(
          id = e.id,
          name = e.name,
          brand = e.brand,
          category = e.category,
          size = e.size,
          gtins = e.gtin.toSet,
          aliases = Set.empty,
          listings = Set.empty,
          status = e.status
        )

      case (ProductState.Empty, _) => ProductState.Empty

      case (s: ProductState.Existing, e) =>
        e match {
          case _: ProductEvent.ProductRegistered => s
          case e: ProductEvent.ProductIdentifierAdded => s.copy(gtins = s.gtins + e.gtin)
          case e: ProductEvent.ProductAliasAdded => s.copy(aliases = s.aliases + e.alias)
          case e: ProductEvent.ListingLinked => s.copy(listings = s.listings + e.key)
          case e: ProductEvent.ProductMerged => s.copy(status = ProductStatus.MergedInto(e.into))
          case e: ProductEvent.ProductAbsorbed =>
            // The winner takes on the loser's keys so every identifier the loser
            // answered to keeps resolving — to the canonical product now.
            s.copy(
              gtins = s.gtins ++ e.gtins,
              aliases = s.aliases ++ e.aliases,
              listings = s.listings ++ e.listings,
              status =
                if s.status == ProductStatus.Provisional then ProductStatus.Active else s.status
            )
          case e: ProductEvent.ProductDeprecated =>
            val _ = e
            s.copy(status = ProductStatus.Deprecated)
        }
    }

  /** Replay — the definition of correctness for an event-sourced aggregate. */
  def replay(events: List[ProductEvent]): ProductState =
    events.foldLeft[ProductState](ProductState.Empty)(evolve)
}

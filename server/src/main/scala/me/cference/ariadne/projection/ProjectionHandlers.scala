package me.cference.ariadne.projection

import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.store.StoreEvent
import me.cference.ariadne.domain.resolution.{ResolutionEvent, ScoredCandidate}
import me.cference.ariadne.matching.MatchInput
import me.cference.ariadne.projection.ReadModelRepository.{CurrentPriceRow, PriceHistoryRow}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Event -> read model. Deliberately thin: every statement the repository runs is idempotent, so
 * these handlers only have to decide WHICH statement, never worry about having run before.
 *
 * The product id comes from the persistence id (`product|{id}`) rather than the event body, since
 * most events carry only a delta.
 */
object ProjectionHandlers {

  def entityId(persistenceId: String): String =
    persistenceId.substring(persistenceId.indexOf('|') + 1)

  def product(repo: ReadModelRepository)(persistenceId: String, event: ProductEvent)(using
      ec: ExecutionContext
  ): Future[Unit] = {
    val id = entityId(persistenceId)
    event match {
      case e: ProductEvent.ProductRegistered =>
        for {
          _ <- repo.upsertProduct(
            id,
            e.name,
            e.brand,
            e.category,
            e.size,
            statusName(e.status),
            None
          )
          _ <- e.gtin.fold(Future.unit)(g => repo.addGtin(g.value, id))
          // products first: match_index carries a foreign key onto it.
          _ <- indexFor(repo, id, e.name, e.brand, e.size)
        } yield ()

      case e: ProductEvent.ProductIdentifierAdded => repo.addGtin(e.gtin.value, id)
      case e: ProductEvent.ProductAliasAdded => repo.addAlias(id, e.alias)

      case e: ProductEvent.ListingLinked =>
        repo.linkListing(
          e.key.storeId.value,
          e.key.externalId,
          id,
          e.confidence.toDouble,
          e.how.toString,
          e.matcher.value
        )

      case e: ProductEvent.ProductMerged =>
        // The LOSER becomes a tombstone pointing at the winner. The row is not deleted:
        // ids never die, they forward, and callers hold ids we do not control (§6.5).
        // It IS dropped from the match index though: a tombstone must never come back
        // as a candidate, or the matcher would re-link a listing onto a product that is
        // no longer a distinct thing.
        for {
          _ <- repo.setProductStatus(id, "MergedInto", Some(e.into.value))
          _ <- repo.deleteMatchIndex(id)
        } yield ()

      case e: ProductEvent.ProductAbsorbed =>
        // The WINNER takes over the loser's keys so every identifier the loser answered
        // to keeps resolving — to the canonical product now.
        for {
          _ <- Future.sequence(e.gtins.toList.map(g => repo.addGtin(g.value, id)))
          _ <- Future.sequence(e.aliases.toList.map(a => repo.addAlias(id, a)))
        } yield ()

      case _: ProductEvent.ProductDeprecated => repo.setProductStatus(id, "Deprecated", None)
    }
  }

  def store(repo: ReadModelRepository)(persistenceId: String, event: StoreEvent)(using
      ec: ExecutionContext
  ): Future[Unit] = {
    val id = entityId(persistenceId)
    event match {
      case e: StoreEvent.StoreRegistered =>
        repo.upsertStore(id, e.name, e.chain.value, e.area.postalPrefix, e.label, active = true)
      case e: StoreEvent.StoreDetailsUpdated =>
        // A delta, so a partial update — an upsert here would blank the fields the
        // command did not mention.
        repo.updateStoreDetails(id, e.name, e.area.map(_.postalPrefix), e.label)
      case StoreEvent.StoreDeactivated =>
        repo.deactivateStore(id)
    }
  }

  def price(repo: ReadModelRepository)(persistenceId: String, seqNr: Long, event: PriceEvent)(using
      ec: ExecutionContext
  ): Future[Unit] =
    event match {
      case e: PriceEvent.PriceObserved =>
        val (kind, storeId, chainId, area) = e.scope match {
          case PriceScope.Exact(s) => ("exact", Some(s.value), None, None)
          case PriceScope.Regional(c, a) => ("area", None, Some(c.value), Some(a.postalPrefix))
        }
        for {
          _ <- repo.appendPriceHistory(
            PriceHistoryRow(
              e.productId.value,
              kind,
              storeId,
              chainId,
              area,
              e.observedAt,
              e.price.amount,
              e.price.currency.toString,
              e.unitPrice.map(_.amount),
              e.unitPrice.map(_.per.amount),
              e.unitPrice.map(_.per.unit.toString),
              e.promo.map(_.description),
              e.priceConfidence.toDouble,
              e.sizeConfidence.toDouble,
              sourceName(e.source),
              rawResponseIdOf(e.source),
              None,
              persistenceId,
              seqNr
            )
          )
          _ <- repo.upsertCurrentPrice(
            CurrentPriceRow(
              e.productId.value,
              e.scope.key,
              kind,
              storeId,
              chainId,
              area,
              e.price.amount,
              e.price.currency.toString,
              e.unitPrice.map(_.amount),
              e.observedAt,
              sourceName(e.source),
              e.sizeConfidence.toDouble
            )
          )
        } yield ()

      // A retraction removes a bad fact from the CURRENT view but leaves history intact —
      // the retraction is itself a recorded event, and rewriting history is what an
      // append-only store exists to prevent.
      case _: PriceEvent.PriceObservationRetracted => Future.unit
    }

  /**
   * ResolutionCase -> the review queue (§6.5). This is the read model ariadne-ui exists to show.
   *
   * Candidates are stored as the matcher offered them, JSON-encoded by hand rather than through a
   * library: the payload is three fields and pulling in a JSON dependency for it would be heavier
   * than the problem. What matters is that the review UI shows what the matcher said AT THE TIME,
   * not a re-derivation from a matcher that may since have changed (§6.6).
   */
  def resolution(repo: ReadModelRepository)(persistenceId: String, event: ResolutionEvent)(using
      ec: ExecutionContext
  ): Future[Unit] = {
    val id = entityId(persistenceId)
    event match {
      case e: ResolutionEvent.ResolutionProposed =>
        repo.upsertResolutionCase(
          id,
          e.subject.name,
          e.subject.brand,
          e.subject.gtin.map(_.value),
          e.subject.listing.map(_.storeId.value),
          e.subject.listing.map(_.externalId),
          candidatesJson(e.candidates)
        )
      case _: ResolutionEvent.ObservationParked => repo.incrementParked(id)
      case e: ResolutionEvent.ResolutionConfirmed =>
        repo.resolveCase(id, s"Confirmed:${e.productId.value}")
      case e: ResolutionEvent.ResolutionRejected =>
        repo.resolveCase(id, s"NewProduct:${e.newProductId.value}")
      case e: ResolutionEvent.MergeRequested =>
        repo.resolveCase(id, s"Merged:${e.winner.value}<-${e.loser.value}")
      case e: ResolutionEvent.SplitRequested =>
        repo.resolveCase(id, s"Split:${e.newProductId.value}")
    }
  }

  private def candidatesJson(candidates: List[ScoredCandidate]): String =
    candidates
      .map(c =>
        s"""{"productId":${quote(c.productId.value)},"score":${c.score.toDouble},"notes":[${c.notes
            .map(quote)
            .mkString(",")}]}"""
      )
      .mkString("[", ",", "]")

  /** Minimal JSON string escaping — enough for ids, scores and the scorer's fixed notes. */
  private def quote(s: String): String =
    "\"" + s.flatMap {
      case '"' => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c => c.toString
    } + "\""

  /** The index stores the name AFTER normalisation, with the size split out (§6.3). */
  private def indexFor(
      repo: ReadModelRepository,
      id: String,
      name: String,
      brand: Option[String],
      knownSize: Option[Quantity]
  ): Future[Unit] = {
    val input = MatchInput.from(name, brand, knownSize)
    repo.upsertMatchIndex(id, input.normalized, input.tokens, input.brand, input.size)
  }

  private def statusName(s: ProductStatus): String = s match {
    case ProductStatus.Provisional => "Provisional"
    case ProductStatus.Active => "Active"
    case ProductStatus.Deprecated => "Deprecated"
    case _: ProductStatus.MergedInto => "MergedInto"
  }

  /** Only a scrape has archived bytes behind it; everything else legitimately has none. */
  private def rawResponseIdOf(s: PriceSource): Option[Long] = s match {
    case sc: PriceSource.Scrape => Some(sc.rawResponseId)
    case _ => None
  }

  private def sourceName(s: PriceSource): String = s match {
    case _: PriceSource.Scrape => "Scrape"
    case _: PriceSource.Purchase => "Purchase"
    case PriceSource.Manual => "Manual"
    case _: PriceSource.Backfill => "Backfill"
  }
}

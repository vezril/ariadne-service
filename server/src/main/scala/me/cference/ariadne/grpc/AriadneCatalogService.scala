package me.cference.ariadne.grpc

import ariadne.v1 as pb
import me.cference.ariadne.domain.{
  CorrelationId,
  Gtin,
  ListingKey,
  MeasureUnit,
  Money,
  Origin,
  ProductId,
  PurchaseId,
  Quantity,
  StoreId
}
import me.cference.ariadne.domain.product.ProductCommand
import me.cference.ariadne.domain.purchase.{PurchaseCommand, PurchaseLine, PurchaseSource}
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.projection.ReadModelRepository.{
  PriceHistoryRow,
  ProductRow,
  ResolvedPrice
}
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * The synchronous read surface (DESIGN §4), served over gRPC.
 *
 * **Reads hit projections only, never entities** — CQRS held strictly, which is why only the two
 * write RPCs take a `decide` function. A read that asked an entity would serialise behind its
 * command queue and could not be served while the shard rebalanced.
 *
 * **Every response echoes the correlation id**, minting one when the caller omits it. A caller that
 * cannot be traced is a caller whose slow path nobody can explain, and §8 wants the chain
 * followable across services rather than per-hop.
 *
 * **A domain refusal is not a gRPC error.** `RegisterProduct` and `RecordPurchase` go through
 * `decide`, so "already registered" is information the caller asked for and rides in the response
 * body. A failed Future here means something genuinely broke; raising a status code for an expected
 * refusal would make the two indistinguishable.
 *
 * Three RPCs from §4's proposal are absent, and their absence is stated in the proto rather than
 * implied: `ListProducts` has no paged query, `ListPurchases` has no projection writing the
 * `purchases` tables the DDL already declares, and neither gap is closed by guessing.
 */
final class AriadneCatalogService(
    reads: CatalogReads,
    resolve: (MatchSubject, CorrelationId) => Future[ResolutionOutcome],
    registerProduct: (ProductId, ProductCommand) => Future[Either[String, Unit]],
    recordPurchase: (PurchaseId, PurchaseCommand) => Future[Either[String, Unit]]
)(using ec: ExecutionContext)
    extends pb.AriadneCatalog {

  import AriadneCatalogService.*

  def getProduct(in: pb.GetProductRequest): Future[pb.GetProductResponse] = {
    val cid = correlation(in.correlationId)
    val requested = in.productId
    // A merge tombstone keeps resolving forever, because Dionysus and Demeter hold ids
    // Ariadne does not control. Following it is OPT-IN so a caller is never silently handed
    // a different id than it asked for — the response says when it was.
    val target =
      if in.followMerges then reads.canonicalProductId(requested).map(_.getOrElse(requested))
      else Future.successful(requested)

    target.flatMap { id =>
      reads.product(id).map { row =>
        pb.GetProductResponse(
          product = row.map(toProduct),
          correlationId = cid,
          redirected = id != requested,
          requestedProductId = requested
        )
      }
    }
  }

  /**
   * A page of the catalog, keyset-paginated on the product id.
   *
   * `next_page_token` is the last id of this page, opaque to the caller, and is set only when a
   * FULL page came back. A caller must not infer "last page" from a short page: a filtered query
   * can return fewer rows than the limit and still have more to give. Emitting a token on a short
   * page instead would cost one wasted empty round trip rather than a wrong answer — the cheap
   * error is the one taken.
   */
  def listProducts(in: pb.ListProductsRequest): Future[pb.ListProductsResponse] = {
    val limit = clamp(in.limit, default = 100, max = 500)
    reads
      .listProducts(
        // UNSPECIFIED is not a filter. Passing it through would match no stored status and
        // return an empty page that reads as an empty catalog.
        statuses = in.statuses.toList
          .filterNot(_ == pb.ProductStatus.PRODUCT_STATUS_UNSPECIFIED)
          .map(statusName),
        category = Option(in.category).map(_.trim).filter(_.nonEmpty),
        afterId = Option(in.pageToken).map(_.trim).filter(_.nonEmpty),
        limit = limit
      )
      .map { rows =>
        pb.ListProductsResponse(
          products = rows.map(toProduct),
          nextPageToken = if rows.sizeIs == limit then rows.last.id else "",
          correlationId = correlation(in.correlationId)
        )
      }
  }

  def searchProducts(in: pb.SearchProductsRequest): Future[pb.SearchProductsResponse] =
    reads
      .searchProducts(in.query, clamp(in.limit, default = 20, max = 100))
      .map(rows =>
        pb.SearchProductsResponse(
          products = rows.map(toProduct),
          correlationId = correlation(in.correlationId)
        )
      )

  /**
   * Hot spot #1 over the wire.
   *
   * This is the same `resolveAndReview` the REST surface calls — Path B, which never auto-creates:
   * an ambiguous or absent match parks a review case for a human rather than inventing a product. A
   * caller gets the case id back so it can follow the question it just raised.
   */
  def resolveProduct(in: pb.ResolveProductRequest): Future[pb.ResolveProductResponse] = {
    val cid = correlation(in.correlationId)
    val subject = MatchSubject(
      name = in.name.trim,
      brand = Option(in.brand).map(_.trim).filter(_.nonEmpty),
      gtin = Option(in.gtin).map(_.trim).filter(_.nonEmpty).map(Gtin.unsafe),
      listing = for {
        store <- Option(in.storeId).map(_.trim).filter(_.nonEmpty)
        external <- Option(in.externalId).map(_.trim).filter(_.nonEmpty)
      } yield ListingKey(StoreId(store), external)
    )

    resolve(subject, CorrelationId(cid)).map { outcome =>
      val result = outcome match {
        case ResolutionOutcome.Matched(productId, confidence, method) =>
          pb.ResolveProductResponse.Outcome.Matched(
            pb.Matched(productId.value, confidence.toDouble, method.toString)
          )
        case ResolutionOutcome.Ambiguous(candidates) =>
          pb.ResolveProductResponse.Outcome.Ambiguous(
            pb.Ambiguous(
              candidates =
                candidates.map(c => pb.Candidate(c.productId.value, c.score.toDouble, c.notes)),
              // Derived from the subject, so it is the SAME case a scrape of the same
              // listing would have opened rather than a parallel duplicate (§6.5).
              resolutionCaseId = ResolutionService.caseIdFor(subject).value
            )
          )
        case ResolutionOutcome.NoMatch =>
          pb.ResolveProductResponse.Outcome.NoMatch(
            pb.NoMatch(ResolutionService.caseIdFor(subject).value)
          )
      }
      pb.ResolveProductResponse(outcome = result, correlationId = cid)
    }
  }

  /**
   * Exact-over-area (§2.3.1), and the response STATES which it gave you.
   *
   * `storeId` is required because scope is part of a price's identity: "the current price" with no
   * store named has no answer that is not a guess across incomparable scopes.
   */
  def getCurrentPrice(in: pb.GetCurrentPriceRequest): Future[pb.GetCurrentPriceResponse] = {
    val cid = correlation(in.correlationId)
    if in.storeId.trim.isEmpty then
      Future.failed(
        new IllegalArgumentException(
          "store_id is required — a price with no scope is not a fact this service holds (§2.3.1)"
        )
      )
    else
      reads.currentPrice(in.productId, in.storeId.trim).map {
        case None => pb.GetCurrentPriceResponse(found = false, correlationId = cid)
        case Some(p) =>
          pb.GetCurrentPriceResponse(
            found = true,
            price = Some(pb.Money(p.amount.toPlainString, p.currency)),
            scope = Some(toScope(p)),
            observedAt = p.observedAt.toString,
            source = p.source,
            sizeConfidence = p.sizeConfidence,
            priceConfidence = p.priceConfidence,
            correlationId = cid
          )
      }
  }

  /**
   * Server-streamed, because a long history is the one response here unbounded in principle.
   *
   * Streamed from a bounded query rather than a cursor: the limit is applied in SQL and the rows
   * are emitted as they map. A true cursor would be the right shape for a full export, and this is
   * a price chart's shape — newest first, capped.
   */
  def getPriceHistory(in: pb.GetPriceHistoryRequest): Source[pb.PricePoint, NotUsed] =
    Source
      .future(
        reads.priceHistory(
          productId = in.productId,
          storeId = Option(in.storeId).map(_.trim).filter(_.nonEmpty),
          from = instantOpt(in.from),
          to = instantOpt(in.to),
          limit = clamp(in.limit, default = 500, max = 5000)
        )
      )
      .mapConcat(identity)
      .map(toPricePoint)

  def registerProduct(in: pb.RegisterProductRequest): Future[pb.RegisterProductResponse] = {
    val cid = correlation(in.correlationId)
    val name = in.name.trim
    if name.isEmpty then
      Future.successful(
        pb.RegisterProductResponse(
          accepted = false,
          refusal = "name is required",
          correlationId = cid
        )
      )
    else {
      val id = Option(in.productId)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(ProductId.apply)
        .getOrElse(derivedProductId(name, Option(in.brand).map(_.trim).filter(_.nonEmpty)))
      val cmd = ProductCommand.RegisterProduct(
        id = id,
        name = name,
        brand = Option(in.brand).map(_.trim).filter(_.nonEmpty),
        category = Option(in.category).map(_.trim).filter(_.nonEmpty),
        size = in.size.flatMap(toQuantity),
        gtin = Option(in.gtin).map(_.trim).filter(_.nonEmpty).map(Gtin.unsafe),
        // Over the wire from another service, which is what `Manual` means here as opposed
        // to a scrape: a human or a service asserted this product rather than a feed.
        origin = Origin.Manual,
        correlationId = CorrelationId(cid)
      )
      registerProduct(id, cmd).map {
        case Right(_) =>
          pb.RegisterProductResponse(productId = id.value, accepted = true, correlationId = cid)
        case Left(why) =>
          pb.RegisterProductResponse(
            productId = id.value,
            accepted = false,
            refusal = why,
            correlationId = cid
          )
      }
    }
  }

  /**
   * §7.1 over gRPC, with the same contract the REST surface has: an identical re-record is a
   * SUCCESS, a different purchase under a taken id is a refusal.
   *
   * The caller mints the id here too. That is not a REST detail — it is what makes the retry
   * addressable at all when Ariadne was unreachable and the request never arrived.
   */
  def recordPurchase(in: pb.RecordPurchaseRequest): Future[pb.RecordPurchaseResponse] = {
    val cid = correlation(in.correlationId)
    parsePurchase(in, CorrelationId(cid)) match {
      case Left(why) =>
        Future.successful(
          pb.RecordPurchaseResponse(
            purchaseId = in.purchaseId,
            accepted = false,
            refusal = why,
            correlationId = cid
          )
        )
      case Right((id, cmd)) =>
        recordPurchase(id, cmd).map {
          case Right(_) =>
            pb.RecordPurchaseResponse(purchaseId = id.value, accepted = true, correlationId = cid)
          case Left(why) =>
            pb.RecordPurchaseResponse(
              purchaseId = id.value,
              accepted = false,
              refusal = why,
              correlationId = cid
            )
        }
    }
  }

  /** Echo the caller's id, or mint one: §8 wants the chain followable, never anonymous. */
  private def correlation(offered: String): String =
    Option(offered).map(_.trim).filter(_.nonEmpty).getOrElse(java.util.UUID.randomUUID().toString)
}

/**
 * The reads this surface needs, as four functions rather than the repository itself.
 *
 * `ReadModelRepository` is final, so it cannot be subclassed in a test — but narrowing to the reads
 * actually used is the better shape regardless, and it is the pattern the REST routes already
 * follow (`StoreRoutes` takes `decide` as a function, not a shard region, so it is testable without
 * a cluster). It also makes the CQRS boundary visible in a signature: there is no way to reach an
 * entity from here.
 */
trait CatalogReads {
  def product(id: String): Future[Option[ProductRow]]
  def canonicalProductId(id: String): Future[Option[String]]
  def searchProducts(term: String, limit: Int): Future[List[ProductRow]]
  def listProducts(
      statuses: List[String],
      category: Option[String],
      afterId: Option[String],
      limit: Int
  ): Future[List[ProductRow]]
  def currentPrice(productId: String, storeId: String): Future[Option[ResolvedPrice]]
  def priceHistory(
      productId: String,
      storeId: Option[String],
      from: Option[Instant],
      to: Option[Instant],
      limit: Int
  ): Future[List[PriceHistoryRow]]
}

object CatalogReads {

  /** The production binding: straight through to the projections. */
  def fromRepository(repo: me.cference.ariadne.projection.ReadModelRepository): CatalogReads =
    new CatalogReads {
      def product(id: String): Future[Option[ProductRow]] = repo.getProduct(id)
      def canonicalProductId(id: String): Future[Option[String]] = repo.resolveCanonical(id)
      def searchProducts(term: String, limit: Int): Future[List[ProductRow]] =
        repo.searchProducts(term, limit)
      def listProducts(
          statuses: List[String],
          category: Option[String],
          afterId: Option[String],
          limit: Int
      ): Future[List[ProductRow]] = repo.listProducts(statuses, category, afterId, limit)
      def currentPrice(productId: String, storeId: String): Future[Option[ResolvedPrice]] =
        repo.currentPriceForStore(productId, storeId)
      def priceHistory(
          productId: String,
          storeId: Option[String],
          from: Option[Instant],
          to: Option[Instant],
          limit: Int
      ): Future[List[PriceHistoryRow]] = repo.priceHistory(productId, storeId, from, to, limit)
    }
}

object AriadneCatalogService {

  /** Clamp a caller's page size: 0 means "unset" in proto3, and an unbounded read is a footgun. */
  private[grpc] def clamp(requested: Int, default: Int, max: Int): Int =
    if requested <= 0 then default else math.min(requested, max)

  private[grpc] def instantOpt(s: String): Option[Instant] =
    Option(s).map(_.trim).filter(_.nonEmpty).flatMap(v => Try(Instant.parse(v)).toOption)

  private[grpc] def toProduct(r: ProductRow): pb.Product =
    pb.Product(
      productId = r.id,
      name = r.name,
      brand = r.brand.getOrElse(""),
      category = r.category.getOrElse(""),
      size = for {
        amount <- r.sizeAmount
        unit <- r.sizeUnit
      } yield pb.Quantity(amount.bigDecimal.stripTrailingZeros.toPlainString, unit),
      status = toStatus(r.status),
      mergedInto = r.mergedInto.getOrElse("")
    )

  /**
   * The inverse of `toStatus` — a proto enum to the string the read model stores.
   *
   * An explicit match rather than something derived from the enum name, so a rename on either side
   * is a compile error here instead of a filter that silently matches nothing.
   */
  private[grpc] def statusName(s: pb.ProductStatus): String =
    s match {
      case pb.ProductStatus.PRODUCT_STATUS_PROVISIONAL => "provisional"
      case pb.ProductStatus.PRODUCT_STATUS_ACTIVE => "active"
      case pb.ProductStatus.PRODUCT_STATUS_MERGED_INTO => "merged_into"
      case pb.ProductStatus.PRODUCT_STATUS_DEPRECATED => "deprecated"
      case other => other.name.toLowerCase
    }

  private[grpc] def toStatus(s: String): pb.ProductStatus =
    s.toLowerCase match {
      case "provisional" => pb.ProductStatus.PRODUCT_STATUS_PROVISIONAL
      case "active" => pb.ProductStatus.PRODUCT_STATUS_ACTIVE
      case "merged_into" | "mergedinto" => pb.ProductStatus.PRODUCT_STATUS_MERGED_INTO
      case "deprecated" => pb.ProductStatus.PRODUCT_STATUS_DEPRECATED
      // An unrecognised status maps to UNSPECIFIED rather than guessing ACTIVE. A new
      // status reaching an old consumer must not read as "fine to use".
      case _ => pb.ProductStatus.PRODUCT_STATUS_UNSPECIFIED
    }

  private[grpc] def toScope(p: ResolvedPrice): pb.PriceScope =
    if p.isExact then
      pb.PriceScope(pb.PriceScope.Scope.Exact(pb.ExactScope(p.storeId.getOrElse(""))))
    else
      pb.PriceScope(
        pb.PriceScope.Scope.Area(pb.AreaScope(p.chainId.getOrElse(""), p.area.getOrElse("")))
      )

  private[grpc] def toPricePoint(r: PriceHistoryRow): pb.PricePoint =
    pb.PricePoint(
      price = Some(pb.Money(r.priceAmount.bigDecimal.toPlainString, r.priceCurrency)),
      scope = Some(
        if r.scopeKind == "exact" then
          pb.PriceScope(pb.PriceScope.Scope.Exact(pb.ExactScope(r.storeId.getOrElse(""))))
        else
          pb.PriceScope(
            pb.PriceScope.Scope.Area(pb.AreaScope(r.chainId.getOrElse(""), r.area.getOrElse("")))
          )
      ),
      observedAt = r.observedAt.toString,
      source = r.source,
      priceConfidence = r.priceConfidence,
      sizeConfidence = r.sizeConfidence,
      unitPrice = r.unitPrice.map(u => pb.Money(u.bigDecimal.toPlainString, r.priceCurrency)),
      promo = r.promo.isDefined
    )

  private[grpc] def toQuantity(q: pb.Quantity): Option[Quantity] =
    for {
      amount <- Try(BigDecimal(q.amount.trim)).toOption
      unit <- MeasureUnit.values.find(_.toString.equalsIgnoreCase(q.unit.trim))
      built <- Quantity(amount, unit).toOption
    } yield built

  /**
   * Derived, not random, for the reason provisional ids are (§6.4.1): a caller retrying a
   * registration whose response it never saw must land on the product it already created.
   */
  private[grpc] def derivedProductId(name: String, brand: Option[String]): ProductId = {
    val key = (brand.toList :+ name)
      .map(_.trim.toLowerCase.replaceAll("\\s+", " "))
      .mkString("|")
    ProductId(s"prod-${java.util.UUID.nameUUIDFromBytes(key.getBytes("UTF-8"))}")
  }

  private[grpc] def parsePurchase(
      in: pb.RecordPurchaseRequest,
      correlationId: CorrelationId
  ): Either[String, (PurchaseId, PurchaseCommand)] =
    for {
      id <- nonEmpty(in.purchaseId, "purchase_id")
        .map(PurchaseId.apply)
        .left
        .map(_ => "purchase_id is required — the caller mints it so a retry is addressable (§7.1)")
      storeId <- nonEmpty(in.storeId, "store_id").map(StoreId.apply)
      purchasedAt <- instantOpt(in.purchasedAt)
        .toRight(s"purchased_at is not an ISO-8601 instant: '${in.purchasedAt}'")
      lines <- in.lines.toList.zipWithIndex
        .foldLeft[Either[String, List[PurchaseLine]]](Right(Nil)) { case (acc, (l, i)) =>
          acc.flatMap(ls => parseLine(l, i).map(ls :+ _))
        }
      total <- in.total.toRight("total is required").flatMap(m => money(m, "total"))
      source <- parseSource(in)
    } yield (
      id,
      PurchaseCommand.RecordPurchase(id, storeId, purchasedAt, lines, total, source, correlationId)
    )

  private def parseLine(l: pb.PurchaseLine, i: Int): Either[String, PurchaseLine] =
    for {
      productId <- nonEmpty(l.productId, s"lines[$i].product_id").map(ProductId.apply)
      quantity <- Try(BigDecimal(l.quantity.trim)).toEither.left
        .map(_ => s"lines[$i].quantity is not a number: '${l.quantity}'")
      _ <- if quantity > 0 then Right(()) else Left(s"lines[$i].quantity must be positive")
      pricePaid <- l.pricePaid
        .toRight(s"lines[$i].price_paid is required")
        .flatMap(m => money(m, s"lines[$i].price_paid"))
      lineTotal <- l.lineTotal
        .toRight(s"lines[$i].line_total is required")
        .flatMap(m => money(m, s"lines[$i].line_total"))
    } yield PurchaseLine(productId, quantity, pricePaid, lineTotal)

  private def parseSource(in: pb.RecordPurchaseRequest): Either[String, PurchaseSource] =
    Option(in.source).map(_.trim.toLowerCase).filter(_.nonEmpty) match {
      case None | Some("manual") => Right(PurchaseSource.Manual)
      case Some("receipt") =>
        Option(in.receiptBlobId)
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(PurchaseSource.Receipt.apply)
          .toRight("receipt_blob_id is required when source is receipt")
      case Some(other) => Left(s"unknown source '$other' — expected manual or receipt")
    }

  private def money(m: pb.Money, field: String): Either[String, Money] =
    for {
      amount <- Try(BigDecimal(m.amount.trim)).toEither.left
        .map(_ => s"$field is not a number: '${m.amount}'")
      built <- Money.cad(amount).left.map(e => s"$field: ${e.message}")
    } yield built

  private def nonEmpty(s: String, field: String): Either[String, String] =
    Option(s).map(_.trim).filter(_.nonEmpty).toRight(s"$field is required")
}

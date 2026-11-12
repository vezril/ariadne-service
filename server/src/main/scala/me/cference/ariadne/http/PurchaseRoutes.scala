package me.cference.ariadne.http

import me.cference.ariadne.domain.{
  CorrelationId,
  DomainError,
  Money,
  ProductId,
  PurchaseId,
  StoreId
}
import me.cference.ariadne.domain.purchase.{PurchaseCommand, PurchaseLine, PurchaseSource}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Manual purchase entry (§7, hot spot #2 — v1).
 *
 * Manual entry is v1 not because it is the least work but because it is the only path with perfect
 * line-level data: a product id and the price actually paid. Receipt scan and bank import are later
 * ADAPTERS onto this same `RecordPurchase` command, so neither needs a second model.
 *
 * There is deliberately no read surface here. §3's table gives this aggregate an offset-only
 * projection — the price-append process manager — and no `purchases` read model, because nothing in
 * the catalog queries purchases; budgeting will, over Hermes, as its own service. Adding a read
 * model now would be building for a consumer that does not exist.
 */
final class PurchaseRoutes(
    decide: (PurchaseId, PurchaseCommand) => Future[Either[String, Unit]]
) extends SprayJsonSupport {

  import JsonFormats.*

  val routes: Route =
    pathPrefix("api" / "v1" / "purchases") {
      concat(
        (post & pathEndOrSingleSlash & entity(as[RecordPurchaseRequest])) { req =>
          withCorrelation { correlation =>
            parsed(req, correlation) match {
              case Left(why) => complete(StatusCodes.BadRequest -> ErrorView(why))
              case Right((id, cmd)) =>
                onComplete(decide(id, cmd)) {
                  // A re-record of the SAME purchase reaches here as a success, because
                  // the aggregate treats it as a no-op (§7.1). That is what lets a
                  // client's outbox drain: an error would be indistinguishable from a
                  // genuine failure and the row would retry forever.
                  case Success(Right(_)) =>
                    complete(StatusCodes.Accepted -> AcceptedView(id.value, "accepted"))
                  // A DIFFERENT purchase under a taken id is an id collision, not a
                  // retry — 409 rather than silently overwriting one receipt with
                  // another.
                  case Success(Left(msg)) => complete(StatusCodes.Conflict -> ErrorView(msg))
                  case Failure(e) =>
                    complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
                }
            }
          }
        },
        (post & path(Segment / "void") & entity(as[VoidPurchaseRequest])) { (id, req) =>
          withCorrelation { correlation =>
            if req.reason.trim.isEmpty then
              complete(
                StatusCodes.BadRequest -> ErrorView(
                  "reason is required — a void with no reason loses why the correction happened"
                )
              )
            else
              val purchaseId = PurchaseId(id)
              onComplete(
                decide(purchaseId, PurchaseCommand.VoidPurchase(req.reason.trim, correlation))
              ) {
                case Success(Right(_)) =>
                  complete(StatusCodes.Accepted -> AcceptedView(id, "voided"))
                case Success(Left(msg)) => complete(StatusCodes.Conflict -> ErrorView(msg))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
              }
          }
        }
      )
    }

  private def withCorrelation(inner: CorrelationId => Route): Route =
    optionalHeaderValueByName("X-Correlation-Id") { cid =>
      inner(CorrelationId(cid.filter(_.trim.nonEmpty).getOrElse(randomCorrelation)))
    }

  private def randomCorrelation: String = java.util.UUID.randomUUID().toString

  /**
   * Turn the wire shape into a command, or say why it cannot become one.
   *
   * Parsing fails at the edge rather than in the aggregate wherever the caller can be told
   * something useful: a malformed decimal or timestamp is a client bug, and `decide` can only
   * report a `DomainError` about a value it was never able to construct. The aggregate still owns
   * every RULE — empty lines, a future purchase, a total that disagrees with its lines — because
   * those must hold no matter which adapter calls it.
   */
  private def parsed(
      req: RecordPurchaseRequest,
      correlation: CorrelationId
  ): Either[String, (PurchaseId, PurchaseCommand)] =
    for {
      id <- nonEmpty(req.id, "id")
        .map(PurchaseId.apply)
        // §7.1: the caller mints the id, so there is nothing to derive when it is
        // missing. Deriving one from content would defeat the point — two genuinely
        // different receipts with identical lines are two purchases, and the same
        // receipt retried must land on the id the client already recorded locally.
        .left
        .map(_ => "id is required — the caller mints it so a retry is addressable (§7.1)")
      storeId <- nonEmpty(req.storeId, "storeId").map(StoreId.apply)
      purchasedAt <- instant(req.purchasedAt)
      lines <- req.lines.zipWithIndex.foldLeft[Either[String, List[PurchaseLine]]](Right(Nil)) {
        case (acc, (line, i)) => acc.flatMap(ls => parsedLine(line, i).map(ls :+ _))
      }
      total <- money(req.total, "total")
      source <- parsedSource(req)
    } yield (
      id,
      PurchaseCommand.RecordPurchase(id, storeId, purchasedAt, lines, total, source, correlation)
    )

  private def parsedLine(
      line: JsonFormats.PurchaseLineRequest,
      i: Int
  ): Either[String, PurchaseLine] =
    for {
      productId <- nonEmpty(line.productId, s"lines[$i].productId").map(ProductId.apply)
      quantity <- decimal(line.quantity, s"lines[$i].quantity")
      _ <- if quantity > 0 then Right(()) else Left(s"lines[$i].quantity must be positive")
      pricePaid <- money(line.pricePaid, s"lines[$i].pricePaid")
      lineTotal <- money(line.lineTotal, s"lines[$i].lineTotal")
    } yield PurchaseLine(productId, quantity, pricePaid, lineTotal)

  /**
   * `manual` by default, and `receipt` only with a blob id.
   *
   * The source is what later tells a price fact's quality apart (§2.4), so a receipt-sourced
   * purchase that cannot name the scan it came from is recorded as manual rather than claiming a
   * provenance it cannot show.
   */
  private def parsedSource(req: RecordPurchaseRequest): Either[String, PurchaseSource] =
    req.source.map(_.trim.toLowerCase).filter(_.nonEmpty) match {
      case None | Some("manual") => Right(PurchaseSource.Manual)
      case Some("receipt") =>
        req.receiptBlobId
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(PurchaseSource.Receipt.apply)
          .toRight("receiptBlobId is required when source is receipt")
      case Some(other) => Left(s"unknown source '$other' — expected manual or receipt")
    }

  private def nonEmpty(s: String, field: String): Either[String, String] =
    Option(s).map(_.trim).filter(_.nonEmpty).toRight(s"$field is required")

  private def decimal(s: String, field: String): Either[String, BigDecimal] =
    Try(BigDecimal(s.trim)).toEither.left.map(_ => s"$field is not a number: '$s'")

  private def money(s: String, field: String): Either[String, Money] =
    decimal(s, field).flatMap(d =>
      Money.cad(d).left.map((e: DomainError) => s"$field: ${e.message}")
    )

  private def instant(s: String): Either[String, Instant] =
    Try(Instant.parse(s.trim)).toEither.left.map(_ =>
      s"purchasedAt is not an ISO-8601 instant: '$s'"
    )
}

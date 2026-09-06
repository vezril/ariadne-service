package me.cference.ariadne.http

import me.cference.ariadne.domain.{CorrelationId, ListingKey, ProductId, StoreId}
import me.cference.ariadne.domain.resolution.{ResolutionCommand, ResolutionId}
import me.cference.ariadne.projection.ReadModelRepository
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The review queue — ariadne-ui's reason to exist (§6.5).
 *
 * The four verbs are Confirm, Reject, Merge and Split. Reads come from the review-queue projection;
 * writes go to the ResolutionCase entity through `decide`, which is a function rather than a shard
 * region so these routes can be tested without standing up a cluster.
 */
final class ReviewRoutes(
    repo: ReadModelRepository,
    decide: (ResolutionId, ResolutionCommand) => Future[Either[String, Unit]]
) extends SprayJsonSupport {

  import JsonFormats.*

  private def correlation(headerValue: Option[String]): CorrelationId =
    // Adopt an incoming correlation id, mint one when absent (§8) — so a decision made
    // in the UI stays on the same thread as the scrape that raised the case.
    CorrelationId(headerValue.getOrElse(java.util.UUID.randomUUID().toString))

  private def submit(id: String, cmd: CorrelationId => ResolutionCommand): Route =
    optionalHeaderValueByName("X-Correlation-Id") { cid =>
      onComplete(decide(ResolutionId(id), cmd(correlation(cid)))) {
        case Success(Right(_)) => complete(StatusCodes.Accepted -> AcceptedView(id, "accepted"))
        // A domain refusal is the caller's fault, not the server's: confirming a
        // non-candidate or re-deciding a closed case are both 409, not 500.
        case Success(Left(msg)) => complete(StatusCodes.Conflict -> ErrorView(msg))
        case Failure(e) => complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
      }
    }

  val routes: Route =
    pathPrefix("api" / "v1" / "resolutions") {
      concat(
        (get & pathEndOrSingleSlash & parameter("limit".as[Int].optional)) { limit =>
          onComplete(repo.listPendingCases(limit.getOrElse(50))) {
            case Success(rows) => complete(CasesResponse(rows.map(toCaseView)))
            case Failure(e) => complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
          }
        },
        (post & path(Segment / "confirm") & entity(as[DecisionRequest])) { (id, req) =>
          req.productId match {
            case Some(p) => submit(id, cid => ResolutionCommand.Confirm(ProductId(p), cid))
            case None =>
              complete(StatusCodes.BadRequest -> ErrorView("productId is required to confirm"))
          }
        },
        (post & path(Segment / "reject") & entity(as[DecisionRequest])) { (id, req) =>
          req.productId match {
            // Reject creates a NEW product from the subject, so the caller supplies the
            // id it will live under (§6.5).
            case Some(p) => submit(id, cid => ResolutionCommand.Reject(ProductId(p), cid))
            case None =>
              complete(
                StatusCodes.BadRequest -> ErrorView(
                  "productId (the new product) is required to reject"
                )
              )
          }
        },
        (post & path(Segment / "merge") & entity(as[DecisionRequest])) { (id, req) =>
          (req.winner, req.loser) match {
            case (Some(w), Some(l)) =>
              submit(id, cid => ResolutionCommand.RequestMerge(ProductId(w), ProductId(l), cid))
            case _ =>
              complete(
                StatusCodes.BadRequest -> ErrorView("winner and loser are required to merge")
              )
          }
        },
        (post & path(Segment / "split") & entity(as[DecisionRequest])) { (id, req) =>
          (req.listingStoreId, req.listingExternalId, req.productId) match {
            case (Some(s), Some(e), Some(p)) =>
              submit(
                id,
                cid => ResolutionCommand.RequestSplit(ListingKey(StoreId(s), e), ProductId(p), cid)
              )
            case _ =>
              complete(
                StatusCodes.BadRequest -> ErrorView(
                  "listingStoreId, listingExternalId and productId are required to split"
                )
              )
          }
        }
      )
    }
}

package me.cference.ariadne.http

import me.cference.ariadne.domain.{CorrelationId, Gtin, ListingKey, StoreId}
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.projection.ReadModelRepository
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The read surface (§4).
 *
 * Serves ariadne-ui AND dionysus-planner (Calvin, 2026-09-05). Everything here reads PROJECTIONS,
 * never entities: CQRS is held strictly, so a slow or recovering aggregate cannot stall a page
 * load. `resolve` is the one exception in spirit — it consults the matcher and may open a review
 * case — and it is still a read of the match index rather than of an aggregate.
 */
final class CatalogRoutes(
    repo: ReadModelRepository,
    /**
     * Path B of §6.4 — an INTERACTIVE resolve, injected so these routes stay testable without a
     * cluster. Deliberately not `resolveForScrape`: Path B must never auto-create a product. A
     * human is right there, and §6.4 says the caller registers deliberately instead.
     */
    resolve: (MatchSubject, CorrelationId) => Future[ResolutionOutcome] = (_, _) =>
      Future.successful(ResolutionOutcome.NoMatch)
) extends SprayJsonSupport {

  import JsonFormats.*

  val routes: Route =
    pathPrefix("api" / "v1") {
      concat(
        pathPrefix("products") {
          concat(
            // Search comes BEFORE the id route: `/products/search` would otherwise be
            // read as a product whose id is "search".
            (get & path("search") & parameter("q") & parameter("limit".as[Int].optional)) {
              (q, limit) =>
                onComplete(repo.searchProducts(q, limit.getOrElse(20))) {
                  case Success(rows) => complete(ProductsResponse(rows.map(toProductView)))
                  case Failure(e) =>
                    complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
                }
            },
            // Before the id route, like `search`: `/products/resolve` would otherwise
            // read as a product whose id is "resolve".
            (post & path("resolve") & entity(as[ResolveProductRequest])) { req =>
              if req.name.trim.isEmpty && req.gtin.isEmpty then
                complete(
                  StatusCodes.BadRequest -> ErrorView("name or gtin is required to resolve")
                )
              else
                optionalHeaderValueByName("X-Correlation-Id") { cid =>
                  val correlation =
                    CorrelationId(cid.getOrElse(java.util.UUID.randomUUID().toString))
                  subjectFrom(req) match {
                    case Left(why) => complete(StatusCodes.BadRequest -> ErrorView(why))
                    case Right(subject) =>
                      onComplete(resolve(subject, correlation)) {
                        case Success(outcome) => complete(toResolutionView(subject, outcome))
                        case Failure(e) =>
                          complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
                      }
                  }
                }
            },
            (get & path(Segment)) { id =>
              onComplete(repo.getProduct(id)) {
                case Success(Some(row)) => complete(toProductView(row))
                case Success(None) => complete(StatusCodes.NotFound -> ErrorView(s"no product $id"))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
              }
            },
            // Price AT a store — the exact-over-area resolution of §2.3.1. The response
            // says which kind it got, so the UI can distinguish a receipt from a flyer.
            (get & path(Segment / "price") & parameter("storeId")) { (id, storeId) =>
              onComplete(repo.currentPriceForStore(id, storeId)) {
                case Success(Some(p)) => complete(toPriceView(p))
                case Success(None) =>
                  complete(StatusCodes.NotFound -> ErrorView(s"no price for $id at $storeId"))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
              }
            }
          )
        }
      )
    }

  private def subjectFrom(req: ResolveProductRequest): Either[String, MatchSubject] = {
    val listing = (req.storeId, req.externalId) match {
      case (Some(s), Some(e)) => Right(Some(ListingKey(StoreId(s), e)))
      case (None, None) => Right(None)
      // Half a listing key is not a weaker key, it is a different question. Silently
      // dropping the half that was supplied would answer a question nobody asked.
      case _ => Left("storeId and externalId must be supplied together")
    }
    for {
      l <- listing
      g <- req.gtin match {
        case None => Right(None)
        case Some(raw) => Gtin.parse(raw).map(Some(_)).left.map(e => s"gtin: $e")
      }
    } yield MatchSubject(req.name.trim, req.brand.map(_.trim).filter(_.nonEmpty), g, l)
  }

  /**
   * All three outcomes are 200.
   *
   * A no-match is an ANSWER — the catalogue genuinely does not know this product — and dressing it
   * as 404 would make a correct reply indistinguishable from a broken request. The caller's next
   * move differs per outcome, so the outcome is a field they can switch on rather than a status
   * code they have to interpret.
   */
  private def toResolutionView(
      subject: MatchSubject,
      outcome: ResolutionOutcome
  ): ProductResolutionView =
    outcome match {
      case ResolutionOutcome.Matched(productId, confidence, method) =>
        ProductResolutionView(
          "matched",
          Some(productId.value),
          Some(confidence.toDouble),
          Some(method.toString),
          None,
          Nil
        )
      case ResolutionOutcome.Ambiguous(candidates) =>
        ProductResolutionView(
          "ambiguous",
          None,
          None,
          None,
          // The case has been opened by now, and this is the handle a caller confirms
          // against once a human picks (§6.5). Derived, so it is the SAME case a scrape
          // of the same subject would have opened rather than a parallel one.
          Some(ResolutionService.caseIdFor(subject).value),
          candidates.map(c => CandidateView(c.productId.value, c.score.toDouble, c.notes))
        )
      case ResolutionOutcome.NoMatch =>
        ProductResolutionView("no_match", None, None, None, None, Nil)
    }
}

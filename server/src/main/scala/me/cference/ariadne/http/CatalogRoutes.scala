package me.cference.ariadne.http

import me.cference.ariadne.projection.ReadModelRepository
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.util.{Failure, Success}

/**
 * The browser-facing read surface (§4).
 *
 * REST is for ariadne-ui only — gRPC is the service-to-service protocol, and this is a thin mirror
 * of the same queries rather than a second behaviour. Everything here reads PROJECTIONS, never
 * entities: CQRS is held strictly, so a slow or recovering aggregate cannot stall a page load.
 */
final class CatalogRoutes(repo: ReadModelRepository) extends SprayJsonSupport {

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
}

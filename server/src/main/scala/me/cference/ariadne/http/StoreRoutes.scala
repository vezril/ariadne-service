package me.cference.ariadne.http

import me.cference.ariadne.domain.{Area, ChainId, CorrelationId, StoreId}
import me.cference.ariadne.domain.store.StoreCommand
import me.cference.ariadne.projection.ReadModelRepository
import me.cference.ariadne.resolver.StoreResolver
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The store surface (§7.2) — list, resolve, register.
 *
 * This closed a gap that neither side had listed: `CatalogRoutes` exposed `storeId` only as a query
 * parameter, so nothing could discover, resolve or create the franchise it names. The receipt
 * migration could not proceed without it, which is a dependency that only becomes visible when
 * something tries to USE the surface rather than read about it.
 *
 * Reads come from the `stores` projection; the single write goes to `StoreEntity` through `decide`,
 * a function rather than a shard region so these routes are testable without a cluster (the
 * `ReviewRoutes` pattern).
 */
final class StoreRoutes(
    repo: ReadModelRepository,
    resolver: StoreResolver,
    decide: (StoreId, StoreCommand) => Future[Either[String, Unit]]
) extends SprayJsonSupport {

  import JsonFormats.*

  val routes: Route =
    pathPrefix("api" / "v1" / "stores") {
      concat(
        // `resolve` is declared BEFORE the id route, or `/stores/resolve` reads as a
        // store whose id is "resolve" — the same ordering trap `/products/search` has.
        (get & path("resolve") & parameter("q") & parameter("area".optional)) { (q, area) =>
          onComplete(resolver.resolve(q, area)) {
            case Success(matches) =>
              complete(
                StoreResolutionView(
                  query = q,
                  // The server states uniqueness rather than letting the caller infer
                  // it from a list of one. A receipt saying "Metro" names a banner, so
                  // several correct matches is ordinary — and a caller that read one
                  // match as certainty would auto-attribute a purchase to a specific
                  // franchise on a coincidence.
                  unique = matches.sizeIs == 1,
                  matches = matches.map(toStoreMatchView)
                )
              )
            case Failure(e) => complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
          }
        },
        (get & pathEndOrSingleSlash & parameters(
          "chainId".optional,
          "area".optional,
          "includeInactive".as[Boolean].optional,
          "limit".as[Int].optional
        )) { (chainId, area, includeInactive, limit) =>
          onComplete(
            repo.listStores(
              chainId,
              area,
              activeOnly = !includeInactive.getOrElse(false),
              limit.getOrElse(100)
            )
          ) {
            case Success(rows) => complete(StoresResponse(rows.map(toStoreView)))
            case Failure(e) => complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
          }
        },
        (post & pathEndOrSingleSlash & entity(as[RegisterStoreRequest])) { req =>
          validated(req) match {
            case Left(why) => complete(StatusCodes.BadRequest -> ErrorView(why))
            case Right(id) =>
              optionalHeaderValueByName("X-Correlation-Id") { cid =>
                val correlation =
                  CorrelationId(cid.getOrElse(java.util.UUID.randomUUID().toString))
                val cmd = StoreCommand.RegisterStore(
                  id,
                  req.name.trim,
                  ChainId(req.chainId.trim),
                  Area(req.area.trim),
                  req.label.map(_.trim).filter(_.nonEmpty),
                  correlation
                )
                onComplete(decide(id, cmd)) {
                  case Success(Right(_)) =>
                    complete(StatusCodes.Accepted -> AcceptedView(id.value, "accepted"))
                  // Registering a franchise that already exists is the caller's
                  // situation, not a server fault — 409, and the id in the body is
                  // the one they wanted, so a retry is answerable without a lookup.
                  case Success(Left(msg)) => complete(StatusCodes.Conflict -> ErrorView(msg))
                  case Failure(e) =>
                    complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
                }
              }
          }
        },
        (get & path(Segment)) { id =>
          onComplete(repo.getStore(id)) {
            case Success(Some(row)) => complete(toStoreView(row))
            case Success(None) => complete(StatusCodes.NotFound -> ErrorView(s"no store $id"))
            case Failure(e) => complete(StatusCodes.InternalServerError -> ErrorView(e.getMessage))
          }
        }
      )
    }

  /**
   * Reject what the aggregate would reject anyway, at the edge where the caller can be told why.
   *
   * `chainId` and `area` are checked here and not only in the domain because they are the two
   * fields a blank value damages SILENTLY: an empty chain makes the store invisible to every
   * regional price observation (§2.3.1), and an empty area makes it uncovered by any flyer. Neither
   * looks broken from the outside — the store simply never has a price.
   */
  private def validated(req: RegisterStoreRequest): Either[String, StoreId] =
    if req.name.trim.isEmpty then Left("name is required")
    else if req.chainId.trim.isEmpty then
      Left("chainId is required — a store with no chain is covered by no regional price")
    else if req.area.trim.isEmpty then
      Left("area is required — a store with no area is covered by no flyer")
    else
      Right(
        req.id
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(StoreId.apply)
          .getOrElse(StoreRoutes.derivedId(req))
      )
}

object StoreRoutes {

  /**
   * The id for a franchise the caller did not name.
   *
   * DERIVED, not random, for the same reason provisional product ids are (§6.4.1): a receipt flow
   * has no id to offer, and a retry — a dropped response, a double tap on a picker — must land on
   * the store it already created rather than register a second one. The aggregate then refuses the
   * duplicate, which the route reports as 409 with that id.
   *
   * Keyed on chain+area+name because that triple is what distinguishes one franchise from another
   * in this model; two stores agreeing on all three are the same store.
   */
  def derivedId(req: JsonFormats.RegisterStoreRequest): StoreId = {
    val key = List(req.chainId, req.area, req.name)
      .map(_.trim.toLowerCase.replaceAll("\\s+", " "))
      .mkString("|")
    StoreId(s"store-${java.util.UUID.nameUUIDFromBytes(key.getBytes("UTF-8"))}")
  }
}

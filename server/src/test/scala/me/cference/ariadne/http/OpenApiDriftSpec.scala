package me.cference.ariadne.http

import me.cference.ariadne.domain.resolution.{ResolutionCommand, ResolutionId}
import me.cference.ariadne.projection.ReadModelRepository
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.server.Directives.concat
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.io.Source
import scala.util.Using

/**
 * The contract drift gate.
 *
 * The REST surface has TWO consumers now (ariadne-ui and dionysus-planner), which makes the OpenAPI
 * document a published contract rather than an affordance this service can change freely. A
 * hand-maintained spec beside hand-written routes drifts silently; this fails CI when it does.
 *
 * Adapted from dionysus-planner's `openapiCoverage.test.ts`, and deliberately NOT a copy of it.
 * They described their own gate's weaknesses, and the sharpest one is a fail-open: theirs regexes
 * route source for `export async function GET`, so wrapping handlers (`export const GET =
 * withLogging(...)`) would silently stop detecting every route and still pass green. They nearly
 * shipped exactly that. A gate that fails OPEN when code style changes is worse than no gate,
 * because it also removes the suspicion that something is unchecked.
 *
 * So the equivalent assertion here is made at RUNTIME instead of over source text: every path in
 * the spec is dispatched against the real route tree and must be `handled`. That cannot fail open
 * from a refactor — if a route stops existing, the router stops matching it, whatever the code
 * looks like. Route-level testing is infrastructure they did not have and we do.
 *
 * What this still does NOT check, stated so nobody mistakes its scope: it verifies an operation
 * EXISTS and is reachable, never that the spec describes it correctly. Wrong parameters, wrong
 * status codes, a response schema that has drifted from reality — all pass. That is existence
 * drift, not shape drift, and closing it needs a generated spec rather than a hand-written one.
 */
final class OpenApiDriftSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with SprayJsonSupport {

  import JsonFormats.*

  /** A representative request per documented operation. */
  private val probes: Map[(String, String), HttpRequest] = Map(
    ("get", "/api/v1/products/{id}") -> Get("/api/v1/products/p-1"),
    ("get", "/api/v1/products/search") -> Get("/api/v1/products/search?q=butter"),
    ("get", "/api/v1/products/{id}/price") -> Get("/api/v1/products/p-1/price?storeId=s-1"),
    ("get", "/api/v1/resolutions") -> Get("/api/v1/resolutions"),
    ("post", "/api/v1/resolutions/{id}/confirm") ->
      Post("/api/v1/resolutions/r-1/confirm", DecisionRequest(Some("p-1"), None, None, None, None)),
    ("post", "/api/v1/resolutions/{id}/reject") ->
      Post("/api/v1/resolutions/r-1/reject", DecisionRequest(Some("p-1"), None, None, None, None)),
    ("post", "/api/v1/resolutions/{id}/merge") ->
      Post(
        "/api/v1/resolutions/r-1/merge",
        DecisionRequest(None, Some("w"), Some("l"), None, None)
      ),
    ("post", "/api/v1/resolutions/{id}/split") ->
      Post(
        "/api/v1/resolutions/r-1/split",
        DecisionRequest(Some("p"), None, None, Some("s"), Some("e"))
      ),
    ("get", "/health") -> Get("/health")
  )

  // Reads are never reached: `handled` is decided by the router, and a repository that
  // fails is still a route that matched. Nothing here touches a database.
  private val repo: ReadModelRepository = null
  private val decide: (ResolutionId, ResolutionCommand) => Future[Either[String, Unit]] =
    (_, _) => Future.successful(Right(()))

  private val routes = concat(
    new CatalogRoutes(repo).routes,
    new ReviewRoutes(repo, decide).routes,
    HealthRoutes("test", () => true),
    new DocsRoutes().routes
  )

  private val specText: String =
    Using.resource(Source.fromResource("openapi/ariadne.yaml"))(_.mkString)

  /** (method, path) for every documented operation. */
  private val documented: List[(String, String)] = {
    val lines = specText.linesIterator.toList
    val PathLine = """^  (/\S*?):\s*$""".r
    val MethodLine = """^    (get|post|put|delete|patch):\s*$""".r
    lines
      .foldLeft((Option.empty[String], List.empty[(String, String)])) {
        case ((current, acc), line) =>
          line match {
            case PathLine(p) => (Some(p), acc)
            case MethodLine(m) => (current, current.map(p => (m, p)).toList ::: acc)
            case _ => (current, acc)
          }
      }
      ._2
      .reverse
  }

  "the drift gate itself" should {
    "find a plausible number of operations, so a broken parse fails CLOSED" in {
      // dionysus-planner's gate would have silently matched zero routes after a
      // refactor and passed green. A parser that finds nothing must fail, not pass —
      // this assertion is the difference between failing closed and failing open.
      documented.size should be >= 9
      documented.map(_._2).distinct.size should be >= 8
    }
  }

  "every documented operation" should {

    "have a probe — a new spec entry cannot be added without exercising it" in {
      val missing = documented.filterNot(probes.contains)
      withClue(s"documented but unprobed: $missing — add a probe to OpenApiDriftSpec\n") {
        missing shouldBe empty
      }
    }

    "be reachable on the real route tree" in {
      // Assertion 1, made at runtime rather than by regexing source. It cannot fail
      // open from a refactor: if a route stops existing the router stops matching it,
      // whatever the code looks like.
      documented.foreach { case key @ (method, path) =>
        val request = probes(key)
        request ~> routes ~> check {
          withClue(s"$method $path is documented but the router does not handle it\n") {
            handled shouldBe true
          }
        }
      }
    }
  }

  "the spec" should {
    "contain NO PHANTOM paths — a deleted route left documented is worse than an undocumented one" in {
      // dionysus-planner's point: this is the half people forget, and it is the half
      // that catches deletions. A client builds against something that will 404.
      val phantom = probes.keys.filterNot(documented.contains).toList
      withClue(s"probed but undocumented (or removed from the spec): $phantom\n") {
        phantom shouldBe empty
      }
    }
  }

  "the Insomnia collection" should {
    "cover every documented operation, so the committed artifact is provably not stale" in {
      // Regenerating is a build step someone forgets. This is what fails CI when they do.
      val collection = Using.resource(Source.fromFile("insomnia/ariadne.yaml"))(_.mkString)
      val uncovered = documented.filterNot { case (_, path) =>
        // Compare on the literal segments; {placeholders} are template variables in the
        // collection and never match textually.
        path.split('/').filter(s => s.nonEmpty && !s.startsWith("{")).forall(collection.contains)
      }
      withClue(s"documented but absent from insomnia/ariadne.yaml: $uncovered\n") {
        uncovered shouldBe empty
      }
    }
  }
}

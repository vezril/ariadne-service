package me.cference.ariadne.http

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.resolution.*
import me.cference.ariadne.projection.{PostgresFixture, ProjectionHandlers, ReadModelRepository}
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives.concat
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.*

import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.Future

/**
 * The browser surface, against a real Postgres.
 *
 * Writes are injected as a function rather than a shard region: what these tests are for is the
 * HTTP contract — status codes, payload shapes, required fields — and standing up a cluster to
 * assert a 409 would test Pekko rather than Ariadne.
 */
final class RestSurfaceSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with ScalatestRouteTest
    with SprayJsonSupport
    with PostgresFixture {

  import JsonFormats.*

  // ScalatestRouteTest's default patience is 150ms — far too short for seeding a real
  // Postgres, and it aborts the suite rather than failing a test.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Millis))

  private lazy val repo = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new ReadModelRepository(new PostgresqlConnectionFactory(cfg))(using executor)
  }

  private var lastCommand: Option[(ResolutionId, ResolutionCommand)] = None
  private var nextResult: Either[String, Unit] = Right(())

  private var nextOutcome: ResolutionOutcome = ResolutionOutcome.NoMatch
  private var lastSubject: Option[MatchSubject] = None

  private lazy val catalog = new CatalogRoutes(
    repo,
    (subject, _) => { lastSubject = Some(subject); Future.successful(nextOutcome) }
  )
  private lazy val review = new ReviewRoutes(
    repo,
    (id, cmd) => { lastCommand = Some(id -> cmd); Future.successful(nextResult) }
  )
  private lazy val routes = concat(catalog.routes, review.routes)

  override def beforeAll(): Unit = {
    applySchema()
    ProjectionHandlers
      .product(repo)(
        "product|p-1",
        ProductEvent.ProductRegistered(
          ProductId("p-1"),
          "Lactantia Salted Butter",
          Some("Lactantia"),
          Some("dairy"),
          Some(Quantity.unsafe(BigDecimal(454), MeasureUnit.Gram)),
          Some(Gtin.unsafe("4006381333931")),
          Origin.Manual,
          ProductStatus.Active
        )
      )(using executor)
      .futureValue
    ProjectionHandlers
      .resolution(repo)(
        "resolution|r-1",
        ResolutionEvent.ResolutionProposed(
          ResolutionId("r-1"),
          MatchSubject(
            "Lactantia Butter",
            Some("Lactantia"),
            listing = Some(ListingKey(StoreId("s-1"), "ext-9"))
          ),
          List(ScoredCandidate(ProductId("p-1"), Confidence.unsafe(0.81), List("size conflict")))
        )
      )(using executor)
      .futureValue
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  "the catalog surface" should {

    "return a product by id" in {
      Get("/api/v1/products/p-1") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val v = responseAs[ProductView]
        v.name shouldBe "Lactantia Salted Butter"
        v.size shouldBe Some("454 g")
      }
    }

    "404 an unknown product rather than returning an empty body" in {
      Get("/api/v1/products/nope") ~> routes ~> check {
        status shouldBe StatusCodes.NotFound
        responseAs[ErrorView].error should include("nope")
      }
    }

    "search, and route /products/search as a search rather than as an id" in {
      // Ordering of directives matters here: `/products/search` would otherwise be read
      // as a product whose id is literally "search".
      Get("/api/v1/products/search?q=butter") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[ProductsResponse].products.map(_.id) should contain("p-1")
      }
    }

    "return collections in an ENVELOPE, not a bare array" in {
      // A top-level array has nowhere to grow; paging later would be a breaking change.
      Get("/api/v1/products/search?q=butter") ~> routes ~> check {
        responseAs[JsValue].asJsObject.fields.keys should contain("products")
      }
    }
  }

  "the review queue" should {

    "list pending cases with their candidates as the matcher offered them" in {
      Get("/api/v1/resolutions") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val body = responseAs[CasesResponse]
        body.cases should have size 1
        val c = body.cases.head
        c.subject.name shouldBe "Lactantia Butter"
        c.subject.storeId shouldBe Some("s-1")
        // The score the DOMAIN computed, not a re-derivation (§6.6).
        c.candidates.convertTo[JsArray].elements.head.asJsObject.fields("score") shouldBe JsNumber(
          0.81
        )
      }
    }

    "accept a confirm and pass the right command through" in {
      nextResult = Right(())
      Post(
        "/api/v1/resolutions/r-1/confirm",
        DecisionRequest(Some("p-1"), None, None, None, None)
      ) ~> routes ~> check {
        status shouldBe StatusCodes.Accepted
        lastCommand.map(_._1) shouldBe Some(ResolutionId("r-1"))
        lastCommand.map(_._2) shouldBe Some(
          ResolutionCommand.Confirm(
            ProductId("p-1"),
            lastCommand.get._2.asInstanceOf[ResolutionCommand.Confirm].correlationId
          )
        )
      }
    }

    "ADOPT an incoming correlation id so a UI decision stays on the scrape's thread" in {
      Post("/api/v1/resolutions/r-1/confirm", DecisionRequest(Some("p-1"), None, None, None, None))
        .withHeaders(RawHeader("X-Correlation-Id", "trace-me")) ~> routes ~> check {
        status shouldBe StatusCodes.Accepted
        lastCommand.get._2
          .asInstanceOf[ResolutionCommand.Confirm]
          .correlationId shouldBe CorrelationId("trace-me")
      }
    }

    "MINT a correlation id when the caller sends none" in {
      Post(
        "/api/v1/resolutions/r-1/confirm",
        DecisionRequest(Some("p-1"), None, None, None, None)
      ) ~> routes ~> check {
        lastCommand.get._2
          .asInstanceOf[ResolutionCommand.Confirm]
          .correlationId
          .value should not be empty
      }
    }

    "map a domain refusal to 409, not 500 — it is the caller's mistake" in {
      // Confirming a non-candidate, or re-deciding a closed case, are conflicts.
      nextResult = Left("p-x was not among the offered candidates")
      Post(
        "/api/v1/resolutions/r-1/confirm",
        DecisionRequest(Some("p-x"), None, None, None, None)
      ) ~> routes ~> check {
        status shouldBe StatusCodes.Conflict
        responseAs[ErrorView].error should include("candidates")
      }
      nextResult = Right(())
    }

    "400 a verb that is missing its required fields" in {
      Post(
        "/api/v1/resolutions/r-1/confirm",
        DecisionRequest(None, None, None, None, None)
      ) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("productId")
      }
      Post(
        "/api/v1/resolutions/r-1/merge",
        DecisionRequest(None, Some("w"), None, None, None)
      ) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
      Post(
        "/api/v1/resolutions/r-1/split",
        DecisionRequest(Some("p"), None, None, Some("s-1"), None)
      ) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "carry all four verbs through to the right command" in {
      Post(
        "/api/v1/resolutions/r-1/reject",
        DecisionRequest(Some("p-new"), None, None, None, None)
      ) ~> routes ~> check {
        lastCommand.get._2 shouldBe a[ResolutionCommand.Reject]
      }
      Post(
        "/api/v1/resolutions/r-1/merge",
        DecisionRequest(None, Some("w"), Some("l"), None, None)
      ) ~> routes ~> check {
        lastCommand.get._2 shouldBe a[ResolutionCommand.RequestMerge]
      }
      Post(
        "/api/v1/resolutions/r-1/split",
        DecisionRequest(Some("p-new"), None, None, Some("s-1"), Some("ext-9"))
      ) ~> routes ~> check {
        lastCommand.get._2 shouldBe a[ResolutionCommand.RequestSplit]
      }
    }
  }

  "resolving a subject (§6.4 Path B)" should {

    "answer `matched` with the product and how it was decided" in {
      nextOutcome =
        ResolutionOutcome.Matched(ProductId("p-1"), Confidence.unsafe(0.97), MatchMethod.Fuzzy)
      Post("/api/v1/products/resolve", ResolveProductRequest("butter", None, None, None, None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.OK
          val v = responseAs[ProductResolutionView]
          v.outcome shouldBe "matched"
          v.productId shouldBe Some("p-1")
          v.confidence shouldBe Some(0.97)
          v.method shouldBe Some("Fuzzy")
        }
    }

    "answer `no_match` with 200 — absence is an ANSWER, not a broken request" in {
      // A 404 here would be indistinguishable from a malformed call, and the caller's
      // correct next move (register deliberately, §6.4) is not an error path.
      nextOutcome = ResolutionOutcome.NoMatch
      Post("/api/v1/products/resolve", ResolveProductRequest("nothing", None, None, None, None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.OK
          val v = responseAs[ProductResolutionView]
          v.outcome shouldBe "no_match"
          v.productId shouldBe None
          v.candidates shouldBe empty
        }
    }

    "answer `ambiguous` with the candidates AND the case to confirm against" in {
      nextOutcome = ResolutionOutcome.Ambiguous(
        List(ScoredCandidate(ProductId("p-1"), Confidence.unsafe(0.81), List("size conflict")))
      )
      val req = ResolveProductRequest("Lactantia Butter", Some("Lactantia"), None, None, None)
      Post("/api/v1/products/resolve", req) ~> routes ~> check {
        val v = responseAs[ProductResolutionView]
        v.outcome shouldBe "ambiguous"
        v.candidates.map(_.productId) shouldBe List("p-1")
        v.candidates.head.notes shouldBe List("size conflict")
        // The SAME case a scrape of this subject would open, not a parallel one — the
        // id is derived from the subject, which is what keeps one question in one row.
        v.caseId shouldBe Some(
          ResolutionService.caseIdFor(MatchSubject("Lactantia Butter", Some("Lactantia"))).value
        )
      }
    }

    "refuse a request that identifies nothing" in {
      Post("/api/v1/products/resolve", ResolveProductRequest("  ", None, None, None, None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.BadRequest
        }
    }

    "refuse HALF a listing key rather than silently dropping it" in {
      // Half a key is not a weaker question, it is a different one. Dropping the half
      // that was supplied would answer something the caller did not ask.
      val req = ResolveProductRequest("butter", None, None, Some("s-1"), None)
      Post("/api/v1/products/resolve", req) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("externalId")
      }
    }

    "reject a GTIN that fails its check digit before it reaches the matcher" in {
      val req = ResolveProductRequest("butter", None, Some("4006381333930"), None, None)
      Post("/api/v1/products/resolve", req) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("gtin")
      }
    }
  }
}

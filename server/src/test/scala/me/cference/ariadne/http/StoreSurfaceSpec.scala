package me.cference.ariadne.http

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.{Area, ChainId, StoreId}
import me.cference.ariadne.domain.store.{StoreCommand, StoreEvent}
import me.cference.ariadne.projection.{PostgresFixture, ProjectionHandlers, ReadModelRepository}
import me.cference.ariadne.resolver.StoreResolver
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

/**
 * The store surface (§7.2), against a real Postgres.
 *
 * The catalogue below is the shape that makes store resolution hard and product resolution easy:
 * THREE IGA franchises. A receipt saying "IGA" is correct about all three and decisive about none,
 * which is the ordinary case rather than the edge one.
 */
final class StoreSurfaceSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with ScalatestRouteTest
    with SprayJsonSupport
    with PostgresFixture {

  import JsonFormats.*

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

  private var lastCommand: Option[(StoreId, StoreCommand)] = None
  private var nextResult: Either[String, Unit] = Right(())

  private lazy val routes = new StoreRoutes(
    repo,
    new StoreResolver(repo)(using executor),
    (id, cmd) => { lastCommand = Some(id -> cmd); Future.successful(nextResult) }
  ).routes

  private def seed(id: String, name: String, chain: String, area: String): Unit =
    ProjectionHandlers
      .store(repo)(
        s"store|$id",
        StoreEvent.StoreRegistered(StoreId(id), name, ChainId(chain), Area(area), None)
      )(using executor)
      .futureValue

  override def beforeAll(): Unit = {
    applySchema()
    seed("s-iga-1", "IGA Beaubien", "iga", "H2X")
    seed("s-iga-2", "IGA Papineau", "iga", "H2X")
    seed("s-iga-3", "IGA Laval", "iga", "H7N")
    seed("s-metro-1", "Metro Plus Mont-Royal", "metro", "H2X")
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  "listing franchises" should {

    "return them all by default" in {
      Get("/api/v1/stores") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[StoresResponse].stores.map(_.id) should contain allOf ("s-iga-1", "s-metro-1")
      }
    }

    "narrow by chain and by area independently" in {
      Get("/api/v1/stores?chainId=iga") ~> routes ~> check {
        responseAs[StoresResponse].stores.map(_.id) should contain theSameElementsAs
          List("s-iga-1", "s-iga-2", "s-iga-3")
      }
      Get("/api/v1/stores?area=H7N") ~> routes ~> check {
        responseAs[StoresResponse].stores.map(_.id) shouldBe List("s-iga-3")
      }
      Get("/api/v1/stores?chainId=iga&area=H2X") ~> routes ~> check {
        responseAs[StoresResponse].stores.map(_.id) should contain theSameElementsAs
          List("s-iga-1", "s-iga-2")
      }
    }

    "return one franchise by id" in {
      Get("/api/v1/stores/s-iga-1") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val v = responseAs[StoreView]
        v.name shouldBe "IGA Beaubien"
        v.chainId shouldBe "iga"
        v.area shouldBe "H2X"
      }
    }

    "404 an unknown id rather than an empty object" in {
      Get("/api/v1/stores/nope") ~> routes ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
  }

  "resolving typed receipt text" should {

    "return EVERY franchise of a named chain, and say it did not narrow" in {
      // The heart of §7.1. "IGA" is correct about three stores. Picking one would
      // attribute a purchase to a specific franchise on no evidence whatsoever — the
      // error §2.3.1 refuses for prices, arriving by way of a receipt.
      Get("/api/v1/stores/resolve?q=IGA") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val v = responseAs[StoreResolutionView]
        v.matches.map(_.store.id) should contain allOf ("s-iga-1", "s-iga-2", "s-iga-3")
        v.unique shouldBe false
        v.matches.map(_.why).distinct shouldBe List("chain 'iga' matches the text")
      }
    }

    "narrow to one when the caller supplies the area, and SAY so" in {
      Get("/api/v1/stores/resolve?q=IGA&area=H7N") ~> routes ~> check {
        val v = responseAs[StoreResolutionView]
        v.matches.map(_.store.id) shouldBe List("s-iga-3")
        // The claim the caller is allowed to auto-pick on.
        v.unique shouldBe true
      }
    }

    "match a franchise by its own name, not only by its chain" in {
      Get("/api/v1/stores/resolve?q=Beaubien") ~> routes ~> check {
        val v = responseAs[StoreResolutionView]
        v.matches.map(_.store.id) shouldBe List("s-iga-1")
        v.matches.head.why shouldBe "store name matches the text"
      }
    }

    "find nothing for text that names nothing, rather than offering the least-bad row" in {
      Get("/api/v1/stores/resolve?q=Costco") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val v = responseAs[StoreResolutionView]
        v.matches shouldBe empty
        // Emphatically not `unique`: no answer is not one answer.
        v.unique shouldBe false
      }
    }
  }

  "registering a franchise" should {

    "accept one and derive a stable id when none is given" in {
      val req = RegisterStoreRequest(None, "IGA Rosemont", "iga", "H2S", Some("Rosemont"))
      Post("/api/v1/stores", req) ~> routes ~> check {
        status shouldBe StatusCodes.Accepted
        val id = responseAs[AcceptedView].id
        id shouldBe StoreRoutes.derivedId(req).value

        val (sentId, cmd) = lastCommand.get
        sentId.value shouldBe id
        cmd shouldBe a[StoreCommand.RegisterStore]
      }
    }

    "derive the SAME id for a retry, so a dropped response cannot double-register" in {
      // A receipt flow has no id to offer and a double tap on a picker is ordinary.
      val a = RegisterStoreRequest(None, "IGA Rosemont", "iga", "H2S", None)
      val b = RegisterStoreRequest(None, "  iga   rosemont ", "IGA", "h2s", Some("different label"))
      // Case and whitespace drift, and a label that is not part of identity.
      StoreRoutes.derivedId(a).value shouldBe StoreRoutes.derivedId(b).value
    }

    "honour an id the caller does supply" in {
      Post("/api/v1/stores", RegisterStoreRequest(Some("s-mine"), "IGA X", "iga", "H2X", None)) ~>
        routes ~> check {
          responseAs[AcceptedView].id shouldBe "s-mine"
        }
    }

    "refuse a blank chainId or area — both fail SILENTLY downstream" in {
      // Neither looks broken from outside: the store simply never has a price, because
      // no regional observation covers it (§2.3.1).
      Post("/api/v1/stores", RegisterStoreRequest(None, "Somewhere", "  ", "H2X", None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[ErrorView].error should include("chainId")
        }
      Post("/api/v1/stores", RegisterStoreRequest(None, "Somewhere", "iga", "", None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[ErrorView].error should include("area")
        }
      Post("/api/v1/stores", RegisterStoreRequest(None, "   ", "iga", "H2X", None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[ErrorView].error should include("name")
        }
    }

    "report a duplicate as 409, not 500 — it is the caller's situation, not a fault" in {
      nextResult = Left("already registered")
      Post("/api/v1/stores", RegisterStoreRequest(None, "IGA Beaubien", "iga", "H2X", None)) ~>
        routes ~> check {
          status shouldBe StatusCodes.Conflict
        }
      nextResult = Right(())
    }
  }
}

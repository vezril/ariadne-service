package me.cference.ariadne.http

import me.cference.ariadne.domain.{CorrelationId, Money, ProductId, PurchaseId, StoreId}
import me.cference.ariadne.domain.purchase.{
  Purchase,
  PurchaseCommand,
  PurchaseLine,
  PurchaseSource,
  PurchaseState
}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.Future

/**
 * The manual purchase surface (§7, hot spot #2).
 *
 * No Postgres here: `PurchaseRoutes` takes `decide` as a FUNCTION rather than a shard region, so
 * the whole surface is exercisable without a cluster or a database (the `ReviewRoutes` pattern).
 * The aggregate's own rules are tested in core; what matters here is the translation — what the
 * wire shape is allowed to be, and which refusal maps to which status.
 */
final class PurchaseSurfaceSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with SprayJsonSupport {

  import JsonFormats.*

  private val at = "2026-11-10T14:30:00Z"
  private val oneLine = List(PurchaseLineRequest("prod-1", "1", "4.99", "4.99"))

  /** Captures what the route asked the aggregate to do; answers however the test wants. */
  final private class Spy(answer: Either[String, Unit] = Right(())) {
    val seen: mutable.ListBuffer[(PurchaseId, PurchaseCommand)] = mutable.ListBuffer.empty
    val route: Route = new PurchaseRoutes((id, cmd) => {
      seen += ((id, cmd))
      Future.successful(answer)
    }).routes
    def recorded: Option[PurchaseCommand.RecordPurchase] =
      seen.collectFirst { case (_, c: PurchaseCommand.RecordPurchase) => c }
  }

  private def body(
      id: String = "p-1",
      lines: List[PurchaseLineRequest] = oneLine,
      total: String = "4.99",
      source: Option[String] = None,
      receiptBlobId: Option[String] = None
  ) = RecordPurchaseRequest(id, "store-1", at, lines, total, source, receiptBlobId)

  "recording a purchase" should {

    "accept a well-formed receipt and pass the caller's own id through" in {
      val spy = new Spy
      Post("/api/v1/purchases", body()) ~> spy.route ~> check {
        status shouldBe StatusCodes.Accepted
        responseAs[AcceptedView].id shouldBe "p-1"
      }
      // §7.1: the id is the CALLER's, because that is what makes a retry addressable. A
      // server-minted id would leave the client unable to name what it already sent.
      spy.seen.map(_._1).toList shouldBe List(PurchaseId("p-1"))
      val cmd = spy.recorded.getOrElse(fail("expected a RecordPurchase"))
      cmd.storeId shouldBe StoreId("store-1")
      cmd.purchasedAt shouldBe Instant.parse(at)
      cmd.total shouldBe Money.unsafe(BigDecimal("4.99"))
      cmd.source shouldBe PurchaseSource.Manual
      cmd.lines shouldBe List(
        PurchaseLine(
          ProductId("prod-1"),
          BigDecimal(1),
          Money.unsafe(BigDecimal("4.99")),
          Money.unsafe(BigDecimal("4.99"))
        )
      )
    }

    "report a re-record of the SAME purchase as success, so a client outbox can drain" in {
      // §7.1, and the reason this surface has the shape it does. Ariadne being
      // unreachable cannot block someone typing a receipt, so the client buffers and
      // retries; an error on a retry is indistinguishable from a genuine failure and the
      // row would retry forever. Asserted against the real aggregate rather than a stub,
      // because the claim is about what `decide` actually answers.
      val now = Instant.parse("2026-11-11T00:00:00Z")
      val cmd = PurchaseCommand.RecordPurchase(
        PurchaseId("p-1"),
        StoreId("store-1"),
        Instant.parse(at),
        List(
          PurchaseLine(
            ProductId("prod-1"),
            BigDecimal(1),
            Money.unsafe(BigDecimal("4.99")),
            Money.unsafe(BigDecimal("4.99"))
          )
        ),
        Money.unsafe(BigDecimal("4.99")),
        PurchaseSource.Manual,
        CorrelationId("c-1")
      )
      val recorded = Purchase.replay(Purchase.decide(PurchaseState.Empty, cmd, now).toOption.get)
      Purchase.decide(recorded, cmd, now) shouldBe Right(Nil)

      // Which the entity reports as an ack, so the route must answer 202 and not a conflict.
      Post("/api/v1/purchases", body()) ~> new Spy(Right(())).route ~> check {
        status shouldBe StatusCodes.Accepted
      }
    }

    "report a DIFFERENT purchase under a taken id as a conflict, not a retry" in {
      // An id collision would otherwise overwrite one receipt with another.
      Post("/api/v1/purchases", body()) ~> new Spy(Left("already registered")).route ~> check {
        status shouldBe StatusCodes.Conflict
        responseAs[ErrorView].error shouldBe "already registered"
      }
    }

    "refuse a missing id rather than invent one" in {
      Post("/api/v1/purchases", body(id = "")) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("caller mints it")
      }
    }

    "refuse money that is not a decimal, before the aggregate sees a value it cannot build" in {
      Post("/api/v1/purchases", body(total = "4,99")) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("total is not a number")
      }
    }

    "refuse a non-positive quantity" in {
      val zero = List(PurchaseLineRequest("prod-1", "0", "4.99", "4.99"))
      Post("/api/v1/purchases", body(lines = zero)) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("quantity must be positive")
      }
    }

    "name the offending line, so a long receipt is debuggable" in {
      val lines = oneLine :+ PurchaseLineRequest("prod-2", "1", "nope", "2.00")
      Post("/api/v1/purchases", body(lines = lines)) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("lines[1].pricePaid")
      }
    }

    "refuse a receipt source that cannot name its scan" in {
      // The source is what tells a price fact's provenance apart (§2.4). A receipt that
      // cannot point at its scan is not recorded as one.
      Post("/api/v1/purchases", body(source = Some("receipt"))) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("receiptBlobId is required")
      }
    }

    "carry a receipt source when it CAN name its scan" in {
      val spy = new Spy
      val req = body(source = Some("receipt"), receiptBlobId = Some("blob-9"))
      Post("/api/v1/purchases", req) ~> spy.route ~> check {
        status shouldBe StatusCodes.Accepted
      }
      spy.recorded.map(_.source) shouldBe Some(PurchaseSource.Receipt("blob-9"))
    }

    "adopt the caller's correlation id when it offers one" in {
      val spy = new Spy
      Post("/api/v1/purchases", body())
        .withHeaders(RawHeader("X-Correlation-Id", "c-9")) ~> spy.route ~> check {
        status shouldBe StatusCodes.Accepted
      }
      spy.recorded.map(_.correlationId) shouldBe Some(CorrelationId("c-9"))
    }

    "mint a correlation id when the caller offers none, so the chain is never anonymous" in {
      val spy = new Spy
      Post("/api/v1/purchases", body()) ~> spy.route ~> check {
        status shouldBe StatusCodes.Accepted
      }
      spy.recorded.map(_.correlationId.value).exists(_.nonEmpty) shouldBe true
    }
  }

  "voiding a purchase" should {

    "pass the reason through" in {
      val spy = new Spy
      Post("/api/v1/purchases/p-1/void", VoidPurchaseRequest("wrong store")) ~>
        spy.route ~> check {
          status shouldBe StatusCodes.Accepted
          responseAs[AcceptedView].status shouldBe "voided"
        }
      spy.seen.map(_._1).toList shouldBe List(PurchaseId("p-1"))
      spy.seen.collectFirst { case (_, c: PurchaseCommand.VoidPurchase) => c.reason } shouldBe
        Some("wrong store")
    }

    "refuse a void with no reason, because the correction loses its why" in {
      Post("/api/v1/purchases/p-1/void", VoidPurchaseRequest("  ")) ~> new Spy().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorView].error should include("reason is required")
      }
    }

    "report voiding an already-voided purchase as a conflict" in {
      Post("/api/v1/purchases/p-1/void", VoidPurchaseRequest("dup")) ~>
        new Spy(Left("already voided")).route ~> check {
          status shouldBe StatusCodes.Conflict
        }
    }
  }
}

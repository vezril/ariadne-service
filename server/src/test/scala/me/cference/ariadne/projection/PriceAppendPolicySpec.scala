package me.cference.ariadne.projection

import me.cference.ariadne.domain.{
  Confidence,
  CorrelationId,
  Money,
  PriceScope,
  ProductId,
  PurchaseId,
  StoreId
}
import me.cference.ariadne.domain.price.{
  PriceCommand,
  PriceObservation,
  PriceSource,
  PriceStreamState
}
import me.cference.ariadne.domain.purchase.{PurchaseEvent, PurchaseLine, PurchaseSource}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * The price-append policy (§2.4): *whenever PurchaseRecorded then observe each line's price paid*.
 *
 * Driven directly rather than through a journal, which is the `ProjectionHandlers` pattern — what
 * the policy decides is the interesting part, and that R2dbcProjection delivers events is Pekko's
 * problem rather than this policy's.
 */
final class PriceAppendPolicySpec extends AnyWordSpec with Matchers with ScalaFutures {

  private val purchaseId = PurchaseId("pur-1")
  private val storeId = StoreId("store-1")
  private val at = Instant.parse("2026-11-10T14:30:00Z")
  private val cid = CorrelationId("c-7")

  private def line(product: String, paid: String, qty: Int = 1) =
    PurchaseLine(
      ProductId(product),
      BigDecimal(qty),
      Money.unsafe(BigDecimal(paid)),
      Money.unsafe(BigDecimal(paid) * qty)
    )

  private def recorded(lines: List[PurchaseLine]): PurchaseEvent.PurchaseRecorded =
    PurchaseEvent.PurchaseRecorded(
      purchaseId,
      storeId,
      at,
      lines,
      Money.sum(lines.map(_.lineTotal)).toOption.flatten.getOrElse(Money.unsafe(BigDecimal(1))),
      PurchaseSource.Manual,
      cid
    )

  /** Collects the commands the policy issues, in the order it issues them. */
  final private class Spy {
    val issued: mutable.ListBuffer[(ProductId, PriceScope, PriceCommand)] =
      mutable.ListBuffer.empty
    val observe: (ProductId, PriceScope, PriceCommand) => Future[Unit] =
      (p, s, c) => { issued += ((p, s, c)); Future.unit }
  }

  "the price-append policy" should {

    "observe one price per line, at the store the receipt names" in {
      val spy = new Spy
      val event = recorded(List(line("prod-1", "4.99"), line("prod-2", "2.50")))
      AriadneProjections.appendPrices(spy.observe)(event).futureValue

      spy.issued should have size 2
      // EXACT, never regional. A receipt is the one price fact that genuinely names an
      // individual franchise — which is precisely what §2.3.1 says a flyer cannot do, so
      // recording it as an area price would throw away the only thing it knows better.
      spy.issued.map(_._2).toList.distinct shouldBe List(PriceScope.Exact(storeId))
      spy.issued.map(_._1).toList shouldBe List(ProductId("prod-1"), ProductId("prod-2"))
    }

    "carry the price PAID, the purchase as source, and the purchase's own correlation id" in {
      val spy = new Spy
      AriadneProjections
        .appendPrices(spy.observe)(recorded(List(line("prod-1", "4.99"))))
        .futureValue

      val cmd = spy.issued
        .collectFirst { case (_, _, c: PriceCommand.ObservePrice) => c }
        .getOrElse(fail("expected an ObservePrice"))
      cmd.price shouldBe Money.unsafe(BigDecimal("4.99"))
      // Source carries the purchase id, which is what keeps a receipt distinguishable
      // from a scrape that happened to print the same number on the same day.
      cmd.source shouldBe PriceSource.Purchase(purchaseId)
      cmd.observedAt shouldBe at
      cmd.correlationId shouldBe cid
      // A till charged it, so the price is not in doubt; and the line names a ProductId
      // outright, so no size was parsed here for Demeter to inherit risk from (§2.3).
      cmd.priceConfidence shouldBe Confidence.Certain
      cmd.sizeConfidence shouldBe Confidence.Certain
      cmd.promo shouldBe None
    }

    "be replay-safe: the aggregate drops a reissued command as a no-op" in {
      // At-least-once delivery means a crash between applying and committing replays the
      // event, so the policy needs no dedup bookkeeping of its own — it is safe only if
      // the command it reissues is one `PriceObservation.decide` recognises. That is the
      // claim worth testing, and it belongs here rather than in core because it is the
      // POLICY's correctness that depends on it.
      val spy = new Spy
      val event = recorded(List(line("prod-1", "4.99")))
      AriadneProjections.appendPrices(spy.observe)(event).futureValue
      AriadneProjections.appendPrices(spy.observe)(event).futureValue

      val first :: second :: Nil =
        spy.issued.map(_._3).collect { case c: PriceCommand.ObservePrice => c }.toList: @unchecked
      first shouldBe second

      val now = Instant.parse("2026-11-11T00:00:00Z")
      val afterFirst =
        PriceObservation.replay(
          PriceObservation.decide(PriceStreamState.Empty, first, now).toOption.get
        )
      PriceObservation.decide(afterFirst, second, now) shouldBe Right(Nil)
    }

    "leave price history alone when a purchase is voided" in {
      // The price paid was still paid. Voiding corrects the PURCHASE record — a mistyped
      // total, the wrong store — and retracting history on that basis would delete a fact
      // Demeter may already have scored against. A genuinely wrong price is retracted
      // explicitly through RetractObservation.
      val spy = new Spy
      AriadneProjections
        .appendPrices(spy.observe)(PurchaseEvent.PurchaseVoided("wrong store", cid))
        .futureValue
      spy.issued shouldBe empty
    }

    "advance over a purchase with no lines rather than issuing nothing-shaped commands" in {
      val spy = new Spy
      AriadneProjections.appendPrices(spy.observe)(recorded(Nil)).futureValue
      spy.issued shouldBe empty
    }

    "append lines in order, so two lines for the same product cannot race the dedup check" in {
      // Both lines land on the SAME price entity. Issued concurrently, the second could
      // read a state that does not yet include the first and append a duplicate.
      val started = new java.util.concurrent.atomic.AtomicInteger(0)
      val gate = scala.concurrent.Promise[Unit]()
      val firstStarted = scala.concurrent.Promise[Unit]()
      val observe: (ProductId, PriceScope, PriceCommand) => Future[Unit] = (_, _, _) =>
        if started.incrementAndGet() == 1 then {
          firstStarted.success(())
          // Stay outstanding until the test releases it.
          gate.future
        } else Future.unit

      val event = recorded(List(line("prod-1", "4.99"), line("prod-1", "4.99")))
      val done = AriadneProjections.appendPrices(observe)(event)

      // Wait for the FIRST call rather than assuming it has happened — appendPrices begins
      // with a flatMap, so the first observe is itself asynchronous and asserting straight
      // away races the policy instead of testing it.
      firstStarted.future.futureValue
      // The second line has not been attempted while the first is still outstanding.
      started.get() shouldBe 1
      gate.success(())
      done.futureValue
      started.get() shouldBe 2
    }
  }
}

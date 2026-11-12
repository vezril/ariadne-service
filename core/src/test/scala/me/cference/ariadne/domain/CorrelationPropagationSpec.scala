package me.cference.ariadne.domain

import me.cference.ariadne.domain.price.{
  PriceCommand,
  PriceObservation,
  PriceSource,
  PriceStreamState
}
import me.cference.ariadne.domain.product.{Product, ProductCommand, ProductState}
import me.cference.ariadne.domain.purchase.*
import me.cference.ariadne.domain.resolution.*
import me.cference.ariadne.domain.store.{Store, StoreCommand, StoreState}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

/**
 * §8's promise, asserted rather than assumed: the correlation id of the command that caused an
 * event is ON that event.
 *
 * This is what makes a scrape → resolve → observe → deal-alert chain traceable across services.
 * Ariadne's publisher reads committed events long after the request is gone, so a correlation that
 * lived only on the command would leave the trace stopping at the write boundary — and the failure
 * is invisible: every message publishes fine, carrying nothing.
 *
 * Written per-aggregate on purpose. A single spot-check would pass while three aggregates quietly
 * dropped it, which is exactly the state this codebase was in before today.
 */
final class CorrelationPropagationSpec extends AnyWordSpec with Matchers {

  private val cid = CorrelationId("trace-me")
  private val now = Instant.parse("2026-09-07T12:00:00Z")

  private def only[E](r: Either[DomainError, List[E]]): E =
    r.fold(e => fail(s"expected events, got $e"), _.headOption.getOrElse(fail("no events")))

  "every aggregate" should {

    "put the command's correlation id on the product event it emits" in {
      val e = only(
        Product.decide(
          ProductState.Empty,
          ProductCommand.RegisterProduct(
            ProductId("p-1"),
            "Butter",
            None,
            None,
            None,
            None,
            Origin.Manual,
            cid
          )
        )
      )
      e.correlationId shouldBe cid
    }

    "put it on the store event" in {
      val e = only(
        Store.decide(
          StoreState.Empty,
          StoreCommand.RegisterStore(
            StoreId("s-1"),
            "IGA",
            ChainId("iga"),
            Area("H2X"),
            None,
            cid
          )
        )
      )
      e.correlationId shouldBe cid
    }

    "put it on the price event — the one Demeter's alerting rides" in {
      val e = only(
        PriceObservation.decide(
          PriceStreamState.Empty,
          PriceCommand.ObservePrice(
            ProductId("p-1"),
            PriceScope.Regional(ChainId("iga"), Area("H2X")),
            Money.unsafe(BigDecimal("4.99")),
            now,
            PriceSource.Scrape("flipp", 1L),
            None,
            None,
            Confidence.Certain,
            Confidence.Certain,
            cid
          ),
          now
        )
      )
      e.correlationId shouldBe cid
    }

    "put it on the purchase event" in {
      val line = PurchaseLine(
        ProductId("p-1"),
        BigDecimal(1),
        Money.unsafe(BigDecimal("4.99")),
        Money.unsafe(BigDecimal("4.99"))
      )
      val e = only(
        Purchase.decide(
          PurchaseState.Empty,
          PurchaseCommand.RecordPurchase(
            PurchaseId("pu-1"),
            StoreId("s-1"),
            now,
            List(line),
            Money.unsafe(BigDecimal("4.99")),
            PurchaseSource.Manual,
            cid
          ),
          now
        )
      )
      e.correlationId shouldBe cid
    }

    "put it on the resolution event — the link between a scrape and a human's decision" in {
      val e = only(
        ResolutionCase.decide(
          ResolutionState.Empty,
          ResolutionCommand.Propose(
            ResolutionId("r-1"),
            MatchSubject("Butter"),
            Nil,
            cid
          )
        )
      )
      e.correlationId shouldBe cid
    }

    "carry a DIFFERENT id per command, so the field is threaded and not a constant" in {
      // The failure this catches: a plausible implementation that stamps a fixed or
      // freshly-minted id would satisfy every assertion above and trace nothing.
      val other = CorrelationId("a-different-trace")
      val a = only(
        Store.decide(
          StoreState.Empty,
          StoreCommand.RegisterStore(StoreId("s-1"), "IGA", ChainId("iga"), Area("H2X"), None, cid)
        )
      )
      val b = only(
        Store.decide(
          StoreState.Empty,
          StoreCommand
            .RegisterStore(StoreId("s-1"), "IGA", ChainId("iga"), Area("H2X"), None, other)
        )
      )
      a.correlationId shouldBe cid
      b.correlationId shouldBe other
      a.correlationId should not be b.correlationId
    }
  }
}

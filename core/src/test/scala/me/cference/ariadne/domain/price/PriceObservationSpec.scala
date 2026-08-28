package me.cference.ariadne.domain
package price

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, ZoneId}

final class PriceObservationSpec extends AnyFunSuite with Matchers {

  private val zone = ZoneId.of("America/Toronto")
  private val pid = ProductId("p-1")
  private val sid = StoreId("s-1")
  private val exact = PriceScope.Exact(sid)
  private val flyer = PriceScope.Regional(ChainId("iga"), Area("H2X"))
  private val cid = CorrelationId("c-1")
  private val now = Instant.parse("2026-08-26T18:00:00Z")
  private val price = Money.unsafe(BigDecimal("4.99"))
  private val scrape = PriceSource.Scrape("flipp")

  private def observe(
      at: Instant,
      p: Money = price,
      source: PriceSource = scrape,
      sizeConf: Confidence = Confidence.Certain,
      scope: PriceScope = exact
  ) = PriceCommand.ObservePrice(
    pid,
    scope,
    p,
    at,
    source,
    None,
    None,
    Confidence.Certain,
    sizeConf,
    cid
  )

  private def streamWith(
      at: Instant,
      p: Money = price,
      source: PriceSource = scrape,
      scope: PriceScope = exact
  ): PriceStreamState =
    PriceObservation.replay(
      List(
        PriceEvent.PriceObserved(
          pid,
          scope,
          p,
          None,
          None,
          Confidence.Certain,
          Confidence.Certain,
          at,
          source
        )
      )
    )

  test("the first observation opens the stream") {
    PriceObservation.decide(PriceStreamState.Empty, observe(now), now, zone) match {
      case Right(List(e: PriceEvent.PriceObserved)) => e.price shouldBe price
      case other => fail(s"unexpected: $other")
    }
  }

  test("same price, same source, same calendar day is a no-op") {
    // Scrapes repeat within a day. Without this, history fills with duplicates
    // and any rolling statistic computed over it is skewed.
    val s = streamWith(Instant.parse("2026-08-26T09:00:00Z"))
    PriceObservation.decide(
      s,
      observe(Instant.parse("2026-08-26T15:00:00Z")),
      now,
      zone
    ) shouldBe Right(Nil)
  }

  test("a CHANGED price on the same day is recorded — dedup must not swallow real movement") {
    val s = streamWith(Instant.parse("2026-08-26T09:00:00Z"))
    val cheaper = Money.unsafe(BigDecimal("3.49"))
    PriceObservation.decide(
      s,
      observe(Instant.parse("2026-08-26T15:00:00Z"), p = cheaper),
      now,
      zone
    ) match {
      case Right(List(e: PriceEvent.PriceObserved)) => e.price shouldBe cheaper
      case other => fail(s"expected the price change to be recorded, got $other")
    }
  }

  test("the same price from a DIFFERENT source is recorded — a paid price is not a flyer claim") {
    // source=purchase is a better fact than source=scrape even at the same
    // number, so it must not be deduped away.
    val s = streamWith(Instant.parse("2026-08-26T09:00:00Z"))
    val paid = PriceSource.Purchase(PurchaseId("pu-1"))
    PriceObservation.decide(
      s,
      observe(Instant.parse("2026-08-26T15:00:00Z"), source = paid),
      now,
      zone
    ) match {
      case Right(List(e: PriceEvent.PriceObserved)) => e.source shouldBe paid
      case other => fail(s"unexpected: $other")
    }
  }

  test("the same price on the NEXT calendar day is a new fact") {
    val s = streamWith(Instant.parse("2026-08-25T15:00:00Z"))
    PriceObservation
      .decide(s, observe(Instant.parse("2026-08-26T15:00:00Z")), now, zone)
      .map(_.size) shouldBe Right(1)
  }

  test("an observation dated in the future is refused") {
    val future = now.plusSeconds(3600)
    PriceObservation.decide(PriceStreamState.Empty, observe(future), now, zone) shouldBe
      Left(DomainError.ObservationInFuture(future.toString))
  }

  test("sizeConfidence rides the event verbatim — it is contract, not decoration") {
    // §2.3: Demeter computes min(splitConfidence, sizeConfidence). If this field
    // were dropped or defaulted to 1.0, Demeter's confidence would read too high
    // and it would never know.
    val ambiguous = Confidence.unsafe(0.4)
    PriceObservation.decide(
      PriceStreamState.Empty,
      observe(now, sizeConf = ambiguous),
      now,
      zone
    ) match {
      case Right(List(e: PriceEvent.PriceObserved)) => e.sizeConfidence.toDouble shouldBe 0.4
      case other => fail(s"unexpected: $other")
    }
  }

  test("retracting on an empty stream is refused; on an open stream it is recorded") {
    PriceObservation.decide(
      PriceStreamState.Empty,
      PriceCommand.RetractObservation(now, "bad parse", cid),
      now,
      zone
    ) shouldBe
      Left(DomainError.NotRegistered)
    val s = streamWith(now)
    PriceObservation
      .decide(s, PriceCommand.RetractObservation(now, "bad parse", cid), now, zone)
      .map(_.size) shouldBe Right(1)
  }

  test("an out-of-order backfill does not drag the dedup anchor backwards") {
    // Backfill replays carry ORIGINAL timestamps, so older facts arrive after
    // newer ones. If an old event reset the anchor, dedup would start comparing
    // against the wrong day.
    val recent = Instant.parse("2026-08-26T12:00:00Z")
    val old = Instant.parse("2020-01-01T12:00:00Z")
    val s = PriceObservation.replay(
      List(
        PriceEvent.PriceObserved(
          pid,
          exact,
          price,
          None,
          None,
          Confidence.Certain,
          Confidence.Certain,
          recent,
          scrape
        ),
        PriceEvent.PriceObserved(
          pid,
          exact,
          price,
          None,
          None,
          Confidence.Certain,
          Confidence.Certain,
          old,
          PriceSource.Backfill("demeter")
        )
      )
    )
    s match {
      case PriceStreamState.Open(_, _, last, count) =>
        last.map(_.at) shouldBe Some(recent)
        count shouldBe 2L
      case PriceStreamState.Empty => fail("should be open")
    }
  }

  test("scope rides the event verbatim — a flyer fact stays a flyer fact") {
    // §2.3.1: the write side records what was actually observed. Turning one
    // regional observation into N per-store facts would fabricate N-1 of them.
    PriceObservation.decide(PriceStreamState.Empty, observe(now, scope = flyer), now, zone) match {
      case Right(List(e: PriceEvent.PriceObserved)) =>
        e.scope shouldBe flyer
        e.scope.isExact shouldBe false
      case other => fail(s"unexpected: $other")
    }
  }

  test("exact and regional observations are SEPARATE streams, so neither dedups the other") {
    // A receipt at store s-1 and a flyer covering s-1's chain+region are two
    // different facts about the same product. If they shared a stream, the same
    // price on the same day would silently swallow the receipt — the better fact.
    exact.key should not be flyer.key

    val fromFlyer = streamWith(Instant.parse("2026-08-26T09:00:00Z"), scope = flyer)
    // Same price, same day, same source, but an EXACT observation: not a duplicate,
    // because it is not a fact about the same scope.
    PriceObservation.decide(
      fromFlyer,
      observe(Instant.parse("2026-08-26T15:00:00Z"), scope = exact),
      now,
      zone
    ) match {
      case Right(List(e: PriceEvent.PriceObserved)) => e.scope shouldBe exact
      case other =>
        fail(s"an exact observation must not be deduped against a regional one, got $other")
    }
  }

  test("scope keys are stable, distinct, and identify the stream") {
    PriceScope.Exact(StoreId("s-1")).key shouldBe "store:s-1"
    PriceScope.Regional(ChainId("iga"), Area("H2X")).key shouldBe "area:iga:H2X"
    // Different chains in the same area, and the same chain in different areas,
    // must not collide — both are genuinely different price facts.
    PriceScope.Regional(ChainId("iga"), Area("H2X")).key should not be
      PriceScope.Regional(ChainId("metro"), Area("H2X")).key
    PriceScope.Regional(ChainId("iga"), Area("H2X")).key should not be
      PriceScope.Regional(ChainId("iga"), Area("H3B")).key
  }

  test("an exact observation is marked exact — the read side ranks on this") {
    exact.isExact shouldBe true
    flyer.isExact shouldBe false
  }
}

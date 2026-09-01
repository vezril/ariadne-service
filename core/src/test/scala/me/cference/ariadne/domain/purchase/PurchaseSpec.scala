package me.cference.ariadne.domain
package purchase

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

final class PurchaseSpec extends AnyFunSuite with Matchers {

  private val now = Instant.parse("2026-08-26T18:00:00Z")
  private val id = PurchaseId("pu-1")
  private val sid = StoreId("s-1")
  private val cid = CorrelationId("c-1")

  private def line(amount: String, qty: BigDecimal = BigDecimal(1)) =
    PurchaseLine(
      ProductId("p-1"),
      qty,
      Money.unsafe(BigDecimal(amount)),
      Money.unsafe(BigDecimal(amount))
    )

  private def record(lines: List[PurchaseLine], total: String, at: Instant = now.minusSeconds(60)) =
    PurchaseCommand.RecordPurchase(
      id,
      sid,
      at,
      lines,
      Money.unsafe(BigDecimal(total)),
      PurchaseSource.Manual,
      cid
    )

  test("a purchase whose lines sum to its stated total is recorded") {
    val cmd = record(List(line("4.99"), line("2.51")), "7.50")
    Purchase.decide(PurchaseState.Empty, cmd, now).map(_.size) shouldBe Right(1)
  }

  test("a stated total that disagrees with the lines is refused") {
    // A receipt whose parts do not sum to its whole is a parse failure. Recording
    // it would put a number into budgeting that no till ever printed.
    val cmd = record(List(line("4.99"), line("2.51")), "9.99")
    Purchase.decide(PurchaseState.Empty, cmd, now) match {
      case Left(e: DomainError.PurchaseTotalMismatch) => e.message should include("does not match")
      case other => fail(s"expected a mismatch refusal, got $other")
    }
  }

  test("an empty purchase is refused") {
    Purchase.decide(PurchaseState.Empty, record(Nil, "0.01"), now) shouldBe Left(
      DomainError.EmptyPurchase
    )
  }

  test("a purchase dated in the future is refused") {
    val future = now.plusSeconds(3600)
    Purchase.decide(
      PurchaseState.Empty,
      record(List(line("1.00")), "1.00", at = future),
      now
    ) shouldBe
      Left(DomainError.PurchaseInFuture(future.toString))
  }

  test("corrections are new facts: void is recorded, and both events persist") {
    val recorded = Purchase.replay(
      List(
        PurchaseEvent.PurchaseRecorded(
          id,
          sid,
          now,
          List(line("1.00")),
          Money.unsafe(BigDecimal("1.00")),
          PurchaseSource.Manual
        )
      )
    )
    Purchase
      .decide(recorded, PurchaseCommand.VoidPurchase("wrong store", cid), now)
      .map(_.size) shouldBe Right(1)

    val voided = Purchase.evolve(recorded, PurchaseEvent.PurchaseVoided("wrong store"))
    voided match {
      case s: PurchaseState.Recorded =>
        s.voided shouldBe true
        // The lines are still there — voiding does not erase the audit trail.
        s.lines should have size 1
      case PurchaseState.Empty => fail("should be recorded")
    }
    Purchase.decide(voided, PurchaseCommand.VoidPurchase("again", cid), now) shouldBe Left(
      DomainError.AlreadyVoided
    )
  }

  test("re-recording the SAME purchase is a no-op success — the outbox must be able to drain") {
    // Ariadne being unreachable cannot block someone typing a receipt, so the caller
    // buffers locally and retries. If a retry of an already-recorded purchase errored,
    // the client could not tell it from a genuine failure and the row would retry
    // forever. A dropped receipt is lost data that the flyer feed can never reconstruct.
    val cmd = record(List(line("1.00")), "1.00", at = now)
    val recorded = Purchase
      .decide(PurchaseState.Empty, cmd, now)
      .map(Purchase.replay)
      .getOrElse(fail("should record"))
    Purchase.decide(recorded, cmd, now) shouldBe Right(Nil)
  }

  test(
    "a DIFFERENT purchase under a taken id is still refused — that is a collision, not a retry"
  ) {
    // Accepting this silently would overwrite one receipt with another.
    val first = record(List(line("1.00")), "1.00", at = now)
    val recorded = Purchase
      .decide(PurchaseState.Empty, first, now)
      .map(Purchase.replay)
      .getOrElse(fail("should record"))
    val different = record(List(line("9.99")), "9.99", at = now)
    Purchase.decide(recorded, different, now) shouldBe Left(DomainError.AlreadyRegistered)
  }

  test("recording twice into the same stream is refused") {
    val recorded = Purchase.replay(
      List(
        PurchaseEvent.PurchaseRecorded(
          id,
          sid,
          now,
          List(line("1.00")),
          Money.unsafe(BigDecimal("1.00")),
          PurchaseSource.Manual
        )
      )
    )
    Purchase.decide(recorded, record(List(line("1.00")), "1.00"), now) shouldBe Left(
      DomainError.AlreadyRegistered
    )
  }
}

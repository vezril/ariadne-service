package me.cference.ariadne.domain
package resolution

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

final class ResolutionCaseSpec extends AnyFunSuite with Matchers {

  private val cid = CorrelationId("c-1")
  private val rid = ResolutionId("r-1")
  private val candidateA = ScoredCandidate(ProductId("p-a"), Confidence.unsafe(0.81), Nil)
  private val candidateB = ScoredCandidate(ProductId("p-b"), Confidence.unsafe(0.74), Nil)
  private val subject = MatchSubject("Lactantia Butter 454 g", Some("Lactantia"))

  private def observation(amount: String) = ParkedObservation(
    Money.unsafe(BigDecimal(amount)),
    Instant.parse("2026-08-26T12:00:00Z"),
    PriceScope.Regional(ChainId("iga"), Area("H2X")),
    Confidence.Certain,
    Confidence.Certain
  )

  private def pending(parked: List[ParkedObservation] = Nil): ResolutionState =
    ResolutionCase.replay(
      ResolutionEvent.ResolutionProposed(rid, subject, List(candidateA, candidateB)) ::
        parked.map(ResolutionEvent.ObservationParked.apply)
    )

  test("a proposal opens a pending case carrying its candidates") {
    ResolutionCase
      .decide(
        ResolutionState.Empty,
        ResolutionCommand.Propose(rid, subject, List(candidateA, candidateB), cid)
      )
      .map(ResolutionCase.replay) match {
      case Right(s: ResolutionState.Pending) => s.candidates should have size 2
      case other => fail(s"unexpected: $other")
    }
  }

  test("observations PARK rather than being attributed to a guess") {
    // The reason parking exists: an ambiguous match must not record price facts
    // against an identity nobody has confirmed. Dropping them instead would lose a
    // real market fact, so they are held.
    val s = pending(List(observation("4.99"), observation("5.49")))
    s match {
      case p: ResolutionState.Pending => p.parked should have size 2
      case other => fail(s"unexpected: $other")
    }
  }

  test("confirming releases every parked observation to the chosen product") {
    val s = pending(List(observation("4.99")))
    ResolutionCase.decide(s, ResolutionCommand.Confirm(ProductId("p-a"), cid)) match {
      case Right(List(e: ResolutionEvent.ResolutionConfirmed)) =>
        e.productId shouldBe ProductId("p-a")
        e.released should have size 1
      case other => fail(s"unexpected: $other")
    }
  }

  test("confirming something that was never offered is REFUSED") {
    // "Confirm" means "this one, of the ones offered". Picking anything else is a
    // different decision, and Reject is the verb for it.
    val s = pending()
    ResolutionCase.decide(s, ResolutionCommand.Confirm(ProductId("p-zzz"), cid)) match {
      case Left(e: DomainError.NotACandidate) => e.message should include("Reject")
      case other => fail(s"expected a refusal, got $other")
    }
  }

  test("rejecting creates a new product and still releases the parked observations") {
    // The facts were real even though none of the candidates were right; they belong
    // to the newly created product, not the bin.
    val s = pending(List(observation("4.99"), observation("3.99")))
    ResolutionCase.decide(s, ResolutionCommand.Reject(ProductId("p-new"), cid)) match {
      case Right(List(e: ResolutionEvent.ResolutionRejected)) =>
        e.newProductId shouldBe ProductId("p-new")
        e.released should have size 2
      case other => fail(s"unexpected: $other")
    }
  }

  test("a merge request records winner and loser, and refuses a self-merge") {
    val s = pending()
    // Fold the new events onto the EXISTING state — replaying them from Empty would
    // just yield Empty, since only a proposal opens a case.
    ResolutionCase
      .decide(s, ResolutionCommand.RequestMerge(ProductId("p-a"), ProductId("p-b"), cid))
      .map(_.foldLeft(s)(ResolutionCase.evolve)) match {
      case Right(r: ResolutionState.Resolved) =>
        r.outcome shouldBe ResolutionOutcome.MergedProducts(ProductId("p-a"), ProductId("p-b"))
      case other => fail(s"unexpected: $other")
    }
    ResolutionCase.decide(
      s,
      ResolutionCommand.RequestMerge(ProductId("p-a"), ProductId("p-a"), cid)
    ) shouldBe
      Left(DomainError.CannotMergeIntoSelf)
  }

  test("a split records the listing being pulled off onto a new product") {
    val listing = ListingKey(StoreId("s-1"), "ext-9")
    val s = pending()
    ResolutionCase
      .decide(s, ResolutionCommand.RequestSplit(listing, ProductId("p-new"), cid))
      .map(_.foldLeft(s)(ResolutionCase.evolve)) match {
      case Right(r: ResolutionState.Resolved) =>
        r.outcome shouldBe ResolutionOutcome.SplitOff(listing, ProductId("p-new"))
      case other => fail(s"unexpected: $other")
    }
  }

  test("a DECIDED case refuses everything — a change of mind is a new case") {
    // Re-deciding would re-release the parked observations and double-record them.
    // Same stance as voiding a purchase rather than editing it.
    val decided = ResolutionCase.evolve(
      pending(List(observation("4.99"))),
      ResolutionEvent.ResolutionConfirmed(ProductId("p-a"), List(observation("4.99")))
    )
    ResolutionCase.decide(decided, ResolutionCommand.Confirm(ProductId("p-b"), cid)) shouldBe
      Left(DomainError.AlreadyResolved)
    ResolutionCase.decide(
      decided,
      ResolutionCommand.ParkObservation(observation("1.00"), cid)
    ) shouldBe
      Left(DomainError.AlreadyResolved)
  }

  test("commands before the case exists are refused; proposing twice is refused") {
    ResolutionCase.decide(
      ResolutionState.Empty,
      ResolutionCommand.Confirm(ProductId("p-a"), cid)
    ) shouldBe
      Left(DomainError.NotRegistered)
    ResolutionCase.decide(pending(), ResolutionCommand.Propose(rid, subject, Nil, cid)) shouldBe
      Left(DomainError.AlreadyRegistered)
  }

  test("a blank subject is refused") {
    ResolutionCase.decide(
      ResolutionState.Empty,
      ResolutionCommand.Propose(rid, MatchSubject("   "), Nil, cid)
    ) shouldBe Left(DomainError.EmptyName)
  }

  test("replay reproduces state exactly") {
    val events = List(
      ResolutionEvent.ResolutionProposed(rid, subject, List(candidateA)),
      ResolutionEvent.ObservationParked(observation("4.99")),
      ResolutionEvent.ResolutionConfirmed(ProductId("p-a"), List(observation("4.99")))
    )
    ResolutionCase.replay(events) shouldBe
      events.foldLeft[ResolutionState](ResolutionState.Empty)(ResolutionCase.evolve)
  }
}

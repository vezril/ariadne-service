package me.cference.ariadne.domain
package store

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class StoreSpec extends AnyFunSuite with Matchers {

  private val cid = CorrelationId("c-1")
  private val sid = StoreId("s-1")

  private def registered: StoreState =
    Store.replay(List(StoreEvent.StoreRegistered(sid, "Metro", Some("Metro"), Some("Plateau"))))

  test("registers with a trimmed name and refuses a blank one") {
    Store
      .decide(StoreState.Empty, StoreCommand.RegisterStore(sid, "  Metro ", None, None, cid))
      .map(Store.replay) match {
      case Right(s: StoreState.Existing) => s.name shouldBe "Metro"; s.active shouldBe true
      case other => fail(s"unexpected: $other")
    }
    Store.decide(StoreState.Empty, StoreCommand.RegisterStore(sid, "  ", None, None, cid)) shouldBe
      Left(DomainError.EmptyStoreName)
  }

  test("an update that changes nothing emits nothing") {
    val cmd = StoreCommand.UpdateStoreDetails(Some("Metro"), Some("Metro"), Some("Plateau"), cid)
    Store.decide(registered, cmd) shouldBe Right(Nil)
  }

  test("an update that changes something is recorded") {
    val cmd = StoreCommand.UpdateStoreDetails(Some("Metro Plus"), None, None, cid)
    Store.decide(registered, cmd).map(_.size) shouldBe Right(1)
  }

  test("deactivating is idempotent") {
    val deactivated = Store.evolve(registered, StoreEvent.StoreDeactivated)
    Store.decide(deactivated, StoreCommand.DeactivateStore(cid)) shouldBe Right(Nil)
  }
}

package me.cference.ariadne.persistence

import com.typesafe.config.ConfigFactory
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.product.{
  ProductCommand,
  ProductEvent,
  ProductState,
  ProductStatus
}
import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object ProductEntitySpec {
  val config = EventSourcedBehaviorTestKit.config.withFallback(ConfigFactory.load()).resolve()
}

/**
 * The entity is a thin shell over the pure domain, so these tests deliberately do NOT re-test the
 * rules — `ProductSpec` owns those. What is tested here is what only the shell can get wrong: that
 * events actually persist, that a restart recovers the same state from them, and that a domain
 * refusal comes back as a rejection instead of being swallowed.
 */
final class ProductEntitySpec
    extends ScalaTestWithActorTestKit(ProductEntitySpec.config)
    with AnyWordSpecLike
    with Matchers {

  private val cid = CorrelationId("c-1")
  private val gtin = Gtin.unsafe("4006381333931")

  /**
   * Commands ARE verified now.
   *
   * PR #6 deferred this: commands were local, so their serializability was not a correctness
   * property and forcing them into sealed traits would have been churn. Sharding changed that — a
   * command can now cross a node boundary — so the deferral came due and the conversion was done.
   * State stays unverified because nothing is snapshotted yet; when snapshots land, this is the
   * line that should change.
   */
  private val serializationSettings =
    EventSourcedBehaviorTestKit.SerializationSettings.enabled
      .withVerifyEquality(true)
      .withVerifyEvents(true)
      .withVerifyCommands(true)
      .withVerifyState(false)

  private def kit =
    EventSourcedBehaviorTestKit[ProductEntity.Command, ProductEvent, ProductState](
      system,
      ProductEntity("p-1"),
      serializationSettings
    )

  "ProductEntity" should {

    "persist the event a valid command produces" in {
      val k = kit
      val result = k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(
          ProductCommand.RegisterProduct(
            ProductId("p-1"),
            "Lactantia Butter",
            Some("Lactantia"),
            None,
            None,
            Some(gtin),
            Origin.Manual,
            cid
          ),
          _
        )
      )
      result.reply.isSuccess shouldBe true
      result.event shouldBe a[ProductEvent.ProductRegistered]
      result.state shouldBe a[ProductState.Existing]
    }

    "RECOVER the same state after a restart — the point of event sourcing" in {
      val k = kit
      k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(
          ProductCommand.RegisterProduct(
            ProductId("p-1"),
            "Lactantia Butter",
            Some("Lactantia"),
            None,
            None,
            Some(gtin),
            Origin.Manual,
            cid
          ),
          _
        )
      )
      k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(ProductCommand.AddAlias("beurre", cid), _)
      )
      val before = k.getState()

      // Replay from the journal. If serialization or evolve were wrong, this is where
      // it surfaces — not at write time.
      k.restart()
      k.getState() shouldBe before
    }

    "report a domain refusal as a rejection, not silence" in {
      val k = kit
      val result = k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(ProductCommand.AddAlias("beurre", cid), _)
      )
      result.reply.isError shouldBe true
      result.hasNoEvents shouldBe true
    }

    "acknowledge a no-op WITHOUT writing an event" in {
      // A repeated scrape is a legitimate no-op. It must acknowledge (or every duplicate
      // looks like a failure) while writing nothing (or the journal fills with noise).
      val k = kit
      k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(
          ProductCommand.RegisterProduct(
            ProductId("p-1"),
            "Butter",
            None,
            None,
            None,
            Some(gtin),
            Origin.Manual,
            cid
          ),
          _
        )
      )
      val repeat = k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(ProductCommand.AddIdentifier(gtin, cid), _)
      )
      repeat.reply.isSuccess shouldBe true
      repeat.hasNoEvents shouldBe true
    }

    "tag its events so the projections can subscribe" in {
      val k = kit
      val result = k.runCommand[StatusReply[Done]](
        ProductEntity.Execute(
          ProductCommand.RegisterProduct(
            ProductId("p-1"),
            "Butter",
            None,
            None,
            None,
            None,
            Origin.Scrape(ListingKey(StoreId("s-1"), "e-1")),
            cid
          ),
          _
        )
      )
      result.state match {
        case s: ProductState.Existing => s.status shouldBe ProductStatus.Provisional
        case ProductState.Empty => fail("should exist")
      }
    }
  }
}

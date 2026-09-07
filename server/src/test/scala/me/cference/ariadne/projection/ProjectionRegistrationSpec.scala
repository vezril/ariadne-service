package me.cference.ariadne.projection

import me.cference.ariadne.hermes.{CatalogMessages, CatalogPublisher}
import org.apache.pekko.projection.Projection
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The gate for the bug that shipped in v0.1.0.
 *
 * `resolutionProjection` was defined and never started. Cases were journaled correctly, nothing
 * projected them, and `GET /api/v1/resolutions` answered empty forever — ariadne-ui's whole reason
 * to exist, silently inert. Every test passed, because they all call the HANDLERS directly. The
 * handler had coverage; the REGISTRATION had none.
 *
 * So this asserts registration, and asserts it two ways on purpose:
 *
 *   1. The names, so dropping or renaming one is a deliberate act rather than a silent edit.
 *   1. The COUNT against reflection over the object's own methods, so adding a fifth projection and
 *      forgetting to list it fails here. This is the half that cannot fail open: a new projection
 *      necessarily adds a method returning `Projection`, whatever it is called and whatever the
 *      code around it looks like.
 *
 * The second assertion is the same discipline as the OpenAPI drift gate — check the real artifact
 * at runtime rather than trusting that someone remembered. That gate covered routes and this one
 * did not exist, which is exactly where the bug got through.
 */
final class ProjectionRegistrationSpec extends AnyWordSpec with Matchers {

  /** Every method on the object that builds a projection, found by return type, not by name. */
  private val projectionMethods: List[String] =
    classOf[AriadneProjections.type].getDeclaredMethods.toList
      .filter(m => classOf[Projection[?]].isAssignableFrom(m.getReturnType))
      .map(_.getName)
      .distinct

  // `definitions` needs an ActorSystem to BUILD a projection, but not to be listed:
  // the behaviour is a lambda, so the names are readable without starting anything.
  private given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic

  /** Nothing is published; the stub exists only so the Hermes projections are DEFINED. */
  private val stubPublisher = new CatalogPublisher {
    def publish(msg: CatalogMessages.Outgoing): scala.concurrent.Future[Unit] =
      scala.concurrent.Future.unit
  }

  private def names(hermes: Boolean): List[String] =
    AriadneProjections
      .definitions(
        null,
        Option.when(hermes)(stubPublisher -> CatalogMessages.Topics())
      )(using null, summon[scala.concurrent.ExecutionContext])
      .map(_.name)

  private val registered: List[String] = names(hermes = true)

  "the projection registry" should {

    "start exactly the projections this service defines" in {
      // `hermesProjection` is ONE generic method serving three projections, so the
      // count is methods + 2. Stated as arithmetic rather than a magic number, since
      // the point is that every method is represented, not that the number is 7.
      withClue(
        s"defined: $projectionMethods\nregistered: $registered\n" +
          "A projection defined but not listed in `definitions` never starts — it does not " +
          "fail, it silently does nothing, which is how the review queue shipped empty.\n"
      ) {
        registered.size shouldBe (projectionMethods.size + 2)
      }
    }

    "not start a publisher when no broker is configured" in {
      // Read models are what the service IS; publishing is what it tells other
      // services. A service with no broker must still project, so the absence is a
      // configuration outcome rather than a degraded one.
      names(hermes = false).filter(_.startsWith("hermes-")) shouldBe empty
      names(hermes = false) should contain("price-history")
    }

    "publish from a SEPARATE offset than the read models" in {
      // The operational reason these are distinct projections at all: sharing an
      // offset with a read model would make a Hermes outage stall the REST surface,
      // so the catalogue would go stale because a broker was unreachable.
      val readModels = Set("product-catalog", "store-coverage", "price-history", "review-queue")
      val publishers = registered.filter(_.startsWith("hermes-")).toSet
      publishers should have size 3
      (publishers & readModels) shouldBe empty
    }

    "register the review queue — the one that was missing" in {
      registered should contain("review-queue")
    }

    "register every read model the REST surface reads from" in {
      registered should contain allOf (
        "product-catalog",
        "store-coverage",
        "price-history",
        "review-queue"
      )
    }

    "name each projection exactly once, or two daemons fight over one offset" in {
      registered.distinct.size shouldBe registered.size
    }

    "find the methods it reflects over, so a failed lookup cannot pass as agreement" in {
      // Without this, reflection finding NOTHING would make the count assertion pass
      // against an empty registry — the fail-open shape this whole spec exists to avoid.
      projectionMethods.size should be >= 4
    }
  }
}

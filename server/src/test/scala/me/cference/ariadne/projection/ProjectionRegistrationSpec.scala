package me.cference.ariadne.projection

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
  private val registered: List[String] =
    AriadneProjections
      .definitions(null)(using null, scala.concurrent.ExecutionContext.parasitic)
      .map(_.name)

  "the projection registry" should {

    "start exactly the projections this service defines" in {
      withClue(
        s"defined: $projectionMethods\nregistered: $registered\n" +
          "A projection defined but not listed in `definitions` never starts — it does not " +
          "fail, it silently does nothing, which is how the review queue shipped empty.\n"
      ) {
        registered.size shouldBe projectionMethods.size
      }
    }

    "register the review queue — the one that was missing" in {
      registered should contain("review-queue")
    }

    "register every read model the REST surface reads from" in {
      registered should contain theSameElementsAs
        List("product-catalog", "store-coverage", "price-history", "review-queue")
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

package me.cference.ariadne.resolver

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.projection.{PostgresFixture, ProjectionHandlers, ReadModelRepository}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Hot spot #1 against a real catalogue.
 *
 * The catalogue below is deliberately adversarial: several butters differing only by brand or pack
 * size, which is exactly the shape that produces false links, and the shape §6.7 says is NOT the
 * same product.
 */
final class ResolverSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with PostgresFixture {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Millis))

  private lazy val repo = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new ReadModelRepository(new PostgresqlConnectionFactory(cfg))
  }
  private lazy val resolver = new Resolver(repo)

  override def beforeAll(): Unit = {
    applySchema()
    seed()
  }
  override def afterAll(): Unit = container.stop()

  private def register(
      id: String,
      name: String,
      brand: Option[String],
      size: Option[Quantity],
      gtin: Option[Gtin]
  ): Unit =
    ProjectionHandlers
      .product(repo)(
        s"product|$id",
        ProductEvent.ProductRegistered(
          ProductId(id),
          name,
          brand,
          Some("dairy"),
          size,
          gtin,
          Origin.Manual,
          ProductStatus.Active
        )
      )
      .futureValue

  private def g(amount: Int) = Some(Quantity.unsafe(BigDecimal(amount), MeasureUnit.Gram))

  private def seed(): Unit = {
    register(
      "lac-454",
      "Lactantia Salted Butter",
      Some("Lactantia"),
      g(454),
      Some(Gtin.unsafe("4006381333931"))
    )
    register("lac-250", "Lactantia Salted Butter", Some("Lactantia"), g(250), None)
    register("gay-454", "Gay Lea Salted Butter", Some("Gay Lea"), g(454), None)
    register("salmon", "Fresh Atlantic Salmon Fillet", None, g(340), None)
  }

  "the strong keys" should {

    "match on GTIN with full confidence, without fuzzing at all" in {
      val out = resolver
        .resolve(
          MatchSubject("something entirely different", gtin = Some(Gtin.unsafe("4006381333931")))
        )
        .futureValue
      out shouldBe ResolutionOutcome.Matched(
        ProductId("lac-454"),
        Confidence.Certain,
        MatchMethod.Gtin
      )
    }

    "short-circuit on a known listing key, which is what makes steady state cheap" in {
      // Fuzzy matching should run once per NEW listing, not once per observation (§6.2).
      repo.linkListing("s-1", "ext-1", "gay-454", 0.95, "Fuzzy", "v1").futureValue
      val out = resolver
        .resolve(
          MatchSubject("noise", listing = Some(ListingKey(StoreId("s-1"), "ext-1")))
        )
        .futureValue
      out shouldBe ResolutionOutcome.Matched(
        ProductId("gay-454"),
        Confidence.Certain,
        MatchMethod.Listing
      )
    }

    "answer with the CANONICAL id when the strong key points at a tombstone" in {
      // Callers hold ids we do not control; a merged id must still resolve, forever.
      register(
        "dup-1",
        "Duplicate Butter",
        Some("Lactantia"),
        g(454),
        Some(Gtin.unsafe("96385074"))
      )
      ProjectionHandlers
        .product(repo)("product|dup-1", ProductEvent.ProductMerged(ProductId("lac-454")))
        .futureValue

      val out =
        resolver.resolve(MatchSubject("x", gtin = Some(Gtin.unsafe("96385074")))).futureValue
      out shouldBe ResolutionOutcome.Matched(
        ProductId("lac-454"),
        Confidence.Certain,
        MatchMethod.Gtin
      )
    }
  }

  "the fuzzy fallback" should {

    "auto-link a clear match" in {
      val out = resolver
        .resolve(MatchSubject("Lactantia Butter, Salted 454 g", Some("Lactantia")))
        .futureValue
      out match {
        case ResolutionOutcome.Matched(id, c, m) =>
          id shouldBe ProductId("lac-454")
          m shouldBe MatchMethod.Fuzzy
          c.toDouble should be >= 0.92
        case other => fail(s"expected an auto-link, got $other")
      }
    }

    "NOT confuse two pack sizes of the same product (§6.7)" in {
      // The 250 g exists in the catalogue and shares brand and every name token with
      // the 454 g. Only the size separates them, and size is identity.
      val out = resolver
        .resolve(MatchSubject("Lactantia Salted Butter 250 g", Some("Lactantia")))
        .futureValue
      out match {
        case ResolutionOutcome.Matched(id, _, _) => id shouldBe ProductId("lac-250")
        case other => fail(s"expected the 250 g, got $other")
      }
    }

    "NOT auto-link across brands" in {
      // Same size, same words, different banner on the wrapper.
      val out =
        resolver.resolve(MatchSubject("Gay Lea Salted Butter 454 g", Some("Gay Lea"))).futureValue
      out match {
        case ResolutionOutcome.Matched(id, _, _) => id shouldBe ProductId("gay-454")
        case other => fail(s"expected the Gay Lea product, got $other")
      }
    }

    "never offer a merged tombstone as a candidate" in {
      // dup-1 was merged above. Matching onto it would re-link a listing to a product
      // that is no longer a distinct thing.
      val out = resolver.resolve(MatchSubject("Duplicate Butter", Some("Lactantia"))).futureValue
      out match {
        case ResolutionOutcome.Matched(id, _, _) => id should not be ProductId("dup-1")
        case ResolutionOutcome.Ambiguous(cs) =>
          cs.map(_.productId) should not contain ProductId("dup-1")
        case ResolutionOutcome.NoMatch => succeed
      }
    }

    "return NoMatch for something the catalogue has never seen" in {
      resolver.resolve(MatchSubject("Zorblex Industrial Lubricant 5 L")).futureValue shouldBe
        ResolutionOutcome.NoMatch
    }

    "offer only candidates that actually reach the review floor" in {
      // Padding the picker with near-zero scores would make review look like a lottery.
      val out = resolver.resolve(MatchSubject("Butter", Some("Unknown Brand"))).futureValue
      out match {
        case ResolutionOutcome.Ambiguous(cs) => all(cs.map(_.score.toDouble)) should be >= 0.60
        case _ => succeed // a decisive answer is fine too
      }
    }
  }
}

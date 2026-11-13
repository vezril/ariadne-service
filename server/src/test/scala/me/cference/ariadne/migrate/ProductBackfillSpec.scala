package me.cference.ariadne.migrate

import me.cference.ariadne.domain.{Confidence, CorrelationId, MatchMethod, Origin, ProductId}
import me.cference.ariadne.domain.product.ProductCommand
import me.cference.ariadne.domain.resolution.{MatchSubject, ScoredCandidate}
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}
import org.scalatest.LoneElement
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * The product half of the Demeter backfill (`migration-demeter.md` step 2).
 *
 * The behaviour worth pinning is what happens on AMBIGUITY, because that is where a backfill can do
 * irreversible damage quietly: picking a candidate would attribute years of one product's price
 * history to another, and nothing downstream would look wrong.
 */
final class ProductBackfillSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LoneElement {

  private val at = Instant.parse("2026-11-13T12:00:00Z")

  private def row(key: String = "dk-1", name: String = "LACTANTIA BUTTER 454G") =
    DemeterCsv.ProductRow(
      productKey = key,
      rawName = name,
      displayNameEn = Some("Lactantia Butter"),
      displayNameFr = None,
      brand = Some("Lactantia"),
      sizeQty = Some(BigDecimal(454)),
      sizeUnit = Some("g"),
      gtin = None
    )

  final private class Harness(
      outcome: MatchSubject => ResolutionOutcome,
      registerAnswer: Either[String, Unit] = Right(())
  ) {
    val mappings: mutable.ListBuffer[ProductBackfill.Mapping] = mutable.ListBuffer.empty
    val registered: mutable.ListBuffer[(ProductId, ProductCommand)] = mutable.ListBuffer.empty
    val resolvedWith: mutable.ListBuffer[(MatchSubject, CorrelationId)] = mutable.ListBuffer.empty

    val backfill = new ProductBackfill(
      resolve = (s, c) => { resolvedWith += ((s, c)); Future.successful(outcome(s)) },
      register = (id, cmd) => { registered += ((id, cmd)); Future.successful(registerAnswer) },
      record = m => { mappings += m; Future.unit }
    )
  }

  "a matched product" should {
    "map to the existing id, with the resolver's own confidence and method recorded" in {
      val h = new Harness(_ =>
        ResolutionOutcome.Matched(ProductId("prod-9"), Confidence.unsafe(0.96), MatchMethod.Gtin)
      )
      val report = h.backfill.run(List(row()), now = () => at).futureValue
      report.matched shouldBe 1
      val m = h.mappings.loneElement
      m.productId shouldBe Some(ProductId("prod-9"))
      m.outcome shouldBe "matched"
      // Recorded so the cutover can audit WHY a key maps where it does rather than
      // trusting the mapping blindly.
      m.confidence shouldBe Some(0.96)
      m.matchMethod shouldBe Some("Gtin")
      m.resolvedAt shouldBe at
      h.registered shouldBe empty
    }
  }

  "an ambiguous product" should {

    "get NO product id — resolution never picks" in {
      // The damage this prevents is silent and permanent: picking would attribute one
      // product's entire price history to another, and nothing downstream would look wrong.
      val h = new Harness(_ =>
        ResolutionOutcome.Ambiguous(
          List(
            ScoredCandidate(ProductId("a"), Confidence.unsafe(0.8), List("name")),
            ScoredCandidate(ProductId("b"), Confidence.unsafe(0.79), List("brand"))
          )
        )
      )
      val report = h.backfill.run(List(row()), now = () => at).futureValue
      report.ambiguous shouldBe 1
      val m = h.mappings.loneElement
      m.productId shouldBe None
      m.outcome shouldBe "ambiguous"
      h.registered shouldBe empty
    }

    "record the review case id, so the parked question is reachable from the mapping" in {
      val h = new Harness(_ => ResolutionOutcome.Ambiguous(Nil))
      h.backfill.run(List(row()), now = () => at).futureValue
      // Derived from the subject, so it is the SAME case a later scrape of this product
      // would open rather than a parallel duplicate (§6.5).
      h.mappings.loneElement.resolutionCaseId shouldBe
        Some(
          ResolutionService
            .caseIdFor(
              MatchSubject(
                name = "LACTANTIA BUTTER 454G",
                brand = Some("Lactantia"),
                gtin = None,
                listing = None
              )
            )
            .value
        )
    }
  }

  "an unmatched product" should {

    "be registered with Origin.Migration, not Origin.Scrape" in {
      // These arrive from Demeter's store rather than a flyer we fetched, and the origin is
      // the only record of that distinction.
      val h = new Harness(_ => ResolutionOutcome.NoMatch)
      val report = h.backfill.run(List(row()), now = () => at).futureValue
      report.registered shouldBe 1
      val (id, cmd) = h.registered.loneElement
      cmd match {
        case c: ProductCommand.RegisterProduct =>
          c.origin shouldBe Origin.Migration("demeter")
          c.name shouldBe "LACTANTIA BUTTER 454G"
          c.brand shouldBe Some("Lactantia")
          c.size.map(_.unit.label) shouldBe Some("g")
          c.id shouldBe id
        case other => fail(s"expected RegisterProduct, got $other")
      }
      h.mappings.loneElement.productId shouldBe Some(id)
    }

    "use a DERIVED id, so a restarted run re-finds rather than re-creates" in {
      // An 18.7k-row migration will be restarted. A random id would make every restart a
      // fresh product — the catalog-of-duplicates outcome the resolver exists to prevent.
      val h1 = new Harness(_ => ResolutionOutcome.NoMatch)
      val h2 = new Harness(_ => ResolutionOutcome.NoMatch)
      h1.backfill.run(List(row()), now = () => at).futureValue
      h2.backfill.run(List(row()), now = () => at).futureValue
      h1.registered.loneElement._1 shouldBe h2.registered.loneElement._1
    }

    "treat a refused registration as already-present and still map the key" in {
      // On a re-run the aggregate refuses the second register, which is the SUCCESS case:
      // the id is derived, so it is the same product. The key resolved to that id whether or
      // not this particular run created it.
      val h =
        new Harness(_ => ResolutionOutcome.NoMatch, registerAnswer = Left("already registered"))
      val report = h.backfill.run(List(row()), now = () => at).futureValue
      report.registered shouldBe 0
      report.alreadyPresent shouldBe 1
      h.mappings.loneElement.productId shouldBe defined
    }

    "drop a size it cannot understand rather than guess the unit" in {
      // Size is part of identity (§6.7), so mapping "oz" onto Gram would make a 12 oz and a
      // 12 g the same product to the matcher.
      val h = new Harness(_ => ResolutionOutcome.NoMatch)
      h.backfill.run(List(row().copy(sizeUnit = Some("oz"))), now = () => at).futureValue
      h.registered.loneElement._2.asInstanceOf[ProductCommand.RegisterProduct].size shouldBe None
    }
  }

  "the resolver subject" should {
    "be built from raw_name, not the display name" in {
      // raw_name is what the matcher was tuned against. Demeter's display names are already
      // normalised by THEIR normaliser, and a twice-normalised string scores differently
      // than the same product will when it next arrives from a flyer.
      val h = new Harness(_ => ResolutionOutcome.NoMatch)
      h.backfill.run(List(row()), now = () => at).futureValue
      h.resolvedWith.loneElement._1.name shouldBe "LACTANTIA BUTTER 454G"
    }

    "carry a correlation id naming the migration and the key" in {
      val h = new Harness(_ => ResolutionOutcome.NoMatch)
      h.backfill.run(List(row(key = "dk-77")), now = () => at).futureValue
      h.resolvedWith.loneElement._2 shouldBe CorrelationId("backfill:demeter:dk-77")
    }

    "record no listing — Flipp item ids change weekly (§2.6 quirk #4)" in {
      val h = new Harness(_ => ResolutionOutcome.NoMatch)
      h.backfill.run(List(row()), now = () => at).futureValue
      h.resolvedWith.loneElement._1.listing shouldBe None
    }
  }

  "the report" should {
    "account for every row, so a run can be verified against the export's size" in {
      val h = new Harness(s =>
        if s.name.contains("A") then ResolutionOutcome.NoMatch
        else if s.name.contains("B") then ResolutionOutcome.Ambiguous(Nil)
        else ResolutionOutcome.Matched(ProductId("x"), Confidence.Certain, MatchMethod.Gtin)
      )
      val rows = List(row("k1", "A one"), row("k2", "B two"), row("k3", "C three"))
      val report = h.backfill.run(rows, now = () => at).futureValue
      report.total shouldBe 3
      report.registered shouldBe 1
      report.ambiguous shouldBe 1
      report.matched shouldBe 1
      h.mappings.map(_.productKey).toList shouldBe List("k1", "k2", "k3")
    }
  }
}

package me.cference.ariadne.grpc

import ariadne.v1 as pb
import me.cference.ariadne.domain.{Confidence, ProductId, PurchaseId}
import me.cference.ariadne.domain.product.ProductCommand
import me.cference.ariadne.domain.purchase.PurchaseCommand
import me.cference.ariadne.domain.resolution.{MatchSubject, ScoredCandidate}
import me.cference.ariadne.projection.ReadModelRepository.{
  PriceHistoryRow,
  ProductRow,
  ResolvedPrice
}
import me.cference.ariadne.domain.MatchMethod
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * The service's async behaviour, against a stubbed repository.
 *
 * `ReadModelRepository` is a class rather than a trait, so the stub overrides only the three reads
 * this spec drives. That is a seam worth noting rather than hiding: the repository being concrete
 * is why this spec stubs by subclassing instead of by interface.
 */
final class AriadneCatalogServiceWiringSpec extends AnyWordSpec with Matchers with ScalaFutures {

  private def row(id: String, status: String = "active", mergedInto: Option[String] = None) =
    ProductRow(id, s"product $id", None, None, None, None, status, mergedInto)

  /** Implements only what this spec reads; the rest answers empty. */
  final private class StubReads(
      products: Map[String, ProductRow] = Map.empty,
      canonical: Map[String, String] = Map.empty
  ) extends CatalogReads {
    def product(id: String): Future[Option[ProductRow]] = Future.successful(products.get(id))
    def canonicalProductId(id: String): Future[Option[String]] =
      Future.successful(canonical.get(id))
    def searchProducts(term: String, limit: Int): Future[List[ProductRow]] =
      Future.successful(products.values.toList.filter(_.name.contains(term)).take(limit))
    def currentPrice(productId: String, storeId: String): Future[Option[ResolvedPrice]] =
      Future.successful(None)
    def priceHistory(
        productId: String,
        storeId: Option[String],
        from: Option[java.time.Instant],
        to: Option[java.time.Instant],
        limit: Int
    ): Future[List[PriceHistoryRow]] = Future.successful(Nil)
  }

  private def service(
      reads: CatalogReads = new StubReads(),
      outcome: ResolutionOutcome = ResolutionOutcome.NoMatch,
      productAnswer: Either[String, Unit] = Right(()),
      purchaseAnswer: Either[String, Unit] = Right(()),
      productSeen: mutable.ListBuffer[(ProductId, ProductCommand)] = mutable.ListBuffer.empty,
      purchaseSeen: mutable.ListBuffer[(PurchaseId, PurchaseCommand)] = mutable.ListBuffer.empty
  ) = new AriadneCatalogService(
    reads,
    (_, _) => Future.successful(outcome),
    (id, cmd) => { productSeen += ((id, cmd)); Future.successful(productAnswer) },
    (id, cmd) => { purchaseSeen += ((id, cmd)); Future.successful(purchaseAnswer) }
  )

  "GetProduct" should {

    "NOT follow a merge tombstone unless asked" in {
      // A caller must never be silently handed a different id than it asked for. Following is
      // opt-in precisely because Dionysus and Demeter hold ids Ariadne does not control.
      val repo0 = new StubReads(
        products = Map("old" -> row("old", "merged_into", Some("new")), "new" -> row("new")),
        canonical = Map("old" -> "new")
      )
      val res =
        service(repo0).getProduct(pb.GetProductRequest(productId = "old")).futureValue
      res.product.map(_.productId) shouldBe Some("old")
      res.redirected shouldBe false
      res.product.map(_.status) shouldBe Some(pb.ProductStatus.PRODUCT_STATUS_MERGED_INTO)
    }

    "follow it when asked, and SAY that it did" in {
      val repo0 = new StubReads(
        products = Map("old" -> row("old", "merged_into", Some("new")), "new" -> row("new")),
        canonical = Map("old" -> "new")
      )
      val res = service(repo0)
        .getProduct(pb.GetProductRequest(productId = "old", followMerges = true))
        .futureValue
      res.product.map(_.productId) shouldBe Some("new")
      res.redirected shouldBe true
      res.requestedProductId shouldBe "old"
    }

    "report an unknown product as an absent product, not an error" in {
      val res = service().getProduct(pb.GetProductRequest(productId = "nope")).futureValue
      res.product shouldBe None
    }

    "echo the caller's correlation id, and mint one when absent" in {
      val echoed = service()
        .getProduct(pb.GetProductRequest(productId = "x", correlationId = "c-9"))
        .futureValue
      echoed.correlationId shouldBe "c-9"
      // §8 wants the chain followable; an anonymous call is one nobody can explain later.
      service()
        .getProduct(pb.GetProductRequest(productId = "x"))
        .futureValue
        .correlationId should not be empty
    }
  }

  "ResolveProduct" should {

    "return the matched product with its confidence and method" in {
      val outcome = ResolutionOutcome.Matched(
        ProductId("prod-1"),
        Confidence.unsafe(0.97),
        MatchMethod.Gtin
      )
      val res = service(outcome = outcome)
        .resolveProduct(pb.ResolveProductRequest(name = "butter"))
        .futureValue
      res.outcome match {
        case pb.ResolveProductResponse.Outcome.Matched(m) =>
          m.productId shouldBe "prod-1"
          m.confidence shouldBe 0.97
          m.how shouldBe "Gtin"
        case other => fail(s"expected Matched, got $other")
      }
    }

    "return candidates AND a case id on ambiguity, never a pick" in {
      // Resolution never picks: several correct candidates and none unique is the ordinary
      // case, and auto-picking would attribute a listing to a product on no evidence.
      val candidates = List(
        ScoredCandidate(ProductId("a"), Confidence.unsafe(0.8), List("name")),
        ScoredCandidate(ProductId("b"), Confidence.unsafe(0.79), List("brand"))
      )
      val res = service(outcome = ResolutionOutcome.Ambiguous(candidates))
        .resolveProduct(pb.ResolveProductRequest(name = "butter"))
        .futureValue
      res.outcome match {
        case pb.ResolveProductResponse.Outcome.Ambiguous(a) =>
          a.candidates.map(_.productId) shouldBe Seq("a", "b")
          // Derived from the subject, so a later scrape of the same listing lands on the
          // SAME case rather than a parallel duplicate (§6.5).
          a.resolutionCaseId shouldBe
            ResolutionService.caseIdFor(MatchSubject(name = "butter")).value
        case other => fail(s"expected Ambiguous, got $other")
      }
    }

    "return a case id on no match too, so the question is followable" in {
      val res = service()
        .resolveProduct(pb.ResolveProductRequest(name = "butter"))
        .futureValue
      res.outcome match {
        case pb.ResolveProductResponse.Outcome.NoMatch(n) =>
          n.resolutionCaseId should not be empty
        case other => fail(s"expected NoMatch, got $other")
      }
    }

    "build a listing key only when BOTH store and external id are given" in {
      // A half-named listing is not a listing; treating it as one would key the review case
      // on a store with no item, and the case id would differ from the scrape's.
      val res = service()
        .resolveProduct(pb.ResolveProductRequest(name = "butter", storeId = "s-1"))
        .futureValue
      res.outcome match {
        case pb.ResolveProductResponse.Outcome.NoMatch(n) =>
          n.resolutionCaseId shouldBe
            ResolutionService.caseIdFor(MatchSubject(name = "butter")).value
        case other => fail(s"expected NoMatch, got $other")
      }
    }
  }

  "GetCurrentPrice" should {
    "refuse a request with no store, because scope is part of a price's identity" in {
      // §2.3.1. "The current price" with no store named has no answer that is not a guess
      // across incomparable scopes.
      service()
        .getCurrentPrice(pb.GetCurrentPriceRequest(productId = "prod-1"))
        .failed
        .futureValue
        .getMessage should include("store_id is required")
    }
  }

  "RegisterProduct" should {

    "report a domain refusal in the BODY, not as a failed call" in {
      // These two RPCs go through `decide`, so "already registered" is information the caller
      // asked for. Raising it as a status code would make an expected refusal and a genuine
      // fault indistinguishable.
      val res = service(productAnswer = Left("already registered"))
        .registerProduct(pb.RegisterProductRequest(name = "Butter"))
        .futureValue
      res.accepted shouldBe false
      res.refusal shouldBe "already registered"
      res.productId should not be empty
    }

    "derive an id when the caller gives none, and keep the caller's when it does" in {
      val seen = mutable.ListBuffer.empty[(ProductId, ProductCommand)]
      service(productSeen = seen)
        .registerProduct(pb.RegisterProductRequest(productId = "mine", name = "Butter"))
        .futureValue
        .productId shouldBe "mine"

      val derived = service()
        .registerProduct(pb.RegisterProductRequest(name = "Butter", brand = "Lactantia"))
        .futureValue
        .productId
      derived shouldBe
        AriadneCatalogService.derivedProductId("Butter", Some("Lactantia")).value
      seen.map(_._1) shouldBe Seq(ProductId("mine"))
    }

    "refuse a nameless product without troubling the aggregate" in {
      val seen = mutable.ListBuffer.empty[(ProductId, ProductCommand)]
      val res = service(productSeen = seen)
        .registerProduct(pb.RegisterProductRequest(name = "  "))
        .futureValue
      res.accepted shouldBe false
      res.refusal should include("name is required")
      seen shouldBe empty
    }
  }

  "RecordPurchase" should {

    "accept a well-formed receipt and pass the caller's id through" in {
      val seen = mutable.ListBuffer.empty[(PurchaseId, PurchaseCommand)]
      val res = service(purchaseSeen = seen)
        .recordPurchase(
          pb.RecordPurchaseRequest(
            purchaseId = "pur-1",
            storeId = "store-1",
            purchasedAt = "2026-11-10T14:30:00Z",
            lines = Seq(
              pb.PurchaseLine(
                "prod-1",
                "1",
                Some(pb.Money("4.99", "CAD")),
                Some(pb.Money("4.99", "CAD"))
              )
            ),
            total = Some(pb.Money("4.99", "CAD"))
          )
        )
        .futureValue
      res.accepted shouldBe true
      res.purchaseId shouldBe "pur-1"
      seen.map(_._1) shouldBe Seq(PurchaseId("pur-1"))
    }

    "report a malformed request as a refusal rather than a failed call" in {
      val res = service()
        .recordPurchase(pb.RecordPurchaseRequest(purchaseId = "", storeId = "s"))
        .futureValue
      res.accepted shouldBe false
      res.refusal should include("caller mints it")
    }

    "report a DIFFERENT purchase under a taken id as a refusal, carrying the aggregate's words" in {
      val res = service(purchaseAnswer = Left("already registered"))
        .recordPurchase(
          pb.RecordPurchaseRequest(
            purchaseId = "pur-1",
            storeId = "store-1",
            purchasedAt = "2026-11-10T14:30:00Z",
            lines = Seq(
              pb.PurchaseLine(
                "prod-1",
                "1",
                Some(pb.Money("4.99", "CAD")),
                Some(pb.Money("4.99", "CAD"))
              )
            ),
            total = Some(pb.Money("4.99", "CAD"))
          )
        )
        .futureValue
      res.accepted shouldBe false
      res.refusal shouldBe "already registered"
    }
  }
}

package me.cference.ariadne.grpc

import ariadne.v1 as pb
import me.cference.ariadne.domain.{CorrelationId, ProductId, PurchaseId, StoreId}
import me.cference.ariadne.domain.purchase.{PurchaseCommand, PurchaseSource}
import me.cference.ariadne.projection.ReadModelRepository.{
  PriceHistoryRow,
  ProductRow,
  ResolvedPrice
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

/**
 * The gRPC surface's translation layer (§4).
 *
 * The pure mappings are tested directly rather than over a real channel. What can go wrong here is
 * a FIELD — a scope flattened to a store id, a confidence defaulted to zero, a refusal raised as an
 * error — and a channel test would exercise Pekko gRPC's transport while telling us nothing about
 * any of those. The service's own async methods are covered in `AriadneCatalogServiceWiringSpec`,
 * which stubs the repository.
 */
final class AriadneCatalogServiceSpec extends AnyWordSpec with Matchers {

  import AriadneCatalogService.*

  private val at = Instant.parse("2026-11-10T14:30:00Z")

  private def historyRow(
      scopeKind: String = "exact",
      storeId: Option[String] = Some("store-1"),
      chainId: Option[String] = None,
      area: Option[String] = None,
      promo: Option[String] = None,
      unitPrice: Option[BigDecimal] = None
  ) = PriceHistoryRow(
    productId = "prod-1",
    scopeKind = scopeKind,
    storeId = storeId,
    chainId = chainId,
    area = area,
    observedAt = at,
    priceAmount = BigDecimal("4.99"),
    priceCurrency = "CAD",
    unitPrice = unitPrice,
    unitPerAmount = None,
    unitPerUnit = None,
    promo = promo,
    priceConfidence = 0.9,
    sizeConfidence = 0.5,
    source = "scrape",
    rawResponseId = Some(7L),
    correlationId = Some("c-1"),
    persistenceId = "price|prod-1|store:store-1",
    seqNr = 3L
  )

  "a price point" should {

    "carry its scope as a TAGGED union, so an area price cannot read as a store price" in {
      // §2.3.1. This is the field whose loss is silent: a flyer claim flattened to a store id
      // would tell Demeter a specific franchise charges that price, on no evidence.
      val exact = toPricePoint(historyRow())
      exact.scope.map(_.scope) shouldBe Some(pb.PriceScope.Scope.Exact(pb.ExactScope("store-1")))

      val areaRow = historyRow(
        scopeKind = "area",
        storeId = None,
        chainId = Some("iga"),
        area = Some("H2X")
      )
      toPricePoint(areaRow).scope.map(_.scope) shouldBe
        Some(pb.PriceScope.Scope.Area(pb.AreaScope("iga", "H2X")))
    }

    "carry BOTH confidences" in {
      // §2.3: Demeter's match confidence is the weakest link of its own split confidence and
      // sizeConfidence, so dropping either lets downstream judgment read too high.
      val p = toPricePoint(historyRow())
      p.priceConfidence shouldBe 0.9
      p.sizeConfidence shouldBe 0.5
    }

    "send money as a decimal string, never a float" in {
      // 4.99 has no exact binary representation; a consumer comparing prices for a deal would
      // be comparing rounding artefacts.
      toPricePoint(historyRow()).price shouldBe Some(pb.Money("4.99", "CAD"))
    }

    "treat an absent promo as not-on-promo, and an absent unit price as absent" in {
      val plain = toPricePoint(historyRow())
      plain.promo shouldBe false
      plain.unitPrice shouldBe None
      toPricePoint(historyRow(promo = Some("2-for-1"))).promo shouldBe true
      toPricePoint(historyRow(unitPrice = Some(BigDecimal("1.10")))).unitPrice shouldBe
        Some(pb.Money("1.10", "CAD"))
    }
  }

  "a product" should {

    "map a known status, and refuse to guess an unknown one" in {
      toProduct(row(status = "active")).status shouldBe pb.ProductStatus.PRODUCT_STATUS_ACTIVE
      toProduct(
        row(status = "provisional")
      ).status shouldBe pb.ProductStatus.PRODUCT_STATUS_PROVISIONAL
      toProduct(
        row(status = "merged_into")
      ).status shouldBe pb.ProductStatus.PRODUCT_STATUS_MERGED_INTO
      // A status a new version introduced must not read as "fine to use" on an old consumer.
      toProduct(row(status = "something-new")).status shouldBe
        pb.ProductStatus.PRODUCT_STATUS_UNSPECIFIED
    }

    "carry size only when both halves are present" in {
      toProduct(row(sizeAmount = Some(BigDecimal(454)), sizeUnit = Some("Gram"))).size shouldBe
        Some(pb.Quantity("454", "Gram"))
      // An amount with no unit is not a size, it is a number. Sending it would invite a
      // consumer to assume grams.
      toProduct(row(sizeAmount = Some(BigDecimal(454)), sizeUnit = None)).size shouldBe None
    }

    "strip trailing zeros so a size compares equal across writers" in {
      toProduct(row(sizeAmount = Some(BigDecimal("454.00")), sizeUnit = Some("Gram"))).size shouldBe
        Some(pb.Quantity("454", "Gram"))
    }
  }

  "a resolved current price" should {
    "state which scope it gave you (§2.3.1 exact-over-area)" in {
      val exact =
        ResolvedPrice(
          new java.math.BigDecimal("4.99"),
          "CAD",
          at,
          "purchase",
          "exact",
          1.0,
          Some("store-1"),
          None,
          None
        )
      toScope(exact).scope shouldBe pb.PriceScope.Scope.Exact(pb.ExactScope("store-1"))

      val area =
        ResolvedPrice(
          new java.math.BigDecimal("4.99"),
          "CAD",
          at,
          "scrape",
          "area",
          0.5,
          None,
          Some("iga"),
          Some("H2X")
        )
      toScope(area).scope shouldBe pb.PriceScope.Scope.Area(pb.AreaScope("iga", "H2X"))
    }
  }

  "a page size" should {
    "fall back to the default when unset, because proto3 cannot tell 0 from absent" in {
      clamp(0, default = 20, max = 100) shouldBe 20
      clamp(-5, default = 20, max = 100) shouldBe 20
    }
    "cap an over-large request rather than honour an unbounded read" in {
      clamp(10_000, default = 500, max = 5000) shouldBe 5000
      clamp(50, default = 500, max = 5000) shouldBe 50
    }
  }

  "a derived product id" should {
    "be stable for the same name and brand, so a retried registration lands twice on one product" in {
      // §6.4.1's reasoning: a caller that never saw its response must re-register onto the
      // SAME product rather than create a second one.
      derivedProductId("Lactantia Butter", Some("Lactantia")) shouldBe
        derivedProductId("  lactantia   butter ", Some("LACTANTIA"))
      derivedProductId("Butter", Some("Lactantia")) should not be
        derivedProductId("Butter", Some("Natrel"))
    }
  }

  "parsing a purchase" should {

    "refuse a missing id rather than mint one (§7.1)" in {
      val in = recordRequest(purchaseId = "")
      parsePurchase(in, CorrelationId("c-1")).left.map(_.contains("caller mints it")) shouldBe
        Left(true)
    }

    "refuse money that is not a decimal before the aggregate sees it" in {
      val in = recordRequest(total = pb.Money("4,99", "CAD"))
      parsePurchase(in, CorrelationId("c-1")).isLeft shouldBe true
    }

    "name the offending line" in {
      val in = recordRequest(lines =
        Seq(
          line(),
          line(productId = "prod-2", pricePaid = pb.Money("nope", "CAD"))
        )
      )
      parsePurchase(in, CorrelationId("c-1")).left.map(_.contains("lines[1]")) shouldBe Left(true)
    }

    "refuse a receipt source that cannot name its scan" in {
      val in = recordRequest(source = "receipt")
      parsePurchase(in, CorrelationId("c-1")).left
        .map(_.contains("receipt_blob_id")) shouldBe Left(true)
    }

    "build the command with the caller's id and the correlation id" in {
      parsePurchase(recordRequest(), CorrelationId("c-7")) match {
        case Right((id, c: PurchaseCommand.RecordPurchase)) =>
          id shouldBe PurchaseId("pur-1")
          c.storeId shouldBe StoreId("store-1")
          c.purchasedAt shouldBe at
          c.source shouldBe PurchaseSource.Manual
          c.correlationId shouldBe CorrelationId("c-7")
          c.lines.map(_.productId) shouldBe List(ProductId("prod-1"))
        case other => fail(s"expected a RecordPurchase, got $other")
      }
    }
  }

  private def row(
      status: String = "active",
      sizeAmount: Option[BigDecimal] = None,
      sizeUnit: Option[String] = None
  ) = ProductRow(
    id = "prod-1",
    name = "Lactantia Butter",
    brand = Some("Lactantia"),
    category = Some("dairy"),
    sizeAmount = sizeAmount,
    sizeUnit = sizeUnit,
    status = status,
    mergedInto = None
  )

  private def line(
      productId: String = "prod-1",
      pricePaid: pb.Money = pb.Money("4.99", "CAD")
  ) = pb.PurchaseLine(
    productId = productId,
    quantity = "1",
    pricePaid = Some(pricePaid),
    lineTotal = Some(pb.Money("4.99", "CAD"))
  )

  private def recordRequest(
      purchaseId: String = "pur-1",
      total: pb.Money = pb.Money("4.99", "CAD"),
      lines: Seq[pb.PurchaseLine] = Seq(line()),
      source: String = "manual"
  ) = pb.RecordPurchaseRequest(
    purchaseId = purchaseId,
    storeId = "store-1",
    purchasedAt = at.toString,
    lines = lines,
    total = Some(total),
    source = source
  )
}

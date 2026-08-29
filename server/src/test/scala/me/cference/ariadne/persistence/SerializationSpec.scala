package me.cference.ariadne.persistence

import com.typesafe.config.ConfigFactory
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.purchase.{PurchaseEvent, PurchaseLine, PurchaseSource}
import me.cference.ariadne.domain.store.StoreEvent
import me.cference.ariadne.domain.resolution.{
  MatchSubject,
  ParkedObservation,
  ResolutionEvent,
  ScoredCandidate
}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.serialization.{SerializationExtension, Serializers}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant

object SerializationSpec {
  // `+=` on jackson-modules needs the base config present before resolution.
  val config = ConfigFactory.load().resolve()
}

/**
 * Every journaled event must survive a round trip byte-for-byte.
 *
 * This is not ceremony. A serialization gap does not fail when the event is written — it fails on
 * REPLAY, which means the aggregate refuses to recover or the projections rebuild wrong, long after
 * the events were accepted. Two real defects were found by this suite before anything was built on
 * top of it:
 *
 *   1. Scala 3 `enum`s with parameterised cases are not Jackson-serialisable at all ("Failed to
 *      create Enum instance"). The journaled ADTs became sealed traits because of this test. 2.
 *      Nested sealed traits need explicit polymorphic type information, supplied by the mix-ins.
 */
final class SerializationSpec
    extends ScalaTestWithActorTestKit(SerializationSpec.config)
    with AnyWordSpecLike
    with Matchers {

  private val ser = SerializationExtension(system.classicSystem)

  private def roundTrip[A <: AnyRef](original: A): A = {
    val s = ser.findSerializerFor(original)
    val manifest = Serializers.manifestFor(s, original)
    ser.deserialize(ser.serialize(original).get, s.identifier, manifest).get.asInstanceOf[A]
  }

  private val gtin = Gtin.unsafe("4006381333931")
  private val listing = ListingKey(StoreId("s-1"), "ext-9")
  private val money = Money.unsafe(BigDecimal("4.99"))
  private val packSize = Quantity.unsafe(BigDecimal(454), MeasureUnit.Gram)

  "product events" should {

    "round-trip a full ProductRegistered, including Origin and status" in {
      val e = ProductEvent.ProductRegistered(
        ProductId("p-1"),
        "Lactantia Salted Butter",
        Some("Lactantia"),
        Some("dairy"),
        Some(packSize),
        Some(gtin),
        Origin.Scrape(listing),
        ProductStatus.Provisional
      )
      roundTrip(e) shouldBe e
    }

    "round-trip with every optional field absent" in {
      // The empty case exercises a different Jackson path than the full one and is
      // exactly what a sparse scraped listing produces.
      val e = ProductEvent.ProductRegistered(
        ProductId("p-2"),
        "Butter",
        None,
        None,
        None,
        None,
        Origin.Manual,
        ProductStatus.Active
      )
      roundTrip(e) shouldBe e
    }

    "round-trip a ListingLinked, which carries an opaque Confidence" in {
      val e = ProductEvent.ListingLinked(
        listing,
        Confidence.unsafe(0.93),
        MatchMethod.Fuzzy,
        MatcherVersion("v1")
      )
      roundTrip(e) shouldBe e
      roundTrip(e).confidence.toDouble shouldBe 0.93
    }

    "round-trip a merge tombstone and an absorption with its key sets" in {
      roundTrip(ProductEvent.ProductMerged(ProductId("p-9"))) shouldBe ProductEvent.ProductMerged(
        ProductId("p-9")
      )
      val absorbed =
        ProductEvent.ProductAbsorbed(ProductId("p-9"), Set(gtin), Set("beurre"), Set(listing))
      roundTrip(absorbed) shouldBe absorbed
    }
  }

  "store events" should {
    "round-trip registration and the parameterless deactivation" in {
      val reg = StoreEvent.StoreRegistered(
        StoreId("s-1"),
        "IGA Plateau",
        ChainId("iga"),
        Area("H2X"),
        Some("Plateau")
      )
      roundTrip(reg) shouldBe reg
      // A case object is the shape most likely to be mangled into a bare string.
      roundTrip(StoreEvent.StoreDeactivated) shouldBe StoreEvent.StoreDeactivated
    }
  }

  "price events" should {

    "round-trip an exact-scoped observation with unit price and promo" in {
      val e = PriceEvent.PriceObserved(
        ProductId("p-1"),
        PriceScope.Exact(StoreId("s-1")),
        money,
        Some(UnitPrice.from(money, packSize)),
        Some(PromoFlag("30% off", Some(BigDecimal(30)))),
        Confidence.Certain,
        Confidence.unsafe(0.4),
        Instant.parse("2026-08-26T12:00:00Z"),
        PriceSource.Purchase(PurchaseId("pu-1"))
      )
      roundTrip(e) shouldBe e
    }

    "round-trip a REGIONAL observation — the scope must survive intact" in {
      // §2.3.1: scope is identity, not decoration. If it degraded on replay, a flyer
      // fact would come back looking like a store fact and the read-side fan-out
      // would silently start fabricating precision.
      val e = PriceEvent.PriceObserved(
        ProductId("p-1"),
        PriceScope.Regional(ChainId("iga"), Area("H2X")),
        money,
        None,
        None,
        Confidence.Certain,
        Confidence.Certain,
        Instant.parse("2026-08-26T12:00:00Z"),
        PriceSource.Scrape("flipp")
      )
      val back = roundTrip(e)
      back shouldBe e
      back.scope shouldBe PriceScope.Regional(ChainId("iga"), Area("H2X"))
      back.scope.isExact shouldBe false
    }

    "round-trip every PriceSource variant" in {
      List(
        PriceSource.Scrape("flipp"),
        PriceSource.Purchase(PurchaseId("pu-1")),
        PriceSource.Manual,
        PriceSource.Backfill("demeter")
      ).foreach { src =>
        val e = PriceEvent.PriceObserved(
          ProductId("p-1"),
          PriceScope.Exact(StoreId("s-1")),
          money,
          None,
          None,
          Confidence.Certain,
          Confidence.Certain,
          Instant.parse("2026-08-26T12:00:00Z"),
          src
        )
        roundTrip(e).source shouldBe src
      }
    }
  }

  "resolution events" should {

    "round-trip a proposal with its subject and scored candidates" in {
      val e = ResolutionEvent.ResolutionProposed(
        me.cference.ariadne.domain.resolution.ResolutionId("r-1"),
        MatchSubject("Lactantia Butter 454 g", Some("Lactantia"), Some(gtin), Some(listing)),
        List(
          ScoredCandidate(ProductId("p-a"), Confidence.unsafe(0.81), List("size conflict")),
          ScoredCandidate(ProductId("p-b"), Confidence.unsafe(0.74), Nil)
        )
      )
      roundTrip(e) shouldBe e
    }

    "round-trip a confirmation carrying released parked observations" in {
      // The richest payload in the journal: a nested PriceScope inside a list inside
      // an event. If the released observations did not survive replay, confirming a
      // case would silently drop real market facts.
      val parked = ParkedObservation(
        money,
        Instant.parse("2026-08-26T12:00:00Z"),
        PriceScope.Regional(ChainId("iga"), Area("H2X")),
        Confidence.Certain,
        Confidence.unsafe(0.4)
      )
      val e = ResolutionEvent.ResolutionConfirmed(ProductId("p-a"), List(parked, parked))
      val back = roundTrip(e)
      back shouldBe e
      back.released should have size 2
      back.released.head.scope shouldBe PriceScope.Regional(ChainId("iga"), Area("H2X"))
    }
  }

  "purchase events" should {
    "round-trip a recorded purchase with its lines" in {
      val e = PurchaseEvent.PurchaseRecorded(
        PurchaseId("pu-1"),
        StoreId("s-1"),
        Instant.parse("2026-08-26T12:00:00Z"),
        List(
          PurchaseLine(ProductId("p-1"), BigDecimal(2), money, Money.unsafe(BigDecimal("9.98")))
        ),
        Money.unsafe(BigDecimal("9.98")),
        PurchaseSource.Receipt("blob-1")
      )
      roundTrip(e) shouldBe e
    }
  }
}

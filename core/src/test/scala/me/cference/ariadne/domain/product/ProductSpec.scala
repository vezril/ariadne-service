package me.cference.ariadne.domain
package product

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class ProductSpec extends AnyFunSuite with Matchers {

  private val cid = CorrelationId("c-1")
  private val pid = ProductId("p-1")
  private val other = ProductId("p-2")
  private val storeId = StoreId("s-1")
  private val listing = ListingKey(storeId, "ext-99")
  private val gtin = Gtin.unsafe("4006381333931")
  private val matcher = MatcherVersion("v1")

  private def register(origin: Origin, name: String = "Lactantia Butter") =
    ProductCommand.RegisterProduct(
      pid,
      name,
      Some("Lactantia"),
      Some("dairy"),
      None,
      None,
      origin,
      cid
    )

  private def existing(origin: Origin = Origin.Manual): ProductState =
    Product
      .decide(ProductState.Empty, register(origin))
      .map(Product.replay)
      .getOrElse(fail("should register"))

  test("a scraped product is born Provisional — unreviewed, and it must show that") {
    // Provenance decides trust. An auto-created product entering the catalog as
    // Active would launder a guess into a confirmed fact.
    val s = existing(Origin.Scrape("flipp", Some(listing)))
    s match {
      case ProductState.Existing(_, _, _, _, _, _, _, _, status) =>
        status shouldBe ProductStatus.Provisional
      case ProductState.Empty => fail("should exist")
    }
  }

  test("a manually registered product is Active") {
    existing(Origin.Manual) match {
      case ProductState.Existing(_, _, _, _, _, _, _, _, status) =>
        status shouldBe ProductStatus.Active
      case ProductState.Empty => fail("should exist")
    }
  }

  test("a blank name is refused, and names are trimmed") {
    Product.decide(ProductState.Empty, register(Origin.Manual, "   ")) shouldBe Left(
      DomainError.EmptyName
    )
    Product
      .decide(ProductState.Empty, register(Origin.Manual, "  Butter  "))
      .map(Product.replay) match {
      case Right(ProductState.Existing(_, name, _, _, _, _, _, _, _)) => name shouldBe "Butter"
      case other => fail(s"unexpected: $other")
    }
  }

  test("registering twice is refused; commands before registration are refused") {
    Product.decide(existing(), register(Origin.Manual)) shouldBe Left(DomainError.AlreadyRegistered)
    Product.decide(ProductState.Empty, ProductCommand.AddAlias("x", cid)) shouldBe Left(
      DomainError.NotRegistered
    )
  }

  test("a merged product is a tombstone that refuses all writes and names its canonical id") {
    // Ids never die, they forward. Writing to a tombstone would append facts to
    // a product that no longer exists as a distinct thing.
    val merged = Product.replay(
      List(
        ProductEvent.ProductRegistered(
          pid,
          "Butter",
          None,
          None,
          None,
          None,
          Origin.Manual,
          ProductStatus.Active
        ),
        ProductEvent.ProductMerged(other)
      )
    )
    Product.decide(merged, ProductCommand.AddAlias("beurre", cid)) shouldBe
      Left(DomainError.ProductIsTombstone(other))
    Product.decide(merged, ProductCommand.AddIdentifier(gtin, cid)) shouldBe
      Left(DomainError.ProductIsTombstone(other))
  }

  test("a product cannot be merged into, or absorb, itself") {
    Product.decide(existing(), ProductCommand.MergeInto(pid, None, cid)) shouldBe Left(
      DomainError.CannotMergeIntoSelf
    )
    Product.decide(
      existing(),
      ProductCommand.Absorb(pid, Set.empty, Set.empty, Set.empty, cid)
    ) shouldBe
      Left(DomainError.CannotMergeIntoSelf)
  }

  test("absorbing takes over the loser's keys so its identifiers keep resolving") {
    val absorbed = Product.replay(
      List(
        ProductEvent.ProductRegistered(
          pid,
          "Butter",
          None,
          None,
          None,
          None,
          Origin.Scrape("flipp", Some(listing)),
          ProductStatus.Provisional
        ),
        ProductEvent.ProductAbsorbed(other, Set(gtin), Set("beurre"), Set(listing))
      )
    )
    absorbed match {
      case ProductState.Existing(_, _, _, _, _, gtins, aliases, listings, status) =>
        gtins should contain(gtin)
        aliases should contain("beurre")
        listings should contain(listing)
        // A provisional product confirmed by absorbing a real one is no longer a guess.
        status shouldBe ProductStatus.Active
      case ProductState.Empty => fail("should exist")
    }
  }

  test("repeat identifiers, aliases and listings are no-ops, not errors") {
    // Scrapes repeat. A repeat must not fail a scrape run, and must not append
    // a duplicate event to the journal either.
    val s = Product.replay(
      List(
        ProductEvent.ProductRegistered(
          pid,
          "Butter",
          None,
          None,
          None,
          Some(gtin),
          Origin.Manual,
          ProductStatus.Active
        ),
        ProductEvent.ProductAliasAdded("beurre"),
        ProductEvent.ListingLinked(listing, Confidence.Certain, MatchMethod.Gtin, matcher)
      )
    )
    Product.decide(s, ProductCommand.AddIdentifier(gtin, cid)) shouldBe Right(Nil)
    Product.decide(s, ProductCommand.AddAlias("beurre", cid)) shouldBe Right(Nil)
    Product.decide(
      s,
      ProductCommand.LinkListing(listing, Confidence.Certain, MatchMethod.Gtin, matcher, None, cid)
    ) shouldBe Right(Nil)
  }

  test("every link records how and by which matcher version — §6.6") {
    // Without matcher_version a resolver change silently orphans history; this
    // is the field that makes a re-match deliberate instead of invisible.
    val s = existing()
    Product.decide(
      s,
      ProductCommand
        .LinkListing(listing, Confidence.unsafe(0.95), MatchMethod.Fuzzy, matcher, None, cid)
    ) match {
      case Right(List(e: ProductEvent.ListingLinked)) =>
        e.how shouldBe MatchMethod.Fuzzy
        e.matcher shouldBe matcher
        e.confidence.toDouble shouldBe 0.95
      case other => fail(s"unexpected: $other")
    }
  }

  test("deprecating twice is refused") {
    val s = Product.replay(
      List(
        ProductEvent.ProductRegistered(
          pid,
          "Butter",
          None,
          None,
          None,
          None,
          Origin.Manual,
          ProductStatus.Active
        ),
        ProductEvent.ProductDeprecated("discontinued")
      )
    )
    Product.decide(s, ProductCommand.Deprecate("again", cid)) shouldBe Left(
      DomainError.AlreadyDeprecated
    )
  }

  test("replay is the definition of state: folding the events reproduces it exactly") {
    val events = List(
      ProductEvent.ProductRegistered(
        pid,
        "Butter",
        Some("Lactantia"),
        Some("dairy"),
        None,
        Some(gtin),
        Origin.Manual,
        ProductStatus.Active
      ),
      ProductEvent.ProductAliasAdded("beurre"),
      ProductEvent.ListingLinked(listing, Confidence.Certain, MatchMethod.Gtin, matcher),
      ProductEvent.ProductDeprecated("discontinued")
    )
    Product.replay(events) shouldBe events.foldLeft[ProductState](ProductState.Empty)(
      Product.evolve
    )
  }
}

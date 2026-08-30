package me.cference.ariadne.persistence

import me.cference.ariadne.domain.{Area, ChainId, PriceScope, ProductId, StoreId}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Sharding identity, tested because two of these are silent failures rather than crashes.
 */
final class ShardingSpec extends AnyWordSpec with Matchers {

  "every entity type key" should {
    "carry its prefix as its name" in {
      // This exists because of a real bug: `TypeKey` was declared BEFORE `EntityPrefix`,
      // and a forward reference to a val initialises to null. It compiled, and every key
      // was silently named null — which would have routed every entity type to the same
      // shard region.
      ProductEntity.TypeKey.name shouldBe ProductEntity.EntityPrefix
      StoreEntity.TypeKey.name shouldBe StoreEntity.EntityPrefix
      PurchaseEntity.TypeKey.name shouldBe PurchaseEntity.EntityPrefix
      ResolutionCaseEntity.TypeKey.name shouldBe ResolutionCaseEntity.EntityPrefix
      PriceStreamEntity.TypeKey.name shouldBe PriceStreamEntity.EntityPrefix

      List(
        ProductEntity.TypeKey.name,
        StoreEntity.TypeKey.name,
        PurchaseEntity.TypeKey.name,
        ResolutionCaseEntity.TypeKey.name,
        PriceStreamEntity.TypeKey.name
      ).foreach(n => n should not be null)
    }

    "be distinct, so entity types cannot collide in the same shard region" in {
      val names = List(
        ProductEntity.TypeKey.name,
        StoreEntity.TypeKey.name,
        PurchaseEntity.TypeKey.name,
        ResolutionCaseEntity.TypeKey.name,
        PriceStreamEntity.TypeKey.name
      )
      names.distinct should have size names.size
    }
  }

  "the price entity id" should {

    "round-trip an exact scope" in {
      val id = Sharding.priceEntityId(ProductId("p-1"), PriceScope.Exact(StoreId("s-1")))
      PriceStreamEntity
        .parseEntityId(id) shouldBe ((ProductId("p-1"), PriceScope.Exact(StoreId("s-1"))))
    }

    "round-trip a regional scope" in {
      val scope = PriceScope.Regional(ChainId("iga"), Area("H2X"))
      val id = Sharding.priceEntityId(ProductId("p-1"), scope)
      PriceStreamEntity.parseEntityId(id) shouldBe ((ProductId("p-1"), scope))
    }

    "produce the SAME persistence id sharding would" in {
      // Sharding must not be a journal migration: PersistenceId(entityType, entityId)
      // has to render the string the entities already wrote under.
      val scope = PriceScope.Regional(ChainId("iga"), Area("H2X"))
      PriceStreamEntity.persistenceId(ProductId("p-1"), scope).id shouldBe
        s"${PriceStreamEntity.EntityPrefix}|${Sharding.priceEntityId(ProductId("p-1"), scope)}"
    }

    "reject a malformed id rather than inventing a scope" in {
      an[IllegalArgumentException] should be thrownBy PriceStreamEntity.parseEntityId(
        "no-separator"
      )
      an[IllegalArgumentException] should be thrownBy PriceStreamEntity.parseEntityId(
        "p-1|nonsense:x:y:z"
      )
    }
  }
}

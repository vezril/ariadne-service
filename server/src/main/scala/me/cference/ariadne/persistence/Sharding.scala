package me.cference.ariadne.persistence

import me.cference.ariadne.domain.{PriceScope, ProductId, StoreId}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}

/**
 * Registers every entity type with cluster sharding.
 *
 * Sharding is what makes an entity addressable BY ID from anywhere — which is what the REST
 * surface, the resolver's case-opening and the scraper all need. It is also why the domain commands
 * became serializable: once entities are sharded, a command may cross a node boundary.
 *
 * The persistence ids are unchanged. `PersistenceId(entityType, entityId)` renders the same
 * `type|id` string the entities already used, so sharding is not a journal migration.
 */
object Sharding {

  def init(system: ActorSystem[?]): Unit = {
    val sharding = ClusterSharding(system)
    sharding.init(Entity(ProductEntity.TypeKey)(ctx => ProductEntity(ctx.entityId)))
    sharding.init(Entity(StoreEntity.TypeKey)(ctx => StoreEntity(ctx.entityId)))
    sharding.init(Entity(PurchaseEntity.TypeKey)(ctx => PurchaseEntity(ctx.entityId)))
    sharding.init(Entity(ResolutionCaseEntity.TypeKey)(ctx => ResolutionCaseEntity(ctx.entityId)))
    sharding.init(
      Entity(PriceStreamEntity.TypeKey) { ctx =>
        val (productId, scope) = PriceStreamEntity.parseEntityId(ctx.entityId)
        PriceStreamEntity(productId, scope)
      }
    )
  }

  // Addressing is by ID: `entityRefFor` is the typed API's way of saying "the entity
  // with this id, wherever it lives". Callers never learn which node that is.

  def product(system: ActorSystem[?], id: ProductId): EntityRef[ProductEntity.Command] =
    ClusterSharding(system).entityRefFor(ProductEntity.TypeKey, id.value)

  def store(system: ActorSystem[?], id: StoreId): EntityRef[StoreEntity.Command] =
    ClusterSharding(system).entityRefFor(StoreEntity.TypeKey, id.value)

  def purchase(system: ActorSystem[?], id: String): EntityRef[PurchaseEntity.Command] =
    ClusterSharding(system).entityRefFor(PurchaseEntity.TypeKey, id)

  def resolution(system: ActorSystem[?], id: String): EntityRef[ResolutionCaseEntity.Command] =
    ClusterSharding(system).entityRefFor(ResolutionCaseEntity.TypeKey, id)

  def price(
      system: ActorSystem[?],
      productId: ProductId,
      scope: PriceScope
  ): EntityRef[PriceStreamEntity.Command] =
    ClusterSharding(system).entityRefFor(PriceStreamEntity.TypeKey, priceEntityId(productId, scope))

  /** The sharded entity id for a price stream — `{productId}|{scope}` (§2.3.1). */
  def priceEntityId(productId: ProductId, scope: PriceScope): String =
    s"${productId.value}|${scope.key}"
}

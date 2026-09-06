package me.cference.ariadne.persistence

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceCommand, PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductCommand, ProductEvent, ProductStatus}
import me.cference.ariadne.domain.purchase.{PurchaseCommand, PurchaseEvent, PurchaseSource}
import me.cference.ariadne.domain.resolution.{ResolutionCommand, ResolutionEvent}
import me.cference.ariadne.domain.store.{StoreCommand, StoreEvent}

/**
 * Polymorphic type information for the journaled ADTs, supplied as MIX-INS.
 *
 * The annotations live here rather than on the domain types so `core` keeps no serialization
 * dependency at all (DESIGN §1) — the domain says what it is, the runtime says how it is stored.
 *
 * The `name` of each subtype is a WIRE CONTRACT: it is written into every persisted event, so
 * renaming one silently breaks replay of everything already in the journal. Rename the Scala class
 * freely; leave these strings alone.
 */
private[persistence] object DomainMixins {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(
        value = classOf[ProductEvent.ProductRegistered],
        name = "ProductRegistered"
      ),
      new JsonSubTypes.Type(
        value = classOf[ProductEvent.ProductIdentifierAdded],
        name = "ProductIdentifierAdded"
      ),
      new JsonSubTypes.Type(
        value = classOf[ProductEvent.ProductAliasAdded],
        name = "ProductAliasAdded"
      ),
      new JsonSubTypes.Type(value = classOf[ProductEvent.ListingLinked], name = "ListingLinked"),
      new JsonSubTypes.Type(value = classOf[ProductEvent.ProductMerged], name = "ProductMerged"),
      new JsonSubTypes.Type(
        value = classOf[ProductEvent.ProductAbsorbed],
        name = "ProductAbsorbed"
      ),
      new JsonSubTypes.Type(
        value = classOf[ProductEvent.ProductDeprecated],
        name = "ProductDeprecated"
      )
    )
  )
  trait ProductEventMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[StoreEvent.StoreRegistered], name = "StoreRegistered"),
      new JsonSubTypes.Type(
        value = classOf[StoreEvent.StoreDetailsUpdated],
        name = "StoreDetailsUpdated"
      ),
      new JsonSubTypes.Type(
        value = classOf[StoreEvent.StoreDeactivated.type],
        name = "StoreDeactivated"
      )
    )
  )
  trait StoreEventMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PriceEvent.PriceObserved], name = "PriceObserved"),
      new JsonSubTypes.Type(
        value = classOf[PriceEvent.PriceObservationRetracted],
        name = "PriceObservationRetracted"
      )
    )
  )
  trait PriceEventMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(
        value = classOf[PurchaseEvent.PurchaseRecorded],
        name = "PurchaseRecorded"
      ),
      new JsonSubTypes.Type(value = classOf[PurchaseEvent.PurchaseVoided], name = "PurchaseVoided")
    )
  )
  trait PurchaseEventMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[Origin.Manual.type], name = "Manual"),
      new JsonSubTypes.Type(value = classOf[Origin.Scrape], name = "Scrape"),
      new JsonSubTypes.Type(value = classOf[Origin.Migration], name = "Migration")
    )
  )
  trait OriginMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[ProductStatus.Provisional.type], name = "Provisional"),
      new JsonSubTypes.Type(value = classOf[ProductStatus.Active.type], name = "Active"),
      new JsonSubTypes.Type(value = classOf[ProductStatus.MergedInto], name = "MergedInto"),
      new JsonSubTypes.Type(value = classOf[ProductStatus.Deprecated.type], name = "Deprecated")
    )
  )
  trait ProductStatusMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PriceScope.Exact], name = "Exact"),
      new JsonSubTypes.Type(value = classOf[PriceScope.Regional], name = "Regional")
    )
  )
  trait PriceScopeMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PriceSource.Scrape], name = "Scrape"),
      new JsonSubTypes.Type(value = classOf[PriceSource.Purchase], name = "Purchase"),
      new JsonSubTypes.Type(value = classOf[PriceSource.Manual.type], name = "Manual"),
      new JsonSubTypes.Type(value = classOf[PriceSource.Backfill], name = "Backfill")
    )
  )
  trait PriceSourceMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PurchaseSource.Manual.type], name = "Manual"),
      new JsonSubTypes.Type(value = classOf[PurchaseSource.Receipt], name = "Receipt"),
      new JsonSubTypes.Type(value = classOf[PurchaseSource.BankImport], name = "BankImport")
    )
  )
  trait PurchaseSourceMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.ResolutionProposed],
        name = "ResolutionProposed"
      ),
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.ObservationParked],
        name = "ObservationParked"
      ),
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.ResolutionConfirmed],
        name = "ResolutionConfirmed"
      ),
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.ResolutionRejected],
        name = "ResolutionRejected"
      ),
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.MergeRequested],
        name = "MergeRequested"
      ),
      new JsonSubTypes.Type(
        value = classOf[ResolutionEvent.SplitRequested],
        name = "SplitRequested"
      )
    )
  )
  trait ResolutionEventMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(
        value = classOf[ProductCommand.RegisterProduct],
        name = "RegisterProduct"
      ),
      new JsonSubTypes.Type(value = classOf[ProductCommand.AddIdentifier], name = "AddIdentifier"),
      new JsonSubTypes.Type(value = classOf[ProductCommand.AddAlias], name = "AddAlias"),
      new JsonSubTypes.Type(value = classOf[ProductCommand.LinkListing], name = "LinkListing"),
      new JsonSubTypes.Type(value = classOf[ProductCommand.MergeInto], name = "MergeInto"),
      new JsonSubTypes.Type(value = classOf[ProductCommand.Absorb], name = "Absorb"),
      new JsonSubTypes.Type(value = classOf[ProductCommand.Deprecate], name = "Deprecate")
    )
  )
  trait ProductCommandMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[StoreCommand.RegisterStore], name = "RegisterStore"),
      new JsonSubTypes.Type(
        value = classOf[StoreCommand.UpdateStoreDetails],
        name = "UpdateStoreDetails"
      ),
      new JsonSubTypes.Type(value = classOf[StoreCommand.DeactivateStore], name = "DeactivateStore")
    )
  )
  trait StoreCommandMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PriceCommand.ObservePrice], name = "ObservePrice"),
      new JsonSubTypes.Type(
        value = classOf[PriceCommand.RetractObservation],
        name = "RetractObservation"
      )
    )
  )
  trait PriceCommandMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(
        value = classOf[PurchaseCommand.RecordPurchase],
        name = "RecordPurchase"
      ),
      new JsonSubTypes.Type(value = classOf[PurchaseCommand.VoidPurchase], name = "VoidPurchase")
    )
  )
  trait PurchaseCommandMixin

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[ResolutionCommand.Propose], name = "Propose"),
      new JsonSubTypes.Type(
        value = classOf[ResolutionCommand.ParkObservation],
        name = "ParkObservation"
      ),
      new JsonSubTypes.Type(value = classOf[ResolutionCommand.Confirm], name = "Confirm"),
      new JsonSubTypes.Type(value = classOf[ResolutionCommand.Reject], name = "Reject"),
      new JsonSubTypes.Type(value = classOf[ResolutionCommand.RequestMerge], name = "RequestMerge"),
      new JsonSubTypes.Type(value = classOf[ResolutionCommand.RequestSplit], name = "RequestSplit")
    )
  )
  trait ResolutionCommandMixin
}

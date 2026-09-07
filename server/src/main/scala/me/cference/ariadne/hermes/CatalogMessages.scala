package me.cference.ariadne.hermes

import me.cference.ariadne.domain.PriceScope
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.purchase.{PurchaseEvent, PurchaseSource}
import spray.json.*

/**
 * Domain event -> published message (§5).
 *
 * **The wire shape here is a PROPOSAL, not a landed contract.** The Lexicon has no product-catalog
 * schema: its only `catalog.proto` reserves `PostCreated`/`TagsChanged`/`PostPurged` for a
 * different "Ariadne" — an analytics consumer of Artemis's media catalog — which is a name
 * collision, not this service. `docs/lexicon-proposal-catalog.proto` is the shape these payloads
 * encode, written so the eventual generated codec is a drop-in: field names are the canonical
 * protobuf-JSON of that proto (lowerCamelCase), so agreeing the contract changes the encoder and
 * not the bytes.
 *
 * Until the Lexicon agrees it, a consumer building against these payloads is building against a
 * proposal. That is stated here rather than discovered by Demeter.
 */
object CatalogMessages extends DefaultJsonProtocol {

  /** A message ready to publish: where it goes, what identifies it, and what it says. */
  final case class Outgoing(
      topic: String,
      messageId: String,
      payload: String,
      attributes: Map[String, String],
      correlationId: String
  )

  /** The three topics (§5). Names are config so a rename is not a redeploy. */
  final case class Topics(
      productRegistered: String = "product.registered",
      priceObserved: String = "product.price.observed",
      purchaseRecorded: String = "purchase.recorded"
  ) {
    def all: List[String] = List(productRegistered, priceObserved, purchaseRecorded)
  }

  /**
   * The deterministic message id (§5) — `{persistenceId}:{seqNr}`.
   *
   * Derived from the journal rather than minted, which is what makes an at-least-once republish
   * recognisable as the same message: the pair is unique for the life of the journal and identical
   * across a retry.
   */
  def messageId(persistenceId: String, seqNr: Long): String = s"$persistenceId:$seqNr"

  /**
   * Product events worth publishing.
   *
   * A `None` is a deliberate silence, not an oversight: alias and identifier additions are internal
   * bookkeeping no consumer has asked for, and publishing everything the journal holds would make
   * the topic a change-data-capture feed rather than a contract.
   *
   * `ProductMerged` publishes ON `product.registered` as a `status=merged_into` message — a
   * consumer holding an id learns to re-point (§5). Whether that overloads the topic is §10.1, open
   * with the Lexicon, and deliberately not decided here.
   */
  def product(
      topics: Topics,
      persistenceId: String,
      seqNr: Long,
      correlationId: String,
      event: ProductEvent
  ): Option[Outgoing] = {
    val id = persistenceId.split('|').lastOption.getOrElse(persistenceId)
    event match {
      case e: ProductEvent.ProductRegistered =>
        Some(
          out(
            topics.productRegistered,
            persistenceId,
            seqNr,
            correlationId,
            Map("event" -> "ProductRegistered", "productId" -> e.id.value),
            JsObject(
              "productId" -> JsString(e.id.value),
              "name" -> JsString(e.name),
              "brand" -> opt(e.brand),
              "category" -> opt(e.category),
              "size" -> e.size.fold[JsValue](JsNull)(q =>
                JsObject(
                  "amount" -> JsString(q.amount.bigDecimal.stripTrailingZeros.toPlainString),
                  "unit" -> JsString(q.unit.toString)
                )
              ),
              "gtin" -> opt(e.gtin.map(_.value)),
              "status" -> JsString(statusName(e.status)),
              "origin" -> JsString(originName(e))
            )
          )
        )
      case e: ProductEvent.ProductMerged =>
        Some(
          out(
            topics.productRegistered,
            persistenceId,
            seqNr,
            correlationId,
            Map("event" -> "ProductMerged", "productId" -> id),
            JsObject(
              "productId" -> JsString(id),
              "status" -> JsString("MERGED_INTO"),
              "mergedInto" -> JsString(e.into.value)
            )
          )
        )
      case _ => None
    }
  }

  /**
   * `PriceObserved` — the one topic with a REQUIRED consumer (Demeter's deal evaluation).
   *
   * Two fields here are contract rather than decoration, and both were argued for in DESIGN:
   *
   *   - **scope** (§2.3.1). An area price speaks for every franchise of that chain in the region.
   *     Flattening it to one store under-counts coverage; expanding it to N stores fabricates N
   *     facts. Demeter cannot alert correctly without knowing which it has.
   *   - **sizeConfidence** (§2.3). Demeter's match confidence is the weakest link of its own split
   *     confidence and this one; omitting it would let downstream judgment read too high.
   */
  def price(
      topics: Topics,
      persistenceId: String,
      seqNr: Long,
      correlationId: String,
      event: PriceEvent
  ): Option[Outgoing] = event match {
    case e: PriceEvent.PriceObserved =>
      Some(
        out(
          topics.priceObserved,
          persistenceId,
          seqNr,
          correlationId,
          Map("event" -> "PriceObserved", "productId" -> e.productId.value),
          JsObject(
            "productId" -> JsString(e.productId.value),
            "scope" -> scopeJson(e.scope),
            "price" -> money(e.price.amount, e.price.currency.toString),
            "unitPrice" -> e.unitPrice.fold[JsValue](JsNull)(u =>
              JsObject(
                "amount" -> JsString(u.amount.bigDecimal.stripTrailingZeros.toPlainString),
                "currency" -> JsString(u.currency.toString),
                "per" -> JsString(u.per.toString)
              )
            ),
            "promo" -> e.promo.fold[JsValue](JsNull)(p =>
              JsObject(
                "description" -> JsString(p.description),
                "percentOff" -> p.percentOff.fold[JsValue](JsNull)(v =>
                  JsString(v.bigDecimal.stripTrailingZeros.toPlainString)
                )
              )
            ),
            "priceConfidence" -> JsNumber(e.priceConfidence.toDouble),
            "sizeConfidence" -> JsNumber(e.sizeConfidence.toDouble),
            "observedAt" -> JsString(e.observedAt.toString),
            "source" -> sourceJson(e.source)
          )
        )
      )
    // A retraction is a correction, and no consumer has been designed to handle one
    // yet (§2.3 says they are rare and manual). Publishing it into a topic whose only
    // consumer treats every message as a new price would make a correction look like
    // an observation — worse than not publishing it.
    case _: PriceEvent.PriceObservationRetracted => None
  }

  def purchase(
      topics: Topics,
      persistenceId: String,
      seqNr: Long,
      correlationId: String,
      event: PurchaseEvent
  ): Option[Outgoing] = event match {
    case e: PurchaseEvent.PurchaseRecorded =>
      Some(
        out(
          topics.purchaseRecorded,
          persistenceId,
          seqNr,
          correlationId,
          Map("event" -> "PurchaseRecorded"),
          JsObject(
            "purchaseId" -> JsString(e.id.value),
            "storeId" -> JsString(e.storeId.value),
            "purchasedAt" -> JsString(e.purchasedAt.toString),
            "lines" -> JsArray(
              e.lines
                .map(l =>
                  JsObject(
                    "productId" -> JsString(l.productId.value),
                    "quantity" -> JsString(l.quantity.bigDecimal.stripTrailingZeros.toPlainString),
                    "pricePaid" -> money(l.pricePaid.amount, l.pricePaid.currency.toString),
                    "lineTotal" -> money(l.lineTotal.amount, l.lineTotal.currency.toString)
                  )
                )
                .toVector
            ),
            "total" -> money(e.total.amount, e.total.currency.toString),
            "source" -> JsString(purchaseSourceName(e.source))
          )
        )
      )
    case _ => None
  }

  // ------------------------------------------------------------------ shapes

  /**
   * Scope as a TAGGED object, never a bare store id.
   *
   * `{"kind":"exact","storeId":...}` vs `{"kind":"area","chainId":...,"area":...}`. A consumer that
   * has to guess which it received will eventually guess wrong, and the wrong guess is silent — a
   * regional flyer claim recorded as one store's price reads as a perfectly normal observation.
   */
  private def scopeJson(scope: PriceScope): JsValue = scope match {
    case PriceScope.Exact(storeId) =>
      JsObject("kind" -> JsString("exact"), "storeId" -> JsString(storeId.value))
    case PriceScope.Regional(chainId, area) =>
      JsObject(
        "kind" -> JsString("area"),
        "chainId" -> JsString(chainId.value),
        "area" -> JsString(area.postalPrefix)
      )
  }

  /**
   * Money as a decimal STRING, never a float.
   *
   * A price is exact and a double is not; `4.99` has no binary representation, and a consumer
   * comparing prices for a deal would be comparing rounding artefacts.
   */
  private def money(amount: BigDecimal, currency: String): JsValue =
    JsObject(
      "amount" -> JsString(amount.bigDecimal.stripTrailingZeros.toPlainString),
      "currency" -> JsString(currency)
    )

  private def sourceJson(source: PriceSource): JsValue = source match {
    case PriceSource.Scrape(scraper, rawResponseId) =>
      JsObject(
        "kind" -> JsString("scrape"),
        "scraper" -> JsString(scraper),
        "rawResponseId" -> JsString(rawResponseId.toString)
      )
    case PriceSource.Purchase(purchaseId) =>
      JsObject("kind" -> JsString("purchase"), "purchaseId" -> JsString(purchaseId.value))
    case PriceSource.Manual => JsObject("kind" -> JsString("manual"))
    case PriceSource.Backfill(origin) =>
      JsObject("kind" -> JsString("backfill"), "origin" -> JsString(origin))
  }

  private def statusName(s: ProductStatus): String = s match {
    case ProductStatus.Provisional => "PROVISIONAL"
    case ProductStatus.Active => "ACTIVE"
    case _: ProductStatus.MergedInto => "MERGED_INTO"
    case ProductStatus.Deprecated => "DEPRECATED"
  }

  private def originName(e: ProductEvent.ProductRegistered): String = e.origin match {
    case me.cference.ariadne.domain.Origin.Manual => "MANUAL"
    case _: me.cference.ariadne.domain.Origin.Scrape => "SCRAPE"
    case _: me.cference.ariadne.domain.Origin.Migration => "MIGRATION"
  }

  private def purchaseSourceName(s: PurchaseSource): String = s match {
    case PurchaseSource.Manual => "MANUAL"
    case _: PurchaseSource.Receipt => "RECEIPT"
    case _: PurchaseSource.BankImport => "BANK_IMPORT"
  }

  private def opt(v: Option[String]): JsValue = v.fold[JsValue](JsNull)(JsString.apply)

  private def out(
      topic: String,
      persistenceId: String,
      seqNr: Long,
      correlationId: String,
      attributes: Map[String, String],
      payload: JsObject
  ): Outgoing =
    Outgoing(
      topic,
      messageId(persistenceId, seqNr),
      payload.compactPrint,
      attributes,
      correlationId
    )
}

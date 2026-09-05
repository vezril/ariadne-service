package me.cference.ariadne.http

import me.cference.ariadne.domain.MeasureUnit
import me.cference.ariadne.projection.ReadModelRepository.{
  CaseRow,
  ProductRow,
  ResolvedPrice,
  StoreRow
}
import me.cference.ariadne.resolver.StoreCandidate
import spray.json.*

/**
 * The browser-facing JSON shapes.
 *
 * These are the REST surface's own view types, NOT the domain's — the browser gets a flat, stable
 * shape, and the domain stays free to change behind it. That separation is why `candidates` arrives
 * as a parsed JsValue rather than a re-encoded string: the projection already stored what the
 * matcher offered, and re-deriving it here would risk showing something different from what the
 * decision was actually made against (§6.6).
 */
object JsonFormats extends DefaultJsonProtocol {

  final case class ProductView(
      id: String,
      name: String,
      brand: Option[String],
      category: Option[String],
      size: Option[String],
      status: String,
      mergedInto: Option[String]
  )

  final case class CaseView(
      id: String,
      subject: SubjectView,
      candidates: JsValue,
      parkedObservations: Int,
      createdAt: String
  )

  final case class SubjectView(
      name: String,
      brand: Option[String],
      gtin: Option[String],
      storeId: Option[String],
      externalId: Option[String]
  )

  final case class PriceView(
      amount: String,
      currency: String,
      observedAt: String,
      source: String,
      /**
       * `exact` or `area` — a caller must be able to tell a receipt from a flyer claim (§2.3.1).
       */
      scope: String,
      /** Convenience for a UI that only wants to badge it. */
      isExact: Boolean
  )

  final case class StoreView(
      id: String,
      name: String,
      chainId: String,
      area: String,
      label: Option[String],
      active: Boolean
  )

  /** A franchise the typed receipt text might mean, and why it is a candidate (§7.1). */
  final case class StoreMatchView(store: StoreView, score: Double, why: String)

  /**
   * The answer to "which store is this?".
   *
   * `unique` is stated EXPLICITLY rather than left for the caller to infer from `matches.length`. A
   * receipt normally says "Metro", which names a chain rather than a franchise, so several correct
   * matches is the ordinary case and not an error — but a caller that inferred uniqueness from a
   * list of one would auto-pick on a coincidence. Making the claim a field means the server says
   * whether the text actually narrowed.
   */
  final case class StoreResolutionView(
      query: String,
      unique: Boolean,
      matches: List[StoreMatchView]
  )

  /**
   * The answer to "which product is this?" (§6.4 Path B).
   *
   * `outcome` is one of `matched`, `ambiguous`, `no_match`, and ALL THREE are 200. A no-match is an
   * answer — the catalogue genuinely does not know this product — and returning 404 would make a
   * correct reply look like a broken request.
   *
   * `caseId` is present on `ambiguous`: the review case has been opened, and it is the handle a
   * caller confirms against once a human picks (§6.5).
   */
  final case class ProductResolutionView(
      outcome: String,
      productId: Option[String],
      confidence: Option[Double],
      method: Option[String],
      caseId: Option[String],
      candidates: List[CandidateView]
  )

  final case class CandidateView(productId: String, score: Double, notes: List[String])

  /**
   * Collections are returned in an ENVELOPE, never as a bare array.
   *
   * Two reasons. Practically, spray offers both `iterableFormat` and `linearSeqFormat` for a List,
   * which are ambiguous to Scala 3's implicit search. Design-wise, a top-level array has nowhere to
   * grow: paging or a total count would be a breaking change, and this is a published surface
   * ariadne-ui builds against.
   */
  final case class ProductsResponse(products: List[ProductView])
  final case class CasesResponse(cases: List[CaseView])
  final case class StoresResponse(stores: List[StoreView])

  final case class ErrorView(error: String)
  final case class DecisionRequest(
      productId: Option[String],
      winner: Option[String],
      loser: Option[String],
      listingStoreId: Option[String],
      listingExternalId: Option[String]
  )
  final case class RegisterProductRequest(
      name: String,
      brand: Option[String],
      category: Option[String],
      gtin: Option[String]
  )
  final case class AcceptedView(id: String, status: String)

  /**
   * `id` is OPTIONAL. A receipt flow does not have one to offer, so the server derives a stable id
   * from chain+area+name when it is absent — a retry of the same registration must not produce a
   * second franchise (the same reasoning as the provisional product id, §6.4.1).
   */
  final case class RegisterStoreRequest(
      id: Option[String],
      name: String,
      chainId: String,
      area: String,
      label: Option[String]
  )

  final case class ResolveProductRequest(
      name: String,
      brand: Option[String],
      gtin: Option[String],
      storeId: Option[String],
      externalId: Option[String]
  )

  given RootJsonFormat[ProductView] = jsonFormat7(ProductView.apply)
  given RootJsonFormat[SubjectView] = jsonFormat5(SubjectView.apply)
  given RootJsonFormat[CaseView] = jsonFormat5(CaseView.apply)
  given RootJsonFormat[PriceView] = jsonFormat6(PriceView.apply)
  given RootJsonFormat[ErrorView] = jsonFormat1(ErrorView.apply)
  given RootJsonFormat[DecisionRequest] = jsonFormat5(DecisionRequest.apply)
  given RootJsonFormat[RegisterProductRequest] = jsonFormat4(RegisterProductRequest.apply)
  given RootJsonFormat[AcceptedView] = jsonFormat2(AcceptedView.apply)
  given RootJsonFormat[StoreView] = jsonFormat6(StoreView.apply)
  given RootJsonFormat[StoreMatchView] = jsonFormat3(StoreMatchView.apply)
  given RootJsonFormat[StoreResolutionView] = jsonFormat3(StoreResolutionView.apply)
  given RootJsonFormat[CandidateView] = jsonFormat3(CandidateView.apply)
  given RootJsonFormat[ProductResolutionView] = jsonFormat6(ProductResolutionView.apply)
  given RootJsonFormat[RegisterStoreRequest] = jsonFormat5(RegisterStoreRequest.apply)
  given RootJsonFormat[ResolveProductRequest] = jsonFormat5(ResolveProductRequest.apply)

  given RootJsonFormat[ProductsResponse] = jsonFormat1(ProductsResponse.apply)
  given RootJsonFormat[CasesResponse] = jsonFormat1(CasesResponse.apply)
  given RootJsonFormat[StoresResponse] = jsonFormat1(StoresResponse.apply)

  def toProductView(r: ProductRow): ProductView =
    ProductView(
      r.id,
      r.name,
      r.brand,
      r.category,
      // The column stores the enum NAME ("Gram"), which is the right thing to persist —
      // stable, and independent of how anything displays it. The browser gets the label
      // ("g"): rendering "454 Gram" would leak an internal identifier into the UI.
      for { a <- r.sizeAmount; u <- r.sizeUnit } yield {
        val label = scala.util.Try(MeasureUnit.valueOf(u).label).getOrElse(u)
        s"${a.bigDecimal.stripTrailingZeros.toPlainString} $label"
      },
      r.status,
      r.mergedInto
    )

  def toCaseView(r: CaseRow): CaseView =
    CaseView(
      r.id,
      SubjectView(r.subjectName, r.subjectBrand, r.subjectGtin, r.subjectStore, r.subjectListing),
      // Parsed, not re-encoded: show exactly what the matcher offered.
      scala.util.Try(r.candidatesJson.parseJson).getOrElse(JsArray.empty),
      r.parkedCount,
      r.createdAt.toString
    )

  def toStoreView(r: StoreRow): StoreView =
    StoreView(r.id, r.name, r.chainId, r.area, r.label, r.active)

  def toStoreMatchView(c: StoreCandidate): StoreMatchView =
    // Rounded: the score is a ranking signal for a picker, and rendering 17 decimal
    // places of a heuristic invites a reader to believe it is a measurement.
    StoreMatchView(
      toStoreView(c.store),
      BigDecimal(c.score).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble,
      c.why
    )

  def toPriceView(p: ResolvedPrice): PriceView =
    PriceView(
      p.amount.stripTrailingZeros.toPlainString,
      p.currency,
      p.observedAt.toString,
      p.source,
      p.scopeKind,
      p.isExact
    )
}

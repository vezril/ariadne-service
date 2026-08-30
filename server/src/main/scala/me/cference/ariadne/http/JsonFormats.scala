package me.cference.ariadne.http

import me.cference.ariadne.domain.MeasureUnit
import me.cference.ariadne.projection.ReadModelRepository.{CaseRow, ProductRow, ResolvedPrice}
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

  given RootJsonFormat[ProductView] = jsonFormat7(ProductView.apply)
  given RootJsonFormat[SubjectView] = jsonFormat5(SubjectView.apply)
  given RootJsonFormat[CaseView] = jsonFormat5(CaseView.apply)
  given RootJsonFormat[PriceView] = jsonFormat6(PriceView.apply)
  given RootJsonFormat[ErrorView] = jsonFormat1(ErrorView.apply)
  given RootJsonFormat[DecisionRequest] = jsonFormat5(DecisionRequest.apply)
  given RootJsonFormat[RegisterProductRequest] = jsonFormat4(RegisterProductRequest.apply)
  given RootJsonFormat[AcceptedView] = jsonFormat2(AcceptedView.apply)

  given RootJsonFormat[ProductsResponse] = jsonFormat1(ProductsResponse.apply)
  given RootJsonFormat[CasesResponse] = jsonFormat1(CasesResponse.apply)

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

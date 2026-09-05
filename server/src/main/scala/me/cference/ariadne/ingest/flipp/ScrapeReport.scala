package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.ingest.http.IngestError

/**
 * Why an item did not become a price fact.
 *
 * Each reason is COUNTED SEPARATELY and that is the point, not bookkeeping. A single "dropped"
 * number would mean several different things at once — "we could not read it", "we read it
 * perfectly and chose not to keep it", "it never had a price to begin with" — and the second is
 * invisible inside the first. That conflation is the one this project keeps having to undo (Demeter
 * session, 2026-09-01, on zero-priced items).
 */
enum SkipReason(val label: String) {

  /** The item carries no price at all. Normal, not a failure: the offer lives in the artwork. */
  case NoPrice extends SkipReason("no price on the item")

  /**
   * A REAL 0.00 read out of the flyer — carrier handset promotions, measured at 3 rows in 35,088.
   * The number is true and using it as a price would be false, so it is refused. Counted under its
   * own reason so it reads as a category rather than as breakage.
   */
  case ZeroPriced extends SkipReason("zero-priced (contract or promotional)")

  /**
   * No usable name at all.
   *
   * Nothing downstream can identify a product from an empty string: the matcher scores TEXT, and a
   * provisional minted from "" would be one bucket every nameless item in the corpus falls into — a
   * single product accumulating unrelated prices, which reads as a real product with a very busy
   * price history.
   */
  case Nameless extends SkipReason("no usable name")

  /** The resolver could not identify the product and no provisional was created. */
  case Unresolved extends SkipReason("identity unresolved")

  /** Ambiguous identity: parked against a review case rather than attributed to a guess. */
  case ParkedForReview extends SkipReason("parked pending review")
}

/**
 * What one scrape run did. Every number here is meant to be read by a human deciding whether the
 * run was healthy, so the counts separate *kinds* of not-happening rather than totalling them.
 */
final case class ScrapeReport(
    runId: String,
    flyersListed: Int = 0,
    flyersSelected: Int = 0,
    flyersFetched: Int = 0,
    itemsDecoded: Int = 0,
    itemsDroppedByDecoder: Int = 0,
    observationsAppended: Int = 0,
    skipped: Map[SkipReason, Int] = Map.empty,
    failures: List[IngestError] = Nil
) {

  def skip(reason: SkipReason): ScrapeReport =
    copy(skipped = skipped.updated(reason, skipped.getOrElse(reason, 0) + 1))

  def fail(e: IngestError): ScrapeReport = copy(failures = failures :+ e)

  /**
   * The ledger's saving, which is the number that says whether quirk #2 is still working. If this
   * ever reads 1.0 the ledger has silently become a no-op — the exact failure the
   * timestamp-precision bug produced, and it looked like a completely successful run.
   */
  def selectionRatio: Option[Double] =
    Option.when(flyersListed > 0)(flyersSelected.toDouble / flyersListed.toDouble)

  def summary: String =
    s"run=$runId listed=$flyersListed selected=$flyersSelected fetched=$flyersFetched " +
      s"decoded=$itemsDecoded droppedByDecoder=$itemsDroppedByDecoder appended=$observationsAppended " +
      s"skipped={${skipped.map { case (r, n) => s"${r.label}=$n" }.mkString(", ")}} " +
      s"failures=${failures.size}" +
      selectionRatio.fold("")(r => f" selection=${r * 100}%.0f%%")
}

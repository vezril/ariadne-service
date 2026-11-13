package me.cference.ariadne.migrate

import me.cference.ariadne.domain.{CorrelationId, Gtin, MeasureUnit, Origin, ProductId, Quantity}
import me.cference.ariadne.domain.product.ProductCommand
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.resolver.{ResolutionOutcome, ResolutionService}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * Step 2 of `migration-demeter.md`: run every Demeter product through the resolver and emit the
 * product_key → ProductId mapping Demeter keeps for cutover.
 *
 * Three outcomes, and the interesting one is the middle:
 *
 *   - **matched** — the catalog already knows this product. Map to it, record the resolver's
 *     confidence and method so the cutover can audit the decision rather than trust it.
 *   - **registered** — the catalog has never seen it, so the backfill registers it with
 *     `Origin.Migration("demeter")`. The corpus IS the catalog for these; refusing to create them
 *     would leave 18.7k products' price history unattachable.
 *   - **ambiguous** — **no id, deliberately.** Resolution never picks (§6.4): several correct
 *     candidates and none unique is the ordinary case, and auto-picking here would silently
 *     attribute years of one product's price history to another. These park in the review queue
 *     with their case id recorded, and the migration doc already plans for it — the backfill
 *     "doubles as the matcher's tuning corpus", which only works if the hard cases are visible
 *     instead of guessed.
 *
 * IDEMPOTENT by construction, which matters because a 18.7k-row run will be restarted. Registration
 * uses the resolver's derived provisional id, so a second run re-finds rather than re-creates; the
 * mapping upserts on `product_key`.
 *
 * Deliberately NOT parallel. The resolver reads a projection it is also (via registration) writing,
 * and the read model is eventually consistent — two concurrent rows for near-identical names can
 * each see "no match" and create two products, which is the catalog-of-duplicates outcome the
 * resolver exists to prevent. A migration is a one-shot batch; its wall-clock is worth less than
 * its correctness.
 */
final class ProductBackfill(
    resolve: (MatchSubject, CorrelationId) => Future[ResolutionOutcome],
    register: (ProductId, ProductCommand) => Future[Either[String, Unit]],
    record: ProductBackfill.Mapping => Future[Unit]
)(using ec: ExecutionContext) {

  import ProductBackfill.*

  def run(
      rows: List[DemeterCsv.ProductRow],
      sourceName: String = "demeter",
      now: () => Instant = () => Instant.now()
  ): Future[Report] =
    rows.foldLeft(Future.successful(Report.empty)) { (prior, row) =>
      prior.flatMap(acc => one(row, sourceName, now()).map(acc.plus))
    }

  private def one(
      row: DemeterCsv.ProductRow,
      sourceName: String,
      at: Instant
  ): Future[Outcome] = {
    // The correlation id names the migration AND the key, so a question raised in the review
    // queue months later is traceable to the row that raised it.
    val correlation = CorrelationId(s"backfill:$sourceName:${row.productKey}")
    val subject = subjectOf(row)

    resolve(subject, correlation).flatMap {
      case ResolutionOutcome.Matched(productId, confidence, method) =>
        val m = Mapping(
          productKey = row.productKey,
          productId = Some(productId),
          outcome = "matched",
          confidence = Some(confidence.toDouble),
          matchMethod = Some(method.toString),
          resolutionCaseId = None,
          sourceName = sourceName,
          resolvedAt = at
        )
        record(m).map(_ => Outcome.Matched)

      case ResolutionOutcome.Ambiguous(_) =>
        val m = Mapping(
          productKey = row.productKey,
          productId = None,
          outcome = "ambiguous",
          confidence = None,
          matchMethod = None,
          // Derived from the subject, so it is the SAME case a later scrape of the same
          // product would open rather than a parallel duplicate (§6.5).
          resolutionCaseId = Some(ResolutionService.caseIdFor(subject).value),
          sourceName = sourceName,
          resolvedAt = at
        )
        record(m).map(_ => Outcome.Ambiguous)

      case ResolutionOutcome.NoMatch =>
        val id = ResolutionService.provisionalIdFor(subject)
        val cmd = ProductCommand.RegisterProduct(
          id = id,
          name = subject.name,
          brand = subject.brand,
          category = None,
          size = sizeOf(row),
          gtin = subject.gtin,
          // Migration, not Scrape: these products are arriving from Demeter's store rather
          // than from a flyer we fetched, and the origin is the only record of that.
          origin = Origin.Migration(sourceName),
          correlationId = correlation
        )
        register(id, cmd).flatMap { result =>
          // A refusal normally means "already registered", which is the wanted outcome on a
          // re-run — the id is derived, so it is the same product. It is recorded on the
          // mapping either way: a key that resolved to this id did so whether or not THIS
          // run was the one that created it.
          val m = Mapping(
            productKey = row.productKey,
            productId = Some(id),
            outcome = "registered",
            confidence = None,
            matchMethod = None,
            resolutionCaseId = None,
            sourceName = sourceName,
            resolvedAt = at
          )
          record(m).map(_ => if result.isRight then Outcome.Registered else Outcome.AlreadyPresent)
        }
    }
  }

  /**
   * What the resolver gets to match on.
   *
   * `raw_name` is preferred over the display names because it is what the matcher was tuned against
   * — Demeter's display names are already normalised by THEIR normaliser, and feeding a
   * twice-normalised string to ours would score differently than the same product will score when
   * it next arrives from a flyer.
   *
   * No `ListingKey`: Flipp item ids change weekly (§2.6 quirk #4), so a listing recorded from a
   * historical corpus is a key that stopped matching long ago.
   */
  private def subjectOf(row: DemeterCsv.ProductRow): MatchSubject =
    MatchSubject(
      name = row.rawName.trim,
      brand = row.brand.map(_.trim).filter(_.nonEmpty),
      // `parse`, not `unsafe`: a historical corpus will contain malformed barcodes, and a
      // GTIN is the resolver's STRONG key — a bad one would match confidently and wrongly.
      // An unparseable barcode degrades the row to fuzzy matching, which is the honest
      // outcome rather than an exception that aborts an 18.7k-row run.
      gtin = row.gtin.map(_.trim).filter(_.nonEmpty).flatMap(g => Gtin.parse(g).toOption),
      listing = None
    )

  /** Size survives only if both halves parse; half a size is not a size (§6.7). */
  private def sizeOf(row: DemeterCsv.ProductRow): Option[Quantity] =
    for {
      amount <- row.sizeQty
      unit <- row.sizeUnit.flatMap(u => ProductBackfill.unitOf(u))
      q <- Quantity(amount, unit).toOption
    } yield q
}

object ProductBackfill {

  final case class Mapping(
      productKey: String,
      productId: Option[ProductId],
      outcome: String,
      confidence: Option[Double],
      matchMethod: Option[String],
      resolutionCaseId: Option[String],
      sourceName: String,
      resolvedAt: Instant
  )

  enum Outcome {
    case Matched, Registered, AlreadyPresent, Ambiguous
  }

  /**
   * The run's own account of itself.
   *
   * `ambiguous` is the number to read first: it is the review-queue backlog the cutover inherits,
   * and the migration doc's "tuning corpus" is exactly this set.
   */
  final case class Report(
      matched: Int,
      registered: Int,
      alreadyPresent: Int,
      ambiguous: Int
  ) {
    def total: Int = matched + registered + alreadyPresent + ambiguous
    def plus(o: Outcome): Report = o match {
      case Outcome.Matched => copy(matched = matched + 1)
      case Outcome.Registered => copy(registered = registered + 1)
      case Outcome.AlreadyPresent => copy(alreadyPresent = alreadyPresent + 1)
      case Outcome.Ambiguous => copy(ambiguous = ambiguous + 1)
    }
    def summary: String =
      s"$total product keys: $matched matched, $registered registered, " +
        s"$alreadyPresent already present, $ambiguous ambiguous (parked for review)"
  }

  object Report {
    val empty: Report = Report(0, 0, 0, 0)
  }

  /**
   * Demeter's unit strings to ours.
   *
   * An unrecognised unit yields NO size rather than a guessed one. A wrong unit is worse than a
   * missing one: size is part of identity (§6.7), so mapping "oz" to Gram would make a 12 oz and a
   * 12 g the same product to the matcher.
   */
  private[migrate] def unitOf(raw: String): Option[MeasureUnit] =
    raw.trim.toLowerCase match {
      case "g" | "gram" | "grams" => Some(MeasureUnit.Gram)
      case "kg" | "kilogram" | "kilograms" => Some(MeasureUnit.Kilogram)
      case "ml" | "millilitre" | "milliliter" => Some(MeasureUnit.Millilitre)
      case "l" | "litre" | "liter" => Some(MeasureUnit.Litre)
      case "ea" | "each" | "ct" | "count" | "un" | "unit" => Some(MeasureUnit.Each)
      case _ => None
    }
}

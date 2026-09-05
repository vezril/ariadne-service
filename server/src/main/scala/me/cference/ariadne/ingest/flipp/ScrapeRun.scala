package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.domain.{ChainId, Confidence, ProductId}
import me.cference.ariadne.domain.resolution.MatchSubject
import me.cference.ariadne.ingest.http.{IngestError, PoliteFetcher}
import me.cference.ariadne.ingest.{ArchivedResponse, RawArchive, RawResponse}
import me.cference.ariadne.text.Locale
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/** Where to look and what a merchant maps to. Config, not domain state (§2.2). */
final case class ScrapeSource(
    name: String,
    baseUrl: String,
    postal: PostalCode,
    locale: Locale,
    /** Which Flipp merchant is which chain. A merchant with no mapping is skipped, not guessed. */
    chains: Map[MerchantId, ChainId]
)

/**
 * One scrape run: list flyers, decide which are worth fetching, fetch them, and turn what comes
 * back into price facts.
 *
 * The ORDER here is the design. Archive before parse, re-stamp before anything trusts an item's
 * merchant, and resolve identity before a price is attributed to a product. Each of those is a
 * place where a shortcut produces a run that looks entirely successful and leaves the corpus wrong.
 *
 * Everything effectful is injected, so the whole run is testable without a network or a scheduler.
 */
final class ScrapeRun(
    fetcher: PoliteFetcher,
    archive: RawArchive,
    ledger: PostgresFlyerLedger,
    resolve: (MatchSubject, String) => Future[Option[ProductId]],
    observe: (ProductId, FlippMapper.Observation) => Future[Unit],
    priceConfidence: Confidence = Confidence.Certain,
    sizeConfidence: Confidence = Confidence.Certain
)(using ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  def run(source: ScrapeSource, runId: String, now: Instant): Future[ScrapeReport] = {
    val listingUrl =
      s"${source.baseUrl}/flyers?locale=${source.locale.queryValue}&postal_code=${source.postal.canonical}"
    val start = ScrapeReport(runId)

    fetchAndArchive(source, runId, listingUrl, "flyers", now).flatMap {
      case Left(err) => Future.successful(start.fail(err))
      case Right(archived) =>
        FlippDecoders
          .parseJson(source.name, archived.body)
          .flatMap(FlippDecoders.decodeListing(source.name, _)) match {
          case Left(e) =>
            Future.successful(start.fail(IngestError.Transport(listingUrl, e.message)))
          case Right(listing) =>
            val listed = start.copy(flyersListed = listing.flyers.size)
            // Quirk #2: the ledger is what makes this affordable — ~18 of 164 on a
            // typical day, against ~9x the load without it.
            ledger.selectToFetch(listing.flyers, now).flatMap { selected =>
              val withSelection = listed.copy(flyersSelected = selected.size)
              if selected.size == listing.flyers.size && listing.flyers.size > 1 then
                log.warn(
                  "Ledger selected EVERY listed flyer ({}). If this persists the ledger is a no-op — " +
                    "check window comparison precision before it becomes 9x load on a bot-walled upstream.",
                  Integer.valueOf(selected.size)
                )
              sequentially(selected, withSelection)((report, flyer) =>
                fetchFlyer(source, runId, flyer, report, now)
              )
            }
        }
    }
  }

  private def fetchFlyer(
      source: ScrapeSource,
      runId: String,
      flyer: Flyer,
      report: ScrapeReport,
      now: Instant
  ): Future[ScrapeReport] = {
    val url =
      s"${source.baseUrl}/flyers/${flyer.id.value}?locale=${source.locale.queryValue}&postal_code=${source.postal.canonical}"
    fetchAndArchive(source, runId, url, "flyer_items", now).flatMap {
      case Left(err) => Future.successful(report.fail(err))
      case Right(archived) =>
        FlippDecoders
          .parseJson(source.name, archived.body)
          .flatMap(FlippDecoders.decodeItems(source.name, _)) match {
          case Left(e) => Future.successful(report.fail(IngestError.Transport(url, e.message)))
          case Right(parsed) =>
            // QUIRK #1 — the most dangerous step in the whole pipeline. Per-flyer
            // responses carry no merchant, so every item must be re-stamped from the
            // flyer that owned the response. Miss it and everything lands on merchant 0,
            // every product collides, and the row counts stay perfectly correct.
            val owned = MerchantRestamp(flyer, parsed.items)
            require(
              !MerchantRestamp.hasUnresolved(owned),
              s"merchant re-stamp did not take for flyer ${flyer.id.value} — refusing to continue; " +
                "unresolved merchants would collide every product into one identity"
            )
            val afterDecode = report.copy(
              flyersFetched = report.flyersFetched + 1,
              itemsDecoded = report.itemsDecoded + owned.size,
              itemsDroppedByDecoder = report.itemsDroppedByDecoder + parsed.dropped
            )
            source.chains.get(flyer.merchantId) match {
              case None =>
                // An unmapped merchant is skipped rather than guessed: inventing a chain
                // would attribute real prices to the wrong banner.
                log.warn(
                  "No chain mapping for merchant {} — skipping flyer {}",
                  flyer.merchantId.value,
                  flyer.id.value
                )
                markFetched(flyer, archived, now).map(_ => afterDecode)
              case Some(chain) =>
                sequentially(owned, afterDecode)((r, item) =>
                  observeItem(item, chain, source, archived.id, r)
                )
                  .flatMap(r => markFetched(flyer, archived, now).map(_ => r))
            }
        }
    }
  }

  private def observeItem(
      item: FlyerItem,
      chain: ChainId,
      source: ScrapeSource,
      rawResponseId: Long,
      report: ScrapeReport
  ): Future[ScrapeReport] = {
    val provenance = FlippMapper.Provenance(source.name, rawResponseId)
    FlippMapper.map(item, chain, source.postal, provenance, priceConfidence, sizeConfidence) match {
      case Left(reason) => Future.successful(report.skip(reason))
      case Right(obs) =>
        resolve(obs.subject, source.name).flatMap {
          // Ambiguous or unresolved identity does NOT become a price fact here. The
          // resolver parks it against a review case instead; attributing a price to a
          // guessed product is the one thing the whole resolver exists to prevent.
          case None => Future.successful(report.skip(SkipReason.ParkedForReview))
          case Some(productId) =>
            observe(productId, obs).map(_ =>
              report.copy(observationsAppended = report.observationsAppended + 1)
            )
        }
    }
  }

  private def markFetched(flyer: Flyer, archived: ArchivedResponse, now: Instant): Future[Unit] =
    ledger.markFetched(flyer.id, flyer.validFrom, flyer.validTo, archived.id, now)

  /**
   * Fetch, then ARCHIVE BEFORE ANYTHING PARSES (§2.6, gate G4).
   *
   * The type system already forbids decoding un-archived bytes — decoders take only
   * `ArchivedResponse` — so this is the only place raw bytes exist, and they exist for one
   * statement before they are kept.
   */
  private def fetchAndArchive(
      source: ScrapeSource,
      runId: String,
      url: String,
      kind: String,
      now: Instant
  ): Future[Either[IngestError, ArchivedResponse]] =
    fetcher.fetch(url, source.locale).flatMap {
      case Left(err) => Future.successful(Left(err))
      case Right(out) =>
        archive
          .archive(
            RawResponse(
              runId,
              source.name,
              kind,
              url,
              Some(source.postal.canonical),
              Some(source.locale.queryValue),
              now,
              out.contentType,
              out.body.getBytes("UTF-8")
            )
          )
          .map(Right(_))
    }

  /**
   * One at a time, deliberately.
   *
   * The rate limiter would space concurrent requests correctly, but sequential fetching keeps the
   * run's behaviour against an undocumented endpoint obvious rather than merely correct — and
   * politeness here is load-bearing, not tuning.
   */
  private def sequentially[A](items: List[A], zero: ScrapeReport)(
      step: (ScrapeReport, A) => Future[ScrapeReport]
  ): Future[ScrapeReport] =
    items.foldLeft(Future.successful(zero))((acc, a) => acc.flatMap(step(_, a)))
}

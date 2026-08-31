package me.cference.ariadne.ingest

import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.Future

/** A response as fetched, before anything has looked at it. */
final case class RawResponse(
    runId: String,
    source: String,
    kind: String,
    url: String,
    postalCode: Option[String],
    locale: Option[String],
    fetchedAt: Instant,
    contentType: String,
    body: Array[Byte]
) {
  def sha256: Array[Byte] = MessageDigest.getInstance("SHA-256").digest(body)
}

/**
 * A response that HAS been archived, carrying the id it was stored under.
 *
 * This type is the whole point of the module.
 *
 * §2.6 requires archive-before-parse, and a requirement enforced by discipline is one that gets
 * skipped at 2am during an incident. So the decoders do not accept `RawResponse` or `Array[Byte]` —
 * they accept only `ArchivedResponse`, which nothing but the archive can produce. Parsing
 * unarchived bytes is therefore not a mistake anyone can make; it is code that does not compile.
 *
 * The `id` is what a `PriceObserved` cites as its provenance, so any observation can be traced back
 * to the exact bytes it was derived from and re-derived if the decoder was wrong.
 */
final case class ArchivedResponse private[ingest] (
    id: Long,
    runId: String,
    source: String,
    kind: String,
    url: String,
    postalCode: Option[String],
    locale: Option[String],
    fetchedAt: Instant,
    contentType: String,
    body: Array[Byte]
) {
  def bodyString: String = new String(body, "UTF-8")
}

/**
 * Keeps the bytes.
 *
 * An interface rather than a concrete store, because where the bytes live is a deployment question
 * (Postgres today, possibly an Apollo tier later) while the guarantee — nothing is parsed before it
 * is kept — is a design one.
 */
trait RawArchive {

  /** Store the bytes and hand back the only thing a decoder will accept. */
  def archive(raw: RawResponse): Future[ArchivedResponse]

  /** Every response of a run, oldest first — the input to replay. */
  def replay(runId: String): Future[List[ArchivedResponse]]

  def get(id: Long): Future[Option[ArchivedResponse]]
}

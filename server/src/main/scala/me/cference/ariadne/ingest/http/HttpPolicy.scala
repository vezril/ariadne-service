package me.cference.ariadne.ingest.http

import me.cference.ariadne.text.Locale

import scala.concurrent.duration.*

/**
 * How Ariadne talks to an undocumented public endpoint politely and defensibly: timeout, retry with
 * full-jitter backoff, per-source rate limiting, realistic headers, bot-wall detection. All calls
 * behind this policy are GETs, so retries are always safe.
 *
 * THESE CONSTANTS ARE THE PORT'S ACCEPTANCE CRITERIA, not defaults to taste. The effect layer
 * around them is a rewrite (Scala 3 + Pekko against cats-effect + http4s), and a faithful-looking
 * rewrite that quietly changes one of these is a correct-looking port that behaves differently
 * against a bot-walled upstream — with nothing in a test written from the new code noticing.
 *
 * `rateWindow` in particular: Ariadne's own DESIGN recorded "4 requests per window" for weeks and
 * never recorded that the window is one minute. A rewrite reading only that would have shipped a
 * limiter four times too permissive and looked right.
 */
final case class HttpPolicyConfig(
    timeout: FiniteDuration = 30.seconds,
    maxAttempts: Int = 3,
    backoffBase: FiniteDuration = 1.second,
    backoffCap: FiniteDuration = 30.seconds,
    rateLimit: Int = 4, // requests per rateWindow, per source
    rateWindow: FiniteDuration = 1.minute,
    userAgent: String = HttpPolicyConfig.DefaultUserAgent,
    botWallSignatures: List[String] = HttpPolicyConfig.DefaultBotWallSignatures
)

object HttpPolicyConfig {
  val DefaultUserAgent: String =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

  /** Config-listed so a new challenge-page signature is an ops change, not a redeploy. */
  val DefaultBotWallSignatures: List[String] =
    List("cf-chl", "cf_chl", "challenge-platform", "captcha", "cf-turnstile")
}

/** What can go wrong talking to a source. */
enum IngestError(val message: String, val retriable: Boolean) {

  /**
   * A challenge page, or a 403. NON-RETRIABLE and deliberately so: retrying into a bot wall is how
   * a polite scraper becomes an impolite one, and the correct response is operator attention.
   */
  case BotWall(url: String, marker: String)
      extends IngestError(s"bot wall at $url (matched: $marker)", retriable = false)

  case Transport(url: String, cause: String)
      extends IngestError(s"transport failure at $url: $cause", retriable = true)
  case Status(url: String, status: Int)
      extends IngestError(s"unexpected status $status at $url", retriable = true)
  case Timeout(url: String) extends IngestError(s"timed out at $url", retriable = true)
}

/**
 * Pure backoff maths, split from the clock so it can be property-tested. PORTED VERBATIM.
 *
 * attempt n wait = min(cap, base * 2^(n-1)) * random_in[0,1] (full jitter)
 */
object Backoff {
  def wait(
      attempt: Int,
      base: FiniteDuration,
      cap: FiniteDuration,
      random: Double
  ): FiniteDuration = {
    require(attempt >= 1, s"attempt must be >= 1, got $attempt")
    val ceiling =
      math.min(cap.toNanos.toDouble, base.toNanos.toDouble * math.pow(2.0, (attempt - 1).toDouble))
    (ceiling * random).toLong.nanos
  }
}

/** PORTED VERBATIM. */
object BotWallDetection {

  /**
   * A 403, or any response whose body carries a known challenge signature, classifies as BotWall —
   * non-retriable, operator attention.
   *
   * Both halves matter: a challenge page can arrive with a 200, and a 403 can arrive with no
   * recognisable body at all. Checking only the status would miss the first; only the body, the
   * second.
   */
  def classify(
      status: Int,
      body: String,
      url: String,
      signatures: List[String]
  ): Option[IngestError.BotWall] = {
    val marker = signatures.find(body.contains)
    if status == 403 then Some(IngestError.BotWall(url, marker.getOrElse("http-403")))
    else marker.map(IngestError.BotWall(url, _))
  }
}

/** PORTED VERBATIM. */
object HeadersPolicy {
  def acceptLanguage(locale: Locale): String =
    locale match {
      case Locale.FrCa => "fr-CA,fr;q=0.9,en;q=0.5"
      case Locale.EnCa => "en-CA,en;q=0.9,fr;q=0.5"
    }

  def headers(config: HttpPolicyConfig, locale: Locale): Map[String, String] =
    Map(
      "User-Agent" -> config.userAgent,
      "Accept" -> "application/json",
      "Accept-Language" -> acceptLanguage(locale)
    )
}

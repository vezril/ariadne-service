package me.cference.ariadne.ingest.http

import me.cference.ariadne.text.Locale
import org.apache.pekko.actor.typed.ActorSystem

import java.time.Instant
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** One HTTP GET. A seam, so the politeness policy can be tested without a network. */
trait Transport {
  def get(url: String, headers: Map[String, String], timeout: FiniteDuration): Future[HttpOutcome]
}

final case class HttpOutcome(status: Int, body: String, contentType: String)

/**
 * The politeness policy applied around a transport: rate limit, realistic headers, retry with
 * full-jitter backoff, and bot-wall classification.
 *
 * All calls behind this are GETs, so retries are always safe.
 *
 * The rewritten half of the port. It carries almost no logic of its own on purpose — the scheduling
 * decision, the backoff maths and the bot-wall classification are all pure functions ported
 * verbatim, and this only sequences them. That is what makes the rewrite defensible: the parts that
 * could silently be wrong did not get rewritten.
 */
final class PoliteFetcher(
    transport: Transport,
    config: HttpPolicyConfig,
    limiter: RateLimiter,
    system: ActorSystem[?],
    random: () => Double = () => scala.util.Random.nextDouble()
) {

  private given ExecutionContext = system.executionContext

  def fetch(
      url: String,
      locale: Locale,
      now: () => Instant = () => Instant.now()
  ): Future[Either[IngestError, HttpOutcome]] =
    attempt(url, locale, 1, now)

  private def attempt(
      url: String,
      locale: Locale,
      n: Int,
      now: () => Instant
  ): Future[Either[IngestError, HttpOutcome]] =
    limiter.acquire().flatMap { _ =>
      transport
        .get(url, HeadersPolicy.headers(config, locale), config.timeout)
        .map { out =>
          // Bot wall first: a challenge page frequently arrives with a 200, so a
          // status-only check would parse the challenge as data.
          BotWallDetection.classify(out.status, out.body, url, config.botWallSignatures) match {
            case Some(wall) => Left(wall)
            case None if out.status / 100 == 2 => Right(out)
            case None => Left(IngestError.Status(url, out.status))
          }
        }
        .recover { case e =>
          Left(IngestError.Transport(url, Option(e.getMessage).getOrElse(e.toString)))
        }
        .flatMap {
          // A bot wall is never retried, whatever the attempt count says. Retrying into
          // one is how a polite scraper becomes an impolite one; it wants an operator.
          case Left(err) if err.retriable && n < config.maxAttempts =>
            val delay = Backoff.wait(n, config.backoffBase, config.backoffCap, random())
            after(delay).flatMap(_ => attempt(url, locale, n + 1, now))
          case settled => Future.successful(settled)
        }
    }

  private def after(delay: FiniteDuration): Future[Unit] =
    if delay <= Duration.Zero then Future.unit
    else {
      val p = scala.concurrent.Promise[Unit]()
      system.scheduler.scheduleOnce(delay, () => p.success(()))
      p.future
    }
}

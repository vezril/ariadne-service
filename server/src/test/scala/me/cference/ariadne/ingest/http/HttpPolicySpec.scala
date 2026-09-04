package me.cference.ariadne.ingest.http

import me.cference.ariadne.text.Locale
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

/**
 * The politeness policy. These are the port's acceptance criteria rather than defaults to taste, so
 * they are asserted literally — the surrounding code is a rewrite, and a rewrite that quietly
 * changes one of these looks correct and behaves differently against a bot-walled upstream.
 */
final class HttpPolicySpec extends AnyWordSpec with Matchers {

  "the constants" should {
    "match demeter's verbatim — every one of them" in {
      val c = HttpPolicyConfig()
      c.maxAttempts shouldBe 3
      c.backoffBase shouldBe 1.second
      c.backoffCap shouldBe 30.seconds
      c.rateLimit shouldBe 4
      // The one this project's own design omitted for weeks: "4 per window" is
      // meaningless without it, and a rewrite reading only the old doc would have
      // shipped a limiter four times too permissive and looked right.
      c.rateWindow shouldBe 1.minute
      c.timeout shouldBe 30.seconds
    }

    "carry the bot-wall SIGNATURE LIST, not merely a 403 check" in {
      HttpPolicyConfig().botWallSignatures should contain allOf
        ("cf-chl", "cf_chl", "challenge-platform", "captcha", "cf-turnstile")
    }
  }

  "Backoff" should {

    "be full-jitter: bounded above by min(cap, base * 2^(n-1)), and reach zero" in {
      // Full jitter, not equal jitter — the whole interval is available, which is what
      // decorrelates retries between concurrent callers.
      Backoff.wait(1, 1.second, 30.seconds, 1.0) shouldBe 1.second
      Backoff.wait(2, 1.second, 30.seconds, 1.0) shouldBe 2.seconds
      Backoff.wait(3, 1.second, 30.seconds, 1.0) shouldBe 4.seconds
      Backoff.wait(3, 1.second, 30.seconds, 0.0) shouldBe 0.nanos
    }

    "cap the ceiling, so a high attempt count cannot sleep for hours" in {
      Backoff.wait(20, 1.second, 30.seconds, 1.0) shouldBe 30.seconds
    }

    "reject attempt 0 rather than silently computing a half-interval" in {
      an[IllegalArgumentException] should be thrownBy Backoff.wait(0, 1.second, 30.seconds, 0.5)
    }
  }

  "BotWallDetection" should {

    val sigs = HttpPolicyConfig().botWallSignatures

    "classify a 403 even with an unrecognisable body" in {
      BotWallDetection.classify(403, "", "u", sigs).map(_.marker) shouldBe Some("http-403")
    }

    "classify a 200 carrying a challenge signature" in {
      // A challenge page arrives with a 200 more often than not. Checking status alone
      // would sail straight past it and parse the challenge as data.
      BotWallDetection
        .classify(200, "<div id=cf-chl-widget>", "u", sigs)
        .map(_.marker) shouldBe Some("cf-chl")
    }

    "prefer the body marker on a 403 that has one, so the operator sees WHICH wall" in {
      BotWallDetection.classify(403, "captcha here", "u", sigs).map(_.marker) shouldBe Some(
        "captcha"
      )
    }

    "not classify an ordinary response" in {
      BotWallDetection.classify(200, """{"items":[]}""", "u", sigs) shouldBe None
    }

    "be NON-RETRIABLE — retrying into a bot wall is how a polite scraper becomes impolite" in {
      IngestError.BotWall("u", "cf-chl").retriable shouldBe false
      IngestError.Transport("u", "reset").retriable shouldBe true
      IngestError.Status("u", 500).retriable shouldBe true
      IngestError.Timeout("u").retriable shouldBe true
    }
  }

  "HeadersPolicy" should {
    "send a locale-appropriate Accept-Language" in {
      HeadersPolicy.acceptLanguage(Locale.FrCa) shouldBe "fr-CA,fr;q=0.9,en;q=0.5"
      HeadersPolicy.acceptLanguage(Locale.EnCa) shouldBe "en-CA,en;q=0.9,fr;q=0.5"
      HeadersPolicy.headers(HttpPolicyConfig(), Locale.FrCa)("User-Agent") should include(
        "Mozilla/5.0"
      )
    }
  }

  "RateLimiter.plan — the pure scheduling decision" should {

    val limit = 4
    val window = 1.minute

    "let the first `limit` requests start immediately" in {
      val now = 0.nanos
      val (starts, _) = (1 to limit).foldLeft((Vector.empty[FiniteDuration], now)) {
        case ((acc, _), _) =>
          RateLimiter.plan(now, acc, limit, window)
      }
      starts should have size limit
      all(starts.map(_.toNanos)) shouldBe 0L
    }

    "delay the (limit+1)th to exactly one window after the FIRST" in {
      // The sliding property: not a fixed bucket that resets, but a window that moves.
      val now = 0.nanos
      var starts = Vector.empty[FiniteDuration]
      (1 to limit).foreach { _ =>
        val (next, _) = RateLimiter.plan(now, starts, limit, window)
        starts = next
      }
      val (_, fifth) = RateLimiter.plan(now, starts, limit, window)
      fifth shouldBe window
    }

    "forget starts that have aged out of the window" in {
      val old = Vector(0.nanos, 1.nanos, 2.nanos, 3.nanos)
      val now = window + 1.second
      val (kept, start) = RateLimiter.plan(now, old, limit, window)
      kept shouldBe Vector(now) // all four expired
      start shouldBe now
    }

    "never permit more than `limit` starts inside any window" in {
      // The property the whole class exists for, checked over a run rather than a case.
      val now = 0.nanos
      var starts = Vector.empty[FiniteDuration]
      val issued = (1 to 20).map { _ =>
        val (next, start) = RateLimiter.plan(now, starts, limit, window)
        starts = next
        start
      }
      issued.foreach { t =>
        issued.count(s => s >= t && s < t + window) should be <= limit
      }
    }
  }

  "RateLimiter.correct" should {
    "replace a planned start with the actual one" in {
      // Sleeps wake late. Without this, later reservations chain off a stale planned
      // time and the drift lets an extra request slip into a window.
      RateLimiter.correct(Vector(1.nanos, 5.nanos), 5.nanos, 7.nanos) shouldBe Vector(
        1.nanos,
        7.nanos
      )
    }
    "leave the vector alone when the planned start is not recorded" in {
      RateLimiter.correct(Vector(1.nanos), 9.nanos, 11.nanos) shouldBe Vector(1.nanos)
    }
  }
}

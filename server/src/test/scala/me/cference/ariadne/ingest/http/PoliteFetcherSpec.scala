package me.cference.ariadne.ingest.http

import me.cference.ariadne.text.Locale
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
 * The rewritten half of the port, tested for the behaviours that protect a bot-walled upstream. No
 * network: the transport is a seam, so these are deterministic.
 */
final class PoliteFetcherSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers {

  private given scala.concurrent.ExecutionContext = system.executionContext

  private val fastConfig = HttpPolicyConfig(backoffBase = 1.milli, backoffCap = 5.millis)

  private def fetcher(t: Transport, config: HttpPolicyConfig = fastConfig) =
    new PoliteFetcher(
      t,
      config,
      RateLimiter(config.rateLimit, config.rateWindow, system),
      system,
      random = () => 1.0 // deterministic jitter
    )

  private class Stub(outcomes: List[Either[Throwable, HttpOutcome]]) extends Transport {
    val calls = new AtomicInteger(0)
    def get(
        url: String,
        headers: Map[String, String],
        timeout: FiniteDuration
    ): Future[HttpOutcome] = {
      val i = calls.getAndIncrement()
      outcomes.lift(i).getOrElse(outcomes.last) match {
        case Right(o) => Future.successful(o)
        case Left(e) => Future.failed(e)
      }
    }
  }

  private def ok(body: String = """{"items":[]}""") = Right(
    HttpOutcome(200, body, "application/json")
  )

  "a successful fetch" should {
    "return the body and call the transport once" in {
      val stub = new Stub(List(ok()))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue shouldBe Right(
        HttpOutcome(200, """{"items":[]}""", "application/json")
      )
      stub.calls.get() shouldBe 1
    }
  }

  "a bot wall" should {

    "NEVER be retried, however many attempts remain" in {
      // Retrying into a bot wall is how a polite scraper becomes an impolite one. This
      // is the single most important behaviour in the file.
      val stub = new Stub(List(Right(HttpOutcome(403, "", "text/html"))))
      val result = fetcher(stub).fetch("u", Locale.EnCa).futureValue
      result.left.map(_.getClass.getSimpleName) shouldBe Left("BotWall")
      stub.calls.get() shouldBe 1
    }

    "be detected on a 200 carrying a challenge signature" in {
      // Challenge pages routinely arrive with a 200. A status-only check would parse
      // the challenge as data.
      val stub = new Stub(List(Right(HttpOutcome(200, "<div class=cf-turnstile>", "text/html"))))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue match {
        case Left(w: IngestError.BotWall) => w.marker shouldBe "cf-turnstile"
        case other => fail(s"expected a bot wall, got $other")
      }
      stub.calls.get() shouldBe 1
    }
  }

  "a retriable failure" should {

    "be retried up to maxAttempts and then give up" in {
      val stub = new Stub(List(Right(HttpOutcome(500, "boom", "text/plain"))))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue.isLeft shouldBe true
      stub.calls.get() shouldBe 3 // maxAttempts, not maxAttempts + 1
    }

    "stop as soon as it succeeds" in {
      val stub = new Stub(List(Right(HttpOutcome(503, "", "")), ok()))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue.isRight shouldBe true
      stub.calls.get() shouldBe 2
    }

    "treat a transport exception as retriable rather than fatal" in {
      val stub = new Stub(List(Left(new RuntimeException("connection reset")), ok()))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue.isRight shouldBe true
      stub.calls.get() shouldBe 2
    }

    "surface the underlying cause rather than swallowing it" in {
      val stub = new Stub(List(Left(new RuntimeException("connection reset"))))
      fetcher(stub).fetch("u", Locale.EnCa).futureValue match {
        case Left(e: IngestError.Transport) => e.message should include("connection reset")
        case other => fail(s"unexpected: $other")
      }
    }
  }

  "the headers" should {
    "carry a realistic user agent and a locale-appropriate Accept-Language" in {
      val captured = new java.util.concurrent.atomic.AtomicReference[Map[String, String]](Map.empty)
      val t = new Transport {
        def get(
            url: String,
            headers: Map[String, String],
            timeout: FiniteDuration
        ): Future[HttpOutcome] = {
          captured.set(headers)
          Future.successful(HttpOutcome(200, "{}", "application/json"))
        }
      }
      fetcher(t).fetch("u", Locale.FrCa).futureValue
      captured.get()("Accept-Language") shouldBe "fr-CA,fr;q=0.9,en;q=0.5"
      captured.get()("User-Agent") should include("Mozilla/5.0")
    }
  }

  "the rate limiter" should {
    "actually delay the request beyond the configured limit" in {
      // The politeness guarantee, end to end rather than only in `plan`.
      val tight = HttpPolicyConfig(rateLimit = 2, rateWindow = 300.millis, backoffBase = 1.milli)
      val stub = new Stub(List(ok()))
      val f = fetcher(stub, tight)
      val started = System.nanoTime()
      val all = Future.sequence((1 to 3).map(_ => f.fetch("u", Locale.EnCa)))
      all.futureValue.foreach(_.isRight shouldBe true)
      val elapsed = (System.nanoTime() - started).nanos
      // Three requests, limit two per 300ms: the third cannot start before the window.
      elapsed should be >= 250.millis
    }
  }
}

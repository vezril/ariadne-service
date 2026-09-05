package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.domain.ChainId
import me.cference.ariadne.text.Locale
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

/**
 * The scheduler has two jobs beyond "run on a timer", and both are about not being rude to an
 * upstream that can and does fight back.
 */
final class ScrapeSchedulerSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with Eventually {

  private given scala.concurrent.ExecutionContext = system.executionContext

  private val source = ScrapeSource(
    "flipp",
    "https://flipp.test",
    PostalCode.unsafe("H2X1Y4"),
    Locale.EnCa,
    Map(MerchantId(42) -> ChainId("iga"))
  )

  "the scheduler" should {

    "not start a second run while the first is still going" in {
      // The scenario that matters: a run slower than the interval. A fixed-rate timer
      // would overlap them, and two concurrent runs of one source double the request
      // rate against a bot-walled endpoint — the limiter is per-run, so nothing else
      // would catch it.
      val started = new AtomicInteger(0)
      val release = Promise[Unit]()
      val run: (ScrapeSource, String, Instant) => Future[ScrapeReport] = (_, id, _) => {
        started.incrementAndGet()
        release.future.map(_ => ScrapeReport(id))
      }

      spawn(ScrapeScheduler(source, run, interval = 10.millis, initialDelay = 1.milli))

      // Many intervals' worth of time passes while the first run is still in flight.
      Thread.sleep(300)
      started.get() shouldBe 1

      release.success(())
      // Only once it finishes does the next one become due.
      eventually(timeout(5.seconds))(started.get() should be > 1)
    }

    "keep scraping after a run fails" in {
      // An upstream outage is a normal Tuesday. A scheduler that died on one would turn
      // a bad afternoon into an outage lasting until a human noticed.
      val attempts = new AtomicInteger(0)
      val run: (ScrapeSource, String, Instant) => Future[ScrapeReport] = (_, id, _) =>
        if attempts.incrementAndGet() == 1 then Future.failed(new RuntimeException("upstream down"))
        else Future.successful(ScrapeReport(id))

      spawn(ScrapeScheduler(source, run, interval = 10.millis, initialDelay = 1.milli))
      eventually(timeout(5.seconds))(attempts.get() should be >= 3)
    }

    "give each run its own id, so archived responses are separable" in {
      // Runs share the archive. A reused id would make replay of "that run" return
      // every run's bytes mixed together.
      val ids = new AtomicReference(List.empty[String])
      val run: (ScrapeSource, String, Instant) => Future[ScrapeReport] = (_, id, _) => {
        ids.updateAndGet(id :: _)
        Future.successful(ScrapeReport(id))
      }

      spawn(ScrapeScheduler(source, run, interval = 10.millis, initialDelay = 1.milli))
      eventually(timeout(5.seconds))(ids.get().size should be >= 3)
      val seen = ids.get()
      seen.distinct.size shouldBe seen.size
    }
  }
}

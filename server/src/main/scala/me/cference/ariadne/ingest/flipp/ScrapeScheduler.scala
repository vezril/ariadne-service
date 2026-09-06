package me.cference.ariadne.ingest.flipp

import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.sharding.typed.ShardedDaemonProcessSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.ShardedDaemonProcess

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
 * The scraping policy's clock — "whenever schedule due then ObservePrice" (DESIGN §2.3), one
 * scheduler per configured source.
 *
 * Two properties matter more than the scheduling itself:
 *
 *   1. **The next run is armed when the previous one FINISHES, not on a fixed period.** A run is
 *      rate-limited to 4 requests per minute against an upstream that may be slow, so its duration
 *      is not something the config can promise. A fixed-rate timer would eventually overlap runs,
 *      and two concurrent runs of the same source would double the request rate against a
 *      bot-walled endpoint — the exact thing the limiter exists to prevent, arriving by way of the
 *      scheduler.
 *   1. **A failed run is logged and rescheduled, never fatal.** An upstream outage is a normal
 *      Tuesday; a scheduler that dies on it would stop scraping until someone noticed.
 */
object ScrapeScheduler {

  sealed trait Command
  private case object Tick extends Command
  final private case class Finished(report: Option[ScrapeReport], error: Option[Throwable])
      extends Command

  def apply(
      source: ScrapeSource,
      run: (ScrapeSource, String, Instant) => scala.concurrent.Future[ScrapeReport],
      interval: FiniteDuration,
      initialDelay: FiniteDuration
  ): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        timers.startSingleTimer(Tick, initialDelay)
        ctx.log.info(
          "Scrape scheduler for source '{}' armed — first run in {}, then every {}",
          source.name,
          initialDelay.toString,
          interval.toString
        )
        idle(source, run, interval, timers)
      }
    }

  private def idle(
      source: ScrapeSource,
      run: (ScrapeSource, String, Instant) => scala.concurrent.Future[ScrapeReport],
      interval: FiniteDuration,
      timers: TimerScheduler[Command]
  ): Behavior[Command] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case Tick =>
          val runId = UUID.randomUUID().toString
          ctx.log.info("Scrape run {} starting for source '{}'", runId, source.name)
          ctx.pipeToSelf(run(source, runId, Instant.now())) {
            case Success(report) => Finished(Some(report), None)
            case Failure(e) => Finished(None, Some(e))
          }
          Behaviors.same

        case Finished(report, error) =>
          report.foreach(r => ctx.log.info("Scrape run finished — {}", r.summary))
          error.foreach(e =>
            // Not fatal: the source is retried on the next tick. A scheduler that
            // stopped here would turn one bad afternoon into an indefinite outage.
            ctx.log.error(s"Scrape run for source '${source.name}' failed — ${e.getMessage}", e)
          )
          timers.startSingleTimer(Tick, interval)
          Behaviors.same
      }
    }

  /**
   * Start one scheduler per source under `ShardedDaemonProcess`.
   *
   * Singleton-per-source is the point, not distribution. The rate limiter is per-JVM, so a second
   * node running the same source would double the request rate with neither instance able to see
   * it. `ShardedDaemonProcess` keeps exactly one alive cluster-wide and restarts it if its node
   * dies — the same mechanism the projections already run under.
   */
  def init(
      sources: List[ScrapeSource],
      run: (ScrapeSource, String, Instant) => scala.concurrent.Future[ScrapeReport],
      interval: FiniteDuration,
      initialDelay: FiniteDuration
  )(using system: org.apache.pekko.actor.typed.ActorSystem[?]): Unit =
    if sources.nonEmpty then
      ShardedDaemonProcess(system).init(
        "scrape-source",
        sources.size,
        i => ScrapeScheduler(sources(i), run, interval, initialDelay),
        ShardedDaemonProcessSettings(system),
        None
      )
}

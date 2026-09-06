package me.cference.ariadne.ingest.http

import org.apache.pekko.actor.typed.ActorSystem

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Sliding-window rate limiter.
 *
 * The SCHEDULING DECISION is a pure function of (now, recorded starts) and ports verbatim from
 * demeter-service — `plan` and `correct` below are their code. Only the effectful wrapper is
 * rebuilt, cats-effect `Ref` + `Temporal.sleep` becoming an `AtomicReference` + the Pekko
 * scheduler.
 *
 * That split is theirs and it is the reason the rewrite is safe: the part that could silently be
 * wrong is pure and tested, and the part that is rewritten has almost no logic in it.
 */
final class RateLimiter private (limit: Int, window: FiniteDuration, system: ActorSystem[?]) {

  private val state = new AtomicReference[Vector[FiniteDuration]](Vector.empty)
  private given ExecutionContext = system.executionContext

  private def monotonic: FiniteDuration = System.nanoTime().nanos

  def acquire(): Future[Unit] = {
    val now = monotonic
    val planned = reserve(now)
    val delay = planned - now
    val slept =
      if delay <= Duration.Zero then Future.unit
      else {
        val p = Promise[Unit]()
        system.scheduler.scheduleOnce(delay, () => p.success(()))
        p.future
      }
    slept.map { _ =>
      // Write back the ACTUAL start, not the planned one.
      //
      // Sleeps wake a few milliseconds late. If later reservations chained off the stale
      // planned time, that drift would accumulate and let an extra request slip into a
      // window — which against a bot-walled upstream is the difference between polite
      // and noticed. Demeter's comment, and the reason `correct` exists at all.
      val actual = monotonic
      if actual > planned then state.updateAndGet(RateLimiter.correct(_, planned, actual))
      ()
    }
  }

  /** Atomic compare-and-set around the pure `plan`. */
  private def reserve(now: FiniteDuration): FiniteDuration = {
    var planned: FiniteDuration = now
    var done = false
    while !done do {
      val current = state.get()
      val (next, start) = RateLimiter.plan(now, current, limit, window)
      if state.compareAndSet(current, next) then {
        planned = start
        done = true
      }
    }
    planned
  }
}

object RateLimiter {

  def apply(limit: Int, window: FiniteDuration, system: ActorSystem[?]): RateLimiter =
    new RateLimiter(limit, window, system)

  /**
   * PORTED VERBATIM. Given the current time and the already-recorded start times, returns the
   * absolute time the next request may start and the updated start list. Guarantees no more than
   * `limit` starts within any `window`.
   */
  def plan(
      now: FiniteDuration,
      starts: Vector[FiniteDuration],
      limit: Int,
      window: FiniteDuration
  ): (Vector[FiniteDuration], FiniteDuration) = {
    val active = starts.filter(_ + window > now).sorted
    val start = if active.size < limit then now else active(active.size - limit) + window
    (active :+ start, start)
  }

  /**
   * PORTED VERBATIM. Replace one recorded planned start with the time the request actually began.
   */
  def correct(
      starts: Vector[FiniteDuration],
      planned: FiniteDuration,
      actual: FiniteDuration
  ): Vector[FiniteDuration] =
    starts.indexOf(planned) match {
      case -1 => starts
      case i => starts.updated(i, actual)
    }
}

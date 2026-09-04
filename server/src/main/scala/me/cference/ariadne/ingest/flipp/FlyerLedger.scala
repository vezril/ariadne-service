package me.cference.ariadne.ingest.flipp

import io.r2dbc.spi.{ConnectionFactory, Row}
import org.reactivestreams.Publisher

import java.time.Instant
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

final case class LedgerEntry(
    flyerId: FlyerId,
    windowFrom: Instant,
    windowTo: Instant,
    fetchedAt: Instant
)

object FlyerLedger {

  /** Demeter's default, carried: a week. */
  val DefaultMaxAge: FiniteDuration = 7.days

  /**
   * THE SELECTION RULE — pure, and PORTED VERBATIM.
   *
   * Fetch iff never fetched, OR the validity window changed (a re-issued flyer), OR the recorded
   * fetch is older than `maxAge`.
   *
   * This is quirk #2 and it is what makes the pipeline affordable: ~18 of 164 listed flyers on a
   * typical day, against roughly 9x the load without it, against an upstream that bot-walls.
   */
  def needsFetch(
      flyer: Flyer,
      recorded: Option[LedgerEntry],
      now: Instant,
      maxAge: FiniteDuration
  ): Boolean =
    recorded match {
      case None => true
      case Some(entry) =>
        val windowChanged = entry.windowFrom != flyer.validFrom || entry.windowTo != flyer.validTo
        val stale = entry.fetchedAt.plusMillis(maxAge.toMillis).isBefore(now)
        windowChanged || stale
    }
}

/**
 * The ledger, on r2dbc. The effectful half — doobie becomes r2dbc — while `needsFetch` above is the
 * decision and is pure.
 */
final class PostgresFlyerLedger(
    cf: ConnectionFactory,
    maxAge: FiniteDuration = FlyerLedger.DefaultMaxAge
)(using
    ec: ExecutionContext
) {

  /** Which of today's listed flyers are worth the expensive items call. */
  def selectToFetch(listing: List[Flyer], now: Instant): Future[List[Flyer]] =
    entriesFor(listing.map(_.id)).map { recorded =>
      listing.filter(f => FlyerLedger.needsFetch(f, recorded.get(f.id), now, maxAge))
    }

  /**
   * Record a completed fetch.
   *
   * Upsert on `flyer_id` alone — NOT on the window. A re-issued flyer must UPDATE its row, not add
   * a second one, or the selection lookup gains an ambiguity it has no rule for.
   */
  def markFetched(
      flyerId: FlyerId,
      windowFrom: Instant,
      windowTo: Instant,
      rawResponseId: Long,
      at: Instant
  ): Future[Unit] =
    exec(
      """INSERT INTO flyer_fetch_ledger (flyer_id, window_from, window_to, fetched_at, raw_response_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (flyer_id) DO UPDATE SET
           window_from = EXCLUDED.window_from,
           window_to = EXCLUDED.window_to,
           fetched_at = EXCLUDED.fetched_at,
           raw_response_id = EXCLUDED.raw_response_id""",
      java.lang.Long.valueOf(flyerId.value),
      windowFrom,
      windowTo,
      at,
      java.lang.Long.valueOf(rawResponseId)
    )

  def entriesFor(ids: List[FlyerId]): Future[Map[FlyerId, LedgerEntry]] =
    if ids.isEmpty then Future.successful(Map.empty)
    else {
      val placeholders = ids.indices.map(i => s"$$${i + 1}").mkString(",")
      query(
        s"SELECT flyer_id, window_from, window_to, fetched_at FROM flyer_fetch_ledger WHERE flyer_id IN ($placeholders)",
        (r: Row) =>
          LedgerEntry(
            FlyerId(r.get(0, classOf[java.lang.Long]).longValue),
            r.get(1, classOf[Instant]),
            r.get(2, classOf[Instant]),
            r.get(3, classOf[Instant])
          )
      )(ids.map(i => java.lang.Long.valueOf(i.value))*).map(_.map(e => e.flyerId -> e).toMap)
    }

  // --- plumbing -----------------------------------------------------------

  private def exec(sql: String, args: Any*): Future[Unit] =
    withConnection { conn =>
      val st = conn.createStatement(sql)
      bind(st, args)
      collect(st.execute(), (_: Row) => ()).map(_ => ())
    }

  private def query[A](sql: String, map: Row => A)(args: Any*): Future[List[A]] =
    withConnection { conn =>
      val st = conn.createStatement(sql)
      bind(st, args)
      collect(st.execute(), map)
    }

  private def bind(st: io.r2dbc.spi.Statement, args: Seq[Any]): Unit =
    args.zipWithIndex.foreach {
      case (null, i) => st.bindNull(i, classOf[Object])
      case (v, i) => st.bind(i, v.asInstanceOf[Object])
    }

  private def withConnection[A](f: io.r2dbc.spi.Connection => Future[A]): Future[A] =
    single(cf.create()).flatMap { conn =>
      val result = f(conn)
      result.transformWith(r => single(conn.close()).transform(_ => r))
    }

  private def collect[A](p: Publisher[? <: io.r2dbc.spi.Result], map: Row => A): Future[List[A]] =
    single[io.r2dbc.spi.Result](p.asInstanceOf[Publisher[io.r2dbc.spi.Result]])
      .flatMap(res => drain(res.map((row, _) => map(row))))

  private def single[A](p: Publisher[A]): Future[A] = {
    val promise = Promise[A]()
    p.subscribe(new org.reactivestreams.Subscriber[A] {
      private var value: Option[A] = None
      def onSubscribe(s: org.reactivestreams.Subscription): Unit = s.request(Long.MaxValue)
      def onNext(a: A): Unit = if value.isEmpty then value = Some(a)
      def onError(t: Throwable): Unit = promise.tryFailure(t)
      def onComplete(): Unit = promise.trySuccess(value.getOrElse(null.asInstanceOf[A]))
    })
    promise.future
  }

  private def drain[A](p: Publisher[A]): Future[List[A]] = {
    val promise = Promise[List[A]]()
    val buf = scala.collection.mutable.ListBuffer.empty[A]
    p.subscribe(new org.reactivestreams.Subscriber[A] {
      def onSubscribe(s: org.reactivestreams.Subscription): Unit = s.request(Long.MaxValue)
      def onNext(a: A): Unit = buf += a
      def onError(t: Throwable): Unit = promise.tryFailure(t)
      def onComplete(): Unit = promise.trySuccess(buf.toList)
    })
    promise.future
  }
}

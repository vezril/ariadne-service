package me.cference.ariadne.ingest

import io.r2dbc.spi.{ConnectionFactory, Row}
import org.reactivestreams.Publisher

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * The archive, in Ariadne's own Postgres — the same database as the journal.
 *
 * Deliberately the simplest thing that keeps the guarantee. This sits on the ingest critical path
 * (nothing parses before it returns), so it has no dependency a scrape run could be blocked by.
 */
final class PostgresRawArchive(cf: ConnectionFactory)(using ec: ExecutionContext)
    extends RawArchive {

  def archive(raw: RawResponse): Future[ArchivedResponse] =
    withConnection { conn =>
      val st = conn
        .createStatement(
          """INSERT INTO raw_response
             (run_id, source, kind, url, postal_code, locale, fetched_at, content_type, body, body_sha256)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
             RETURNING id"""
        )
      bind(
        st,
        Seq(
          raw.runId,
          raw.source,
          raw.kind,
          raw.url,
          raw.postalCode.orNull,
          raw.locale.orNull,
          raw.fetchedAt,
          raw.contentType,
          raw.body,
          raw.sha256
        )
      )
      first(st.execute(), r => r.get(0, classOf[java.lang.Long]).longValue).map { id =>
        ArchivedResponse(
          id,
          raw.runId,
          raw.source,
          raw.kind,
          raw.url,
          raw.postalCode,
          raw.locale,
          raw.fetchedAt,
          raw.contentType,
          raw.body
        )
      }
    }

  def replay(runId: String): Future[List[ArchivedResponse]] =
    withConnection { conn =>
      val st = conn.createStatement(
        """SELECT id, run_id, source, kind, url, postal_code, locale, fetched_at, content_type, body
           FROM raw_response WHERE run_id = $1 ORDER BY id"""
      )
      bind(st, Seq(runId))
      collect(st.execute(), rowToArchived)
    }

  def get(id: Long): Future[Option[ArchivedResponse]] =
    withConnection { conn =>
      val st = conn.createStatement(
        """SELECT id, run_id, source, kind, url, postal_code, locale, fetched_at, content_type, body
           FROM raw_response WHERE id = $1"""
      )
      bind(st, Seq(java.lang.Long.valueOf(id)))
      collect(st.execute(), rowToArchived).map(_.headOption)
    }

  private val rowToArchived: Row => ArchivedResponse = r =>
    ArchivedResponse(
      r.get(0, classOf[java.lang.Long]).longValue,
      r.get(1, classOf[String]),
      r.get(2, classOf[String]),
      r.get(3, classOf[String]),
      r.get(4, classOf[String]),
      Option(r.get(5, classOf[String])),
      Option(r.get(6, classOf[String])),
      r.get(7, classOf[Instant]),
      r.get(8, classOf[String]),
      r.get(9, classOf[Array[Byte]])
    )

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

  private def first[A](p: Publisher[? <: io.r2dbc.spi.Result], map: Row => A): Future[A] =
    collect(p, map).map(
      _.headOption.getOrElse(throw new IllegalStateException("archive insert returned no id"))
    )

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

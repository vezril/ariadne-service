package me.cference.ariadne.projection

import io.r2dbc.spi.{ConnectionFactory, Result, Row}
import me.cference.ariadne.domain.*
import org.reactivestreams.Publisher

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

/**
 * The read models' SQL, in one place.
 *
 * Owns its own connection rather than writing through the projection's session (Apollo's
 * precedent): every statement here is idempotent, which makes at-least-once delivery observably
 * equivalent to exactly-once without a shared transaction.
 */
final class ReadModelRepository(cf: ConnectionFactory)(using ec: ExecutionContext) {

  import ReadModelRepository.*

  // ---------------------------------------------------------------- products

  def upsertProduct(
      id: String,
      name: String,
      brand: Option[String],
      category: Option[String],
      size: Option[Quantity],
      status: String,
      mergedInto: Option[String]
  ): Future[Unit] =
    exec(
      """INSERT INTO products (id, name, brand, category, size_amount, size_unit, status, merged_into)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO UPDATE SET
           name = EXCLUDED.name, brand = EXCLUDED.brand, category = EXCLUDED.category,
           size_amount = EXCLUDED.size_amount, size_unit = EXCLUDED.size_unit,
           status = EXCLUDED.status, merged_into = EXCLUDED.merged_into,
           updated_at = now()""",
      id,
      name,
      brand.orNull,
      category.orNull,
      size.map(_.amount.bigDecimal).orNull,
      size.map(_.unit.toString).orNull,
      status,
      mergedInto.orNull
    )

  def setProductStatus(id: String, status: String, mergedInto: Option[String]): Future[Unit] =
    exec(
      "UPDATE products SET status = $1, merged_into = $2, updated_at = now() WHERE id = $3",
      status,
      mergedInto.orNull,
      id
    )

  def addGtin(gtin: String, productId: String): Future[Unit] =
    exec(
      "INSERT INTO product_gtins (gtin, product_id) VALUES ($1, $2) ON CONFLICT (gtin) DO UPDATE SET product_id = EXCLUDED.product_id",
      gtin,
      productId
    )

  def addAlias(productId: String, alias: String): Future[Unit] =
    exec(
      "INSERT INTO product_aliases (product_id, alias) VALUES ($1, $2) ON CONFLICT DO NOTHING",
      productId,
      alias
    )

  def linkListing(
      storeId: String,
      externalId: String,
      productId: String,
      confidence: Double,
      method: String,
      matcher: String
  ): Future[Unit] =
    exec(
      """INSERT INTO product_listings (store_id, external_id, product_id, confidence, method, matcher)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (store_id, external_id) DO UPDATE SET
           product_id = EXCLUDED.product_id, confidence = EXCLUDED.confidence,
           method = EXCLUDED.method, matcher = EXCLUDED.matcher""",
      storeId,
      externalId,
      productId,
      java.lang.Double.valueOf(confidence),
      method,
      matcher
    )

  /** Follows merge redirects to the canonical id. Ids never die, they forward (§6.5). */
  def resolveCanonical(productId: String): Future[Option[String]] =
    query(
      """WITH RECURSIVE chain(id, merged_into) AS (
           SELECT id, merged_into FROM products WHERE id = $1
           UNION ALL
           SELECT p.id, p.merged_into FROM products p JOIN chain c ON p.id = c.merged_into
         )
         SELECT id FROM chain WHERE merged_into IS NULL LIMIT 1""",
      r => r.get(0, classOf[String])
    )(productId).map(_.headOption)

  // ------------------------------------------------------------------ stores

  def upsertStore(
      id: String,
      name: String,
      chainId: String,
      area: String,
      label: Option[String],
      active: Boolean
  ): Future[Unit] =
    for {
      _ <- exec(
        """INSERT INTO stores (id, name, chain_id, area, label, active) VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, chain_id = EXCLUDED.chain_id,
             area = EXCLUDED.area, label = EXCLUDED.label, active = EXCLUDED.active""",
        id,
        name,
        chainId,
        area,
        label.orNull,
        java.lang.Boolean.valueOf(active)
      )
      // store-coverage is derived from the same events; keeping it in step here means
      // a newly registered franchise is immediately eligible for area prices.
      _ <- exec(
        """INSERT INTO store_coverage (store_id, chain_id, area) VALUES ($1, $2, $3)
           ON CONFLICT (store_id) DO UPDATE SET chain_id = EXCLUDED.chain_id, area = EXCLUDED.area""",
        id,
        chainId,
        area
      )
    } yield ()

  /**
   * A partial update: only the fields the command actually supplied change.
   *
   * `chain` is absent on purpose — a franchise does not change banner, and letting it would
   * silently re-point every area observation that spoke for this store (§2.2).
   */
  def updateStoreDetails(
      id: String,
      name: Option[String],
      area: Option[String],
      label: Option[String]
  ): Future[Unit] =
    for {
      _ <- exec(
        """UPDATE stores SET
             name  = COALESCE($2, name),
             area  = COALESCE($3, area),
             label = COALESCE($4, label)
           WHERE id = $1""",
        id,
        name.orNull,
        area.orNull,
        label.orNull
      )
      // Coverage follows the store's area, or an area change would leave the fan-out
      // answering for the region the franchise used to be in.
      _ <- exec(
        "UPDATE store_coverage SET area = COALESCE($2, area) WHERE store_id = $1",
        id,
        area.orNull
      )
    } yield ()

  /**
   * A deactivated store stops being covered by area observations but keeps its row: its purchase
   * and price history still reference it, and deleting it would orphan recorded facts.
   */
  def deactivateStore(id: String): Future[Unit] =
    for {
      _ <- exec("UPDATE stores SET active = FALSE WHERE id = $1", id)
      _ <- exec("DELETE FROM store_coverage WHERE store_id = $1", id)
    } yield ()

  // ------------------------------------------------------------------ prices

  def appendPriceHistory(h: PriceHistoryRow): Future[Unit] =
    exec(
      """INSERT INTO price_history
         (product_id, scope_kind, store_id, chain_id, area, observed_at, price_amount, price_currency,
          unit_price, unit_per_amount, unit_per_unit, promo, price_confidence, size_confidence,
          source, raw_response_id, correlation_id, persistence_id, seq_nr)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
         ON CONFLICT (persistence_id, seq_nr) DO NOTHING""",
      h.productId,
      h.scopeKind,
      h.storeId.orNull,
      h.chainId.orNull,
      h.area.orNull,
      h.observedAt,
      h.priceAmount.bigDecimal,
      h.priceCurrency,
      h.unitPrice.map(_.bigDecimal).orNull,
      h.unitPerAmount.map(_.bigDecimal).orNull,
      h.unitPerUnit.orNull,
      h.promo.orNull,
      java.lang.Double.valueOf(h.priceConfidence),
      java.lang.Double.valueOf(h.sizeConfidence),
      h.source,
      h.rawResponseId.map(java.lang.Long.valueOf).orNull,
      h.correlationId.orNull,
      h.persistenceId,
      java.lang.Long.valueOf(h.seqNr)
    )

  /**
   * Last-write-wins by `observedAt`, NOT by arrival order.
   *
   * A backfill replays original timestamps, so events arrive out of order by design. Without the
   * guard an old backfilled price would overwrite today's.
   */
  def upsertCurrentPrice(c: CurrentPriceRow): Future[Unit] =
    exec(
      """INSERT INTO current_price
         (product_id, scope_key, scope_kind, store_id, chain_id, area, price_amount, price_currency,
          unit_price, observed_at, source, size_confidence)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT (product_id, scope_key) DO UPDATE SET
           price_amount = EXCLUDED.price_amount, price_currency = EXCLUDED.price_currency,
           unit_price = EXCLUDED.unit_price, observed_at = EXCLUDED.observed_at,
           source = EXCLUDED.source, size_confidence = EXCLUDED.size_confidence
         WHERE current_price.observed_at <= EXCLUDED.observed_at""",
      c.productId,
      c.scopeKey,
      c.scopeKind,
      c.storeId.orNull,
      c.chainId.orNull,
      c.area.orNull,
      c.priceAmount.bigDecimal,
      c.priceCurrency,
      c.unitPrice.map(_.bigDecimal).orNull,
      c.observedAt,
      c.source,
      java.lang.Double.valueOf(c.sizeConfidence)
    )

  /**
   * THE §2.3.1 READ-TIME FAN-OUT.
   *
   * What does this product cost at this store? Prefer the most recent EXACT observation for the
   * store; fall back to the most recent AREA observation whose chain+area covers it. The answer
   * reports which it was, so a caller can tell a receipt price from a regional flyer claim.
   *
   * Resolving here rather than materialising per-store rows means a newly registered franchise is
   * priced correctly the moment it exists, with no backfill and nothing to go stale.
   */
  def currentPriceForStore(productId: String, storeId: String): Future[Option[ResolvedPrice]] =
    query(
      """SELECT cp.price_amount, cp.price_currency, cp.observed_at, cp.source, cp.scope_kind
         FROM current_price cp
         LEFT JOIN store_coverage sc ON sc.store_id = $2
         WHERE cp.product_id = $1
           AND (
             (cp.scope_kind = 'exact' AND cp.store_id = $2)
             OR (cp.scope_kind = 'area' AND cp.chain_id = sc.chain_id AND cp.area = sc.area)
           )
         ORDER BY (cp.scope_kind = 'exact') DESC, cp.observed_at DESC
         LIMIT 1""",
      r =>
        ResolvedPrice(
          r.get(0, classOf[java.math.BigDecimal]),
          r.get(1, classOf[String]),
          r.get(2, classOf[Instant]),
          r.get(3, classOf[String]),
          r.get(4, classOf[String])
        )
    )(productId, storeId).map(_.headOption)

  // ----------------------------------------------------------- match index

  def upsertMatchIndex(
      productId: String,
      normalizedName: String,
      tokens: List[String],
      brandNorm: Option[String],
      size: Option[Quantity]
  ): Future[Unit] =
    exec(
      """INSERT INTO match_index
         (product_id, normalized_name, name_tokens, brand_norm, size_amount, size_unit, size_dimension)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (product_id) DO UPDATE SET
           normalized_name = EXCLUDED.normalized_name, name_tokens = EXCLUDED.name_tokens,
           brand_norm = EXCLUDED.brand_norm, size_amount = EXCLUDED.size_amount,
           size_unit = EXCLUDED.size_unit, size_dimension = EXCLUDED.size_dimension""",
      productId,
      normalizedName,
      tokens.toArray,
      brandNorm.orNull,
      size.map(_.amount.bigDecimal).orNull,
      size.map(_.unit.toString).orNull,
      size.map(_.dimension.toString).orNull
    )

  def deleteMatchIndex(productId: String): Future[Unit] =
    exec("DELETE FROM match_index WHERE product_id = $1", productId)

  /** The strong key (§6.1). A hit here is identity, not a guess. */
  def findProductByGtin(gtin: String): Future[Option[String]] =
    query("SELECT product_id FROM product_gtins WHERE gtin = $1", r => r.get(0, classOf[String]))(
      gtin
    )
      .map(_.headOption)

  /** The second strong key (§6.2): a listing already resolved short-circuits the matcher. */
  def findProductByListing(storeId: String, externalId: String): Future[Option[String]] =
    query(
      "SELECT product_id FROM product_listings WHERE store_id = $1 AND external_id = $2",
      r => r.get(0, classOf[String])
    )(storeId, externalId).map(_.headOption)

  /**
   * Trigram top-K.
   *
   * RETRIEVAL ONLY — this narrows the catalogue, it does not decide. The exact score is recomputed
   * by the pure scorer on the shortlist, because pg_trgm's similarity and the scorer's blend
   * disagree by construction and the number recorded on a link must be the one the domain computed,
   * not the one the index guessed (§6.3).
   *
   * `similarity` rather than the `%` operator so the floor is explicit here instead of depending on
   * a session-level `pg_trgm.similarity_threshold` that a migration could silently change.
   */
  def trigramCandidates(
      normalizedName: String,
      limit: Int,
      floor: Double = 0.15
  ): Future[List[MatchCandidateRow]] =
    query(
      """SELECT mi.product_id, mi.normalized_name, mi.brand_norm, mi.size_amount, mi.size_unit
         FROM match_index mi
         JOIN products p ON p.id = mi.product_id
         -- A tombstone is not a candidate: it forwards, so matching onto it would
         -- re-link a listing to a product that no longer exists as a distinct thing.
         WHERE p.merged_into IS NULL
           AND similarity(mi.normalized_name, $1) >= $3
         ORDER BY similarity(mi.normalized_name, $1) DESC
         LIMIT $2""",
      r =>
        MatchCandidateRow(
          r.get(0, classOf[String]),
          r.get(1, classOf[String]),
          Option(r.get(2, classOf[String])),
          Option(r.get(3, classOf[java.math.BigDecimal])).map(BigDecimal(_)),
          Option(r.get(4, classOf[String]))
        )
    )(normalizedName, Integer.valueOf(limit), java.lang.Double.valueOf(floor))

  // ----------------------------------------------------------- review queue

  def upsertResolutionCase(
      id: String,
      subjectName: String,
      subjectBrand: Option[String],
      subjectGtin: Option[String],
      subjectStore: Option[String],
      subjectListing: Option[String],
      candidatesJson: String
  ): Future[Unit] =
    exec(
      """INSERT INTO resolution_cases
         (id, state, subject_name, subject_brand, subject_gtin, subject_store, subject_listing, candidates)
         VALUES ($1, 'pending', $2, $3, $4, $5, $6, $7::jsonb)
         ON CONFLICT (id) DO UPDATE SET
           subject_name = EXCLUDED.subject_name, subject_brand = EXCLUDED.subject_brand,
           subject_gtin = EXCLUDED.subject_gtin, subject_store = EXCLUDED.subject_store,
           subject_listing = EXCLUDED.subject_listing, candidates = EXCLUDED.candidates""",
      id,
      subjectName,
      subjectBrand.orNull,
      subjectGtin.orNull,
      subjectStore.orNull,
      subjectListing.orNull,
      candidatesJson
    )

  def incrementParked(id: String): Future[Unit] =
    exec("UPDATE resolution_cases SET parked_count = parked_count + 1 WHERE id = $1", id)

  /** Terminal. `decided_at` is set once and never cleared — a decided case leaves the queue. */
  def resolveCase(id: String, outcome: String): Future[Unit] =
    exec(
      "UPDATE resolution_cases SET state = 'resolved', outcome = $2, decided_at = now() WHERE id = $1",
      id,
      outcome
    )

  def pendingCaseCount(): Future[Long] =
    query(
      "SELECT count(*) FROM resolution_cases WHERE state = 'pending'",
      r => r.get(0, classOf[java.lang.Long])
    )()
      .map(_.headOption.map(_.longValue).getOrElse(0L))

  /** The pending review queue, newest first — what ariadne-ui lists (§6.5). */
  def listPendingCases(limit: Int = 50): Future[List[CaseRow]] =
    query(
      """SELECT id, subject_name, subject_brand, subject_gtin, subject_store, subject_listing,
                candidates::text, parked_count, created_at
         FROM resolution_cases
         WHERE state = 'pending'
         ORDER BY created_at DESC
         LIMIT $1""",
      r =>
        CaseRow(
          r.get(0, classOf[String]),
          r.get(1, classOf[String]),
          Option(r.get(2, classOf[String])),
          Option(r.get(3, classOf[String])),
          Option(r.get(4, classOf[String])),
          Option(r.get(5, classOf[String])),
          r.get(6, classOf[String]),
          r.get(7, classOf[Integer]).intValue,
          r.get(8, classOf[Instant])
        )
    )(Integer.valueOf(limit))

  def getProduct(id: String): Future[Option[ProductRow]] =
    query(
      """SELECT id, name, brand, category, size_amount, size_unit, status, merged_into
         FROM products WHERE id = $1""",
      productRow
    )(id).map(_.headOption)

  /**
   * Typeahead over the catalogue. Ranked by trigram similarity against the SAME normalised text the
   * matcher indexes, so what a human searches and what the resolver matches agree — a search that
   * ranked differently from the matcher would make the review queue confusing to work.
   *
   * Tombstones are excluded: they forward, and offering one as a search result invites a human to
   * link something onto a product that no longer exists as a distinct thing.
   */
  def searchProducts(term: String, limit: Int = 20): Future[List[ProductRow]] =
    query(
      """SELECT p.id, p.name, p.brand, p.category, p.size_amount, p.size_unit, p.status, p.merged_into
         FROM products p
         JOIN match_index mi ON mi.product_id = p.id
         WHERE p.merged_into IS NULL
           AND (mi.normalized_name ILIKE '%' || $1 || '%' OR similarity(mi.normalized_name, $1) >= 0.2)
         ORDER BY similarity(mi.normalized_name, $1) DESC
         LIMIT $2""",
      productRow
    )(term, Integer.valueOf(limit))

  private val productRow: Row => ProductRow = r =>
    ProductRow(
      r.get(0, classOf[String]),
      r.get(1, classOf[String]),
      Option(r.get(2, classOf[String])),
      Option(r.get(3, classOf[String])),
      Option(r.get(4, classOf[java.math.BigDecimal])).map(BigDecimal(_)),
      Option(r.get(5, classOf[String])),
      r.get(6, classOf[String]),
      Option(r.get(7, classOf[String]))
    )

  // ------------------------------------------------------------------ plumbing

  private def exec(sql: String, args: Any*): Future[Unit] =
    runStatement(sql, args).map(_ => ())

  private def query[A](sql: String, map: Row => A)(args: Any*): Future[List[A]] =
    runQuery(sql, args, map)

  private def runStatement(sql: String, args: Seq[Any]): Future[Long] =
    withConnection { conn =>
      val st = bind(conn.createStatement(sql), args)
      toFuture(st.execute()).flatMap(res => toFuture(res.getRowsUpdated).map(_.longValue))
    }

  private def runQuery[A](sql: String, args: Seq[Any], map: Row => A): Future[List[A]] =
    withConnection { conn =>
      val st = bind(conn.createStatement(sql), args)
      toFuture(st.execute()).flatMap { res =>
        collect[A](res.map((row, _) => map(row)))
      }
    }

  private def bind(st: io.r2dbc.spi.Statement, args: Seq[Any]): io.r2dbc.spi.Statement = {
    args.zipWithIndex.foreach {
      case (null, i) => st.bindNull(i, classOf[Object])
      case (v, i) => st.bind(i, v.asInstanceOf[Object])
    }
    st
  }

  private def withConnection[A](f: io.r2dbc.spi.Connection => Future[A]): Future[A] =
    toFuture(cf.create()).flatMap { conn =>
      val result = f(conn)
      result.transformWith(r => toFuture(conn.close()).transform(_ => r))
    }

  private def toFuture[A](p: Publisher[A]): Future[A] = {
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

  private def collect[A](p: Publisher[A]): Future[List[A]] = {
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

object ReadModelRepository {

  final case class PriceHistoryRow(
      productId: String,
      scopeKind: String,
      storeId: Option[String],
      chainId: Option[String],
      area: Option[String],
      observedAt: Instant,
      priceAmount: BigDecimal,
      priceCurrency: String,
      unitPrice: Option[BigDecimal],
      unitPerAmount: Option[BigDecimal],
      unitPerUnit: Option[String],
      promo: Option[String],
      priceConfidence: Double,
      sizeConfidence: Double,
      source: String,
      /** The archived bytes this fact came from. None for manual, purchase and migrated rows. */
      rawResponseId: Option[Long],
      correlationId: Option[String],
      persistenceId: String,
      seqNr: Long
  )

  final case class CurrentPriceRow(
      productId: String,
      scopeKey: String,
      scopeKind: String,
      storeId: Option[String],
      chainId: Option[String],
      area: Option[String],
      priceAmount: BigDecimal,
      priceCurrency: String,
      unitPrice: Option[BigDecimal],
      observedAt: Instant,
      source: String,
      sizeConfidence: Double
  )

  final case class CaseRow(
      id: String,
      subjectName: String,
      subjectBrand: Option[String],
      subjectGtin: Option[String],
      subjectStore: Option[String],
      subjectListing: Option[String],
      candidatesJson: String,
      parkedCount: Int,
      createdAt: Instant
  )

  final case class ProductRow(
      id: String,
      name: String,
      brand: Option[String],
      category: Option[String],
      sizeAmount: Option[BigDecimal],
      sizeUnit: Option[String],
      status: String,
      mergedInto: Option[String]
  )

  /** One shortlist entry, as stored — the scorer re-derives everything else. */
  final case class MatchCandidateRow(
      productId: String,
      normalizedName: String,
      brandNorm: Option[String],
      sizeAmount: Option[BigDecimal],
      sizeUnit: Option[String]
  )

  /** A price with its provenance — a caller must be able to tell a receipt from a flyer. */
  final case class ResolvedPrice(
      amount: java.math.BigDecimal,
      currency: String,
      observedAt: Instant,
      source: String,
      scopeKind: String
  ) {
    def isExact: Boolean = scopeKind == "exact"
  }
}

package me.cference.ariadne.projection

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.resolution.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Using

/** The review queue — the read model ariadne-ui exists to render (§6.5). */
final class ReviewQueueSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with PostgresFixture {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  private lazy val repo = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new ReadModelRepository(new PostgresqlConnectionFactory(cfg))
  }

  override def beforeAll(): Unit = applySchema()
  override def afterAll(): Unit = container.stop()

  private def on(id: String, e: ResolutionEvent) =
    ProjectionHandlers.resolution(repo)(s"resolution|$id", e).futureValue

  private def scalar(sql: String): Option[String] =
    Using.resource(connection()) { c =>
      val rs = c.createStatement().executeQuery(sql)
      if rs.next() then Option(rs.getString(1)) else None
    }

  private val subject = MatchSubject(
    "Lactantia Butter 454 g",
    Some("Lactantia"),
    Some(Gtin.unsafe("4006381333931")),
    Some(ListingKey(StoreId("s-1"), "ext-9"))
  )
  private val candidates = List(
    ScoredCandidate(ProductId("p-a"), Confidence.unsafe(0.81), List("size conflict")),
    ScoredCandidate(ProductId("p-b"), Confidence.unsafe(0.74), Nil)
  )

  "a proposed case" should {

    "appear in the queue with its subject and candidates" in {
      on("r-1", ResolutionEvent.ResolutionProposed(ResolutionId("r-1"), subject, candidates))
      scalar("SELECT state FROM resolution_cases WHERE id = 'r-1'") shouldBe Some("pending")
      scalar("SELECT subject_brand FROM resolution_cases WHERE id = 'r-1'") shouldBe Some(
        "Lactantia"
      )
      repo.pendingCaseCount().futureValue shouldBe 1L
    }

    "store candidates as valid JSON, queryable by Postgres" in {
      // Stored as offered — the UI must show what the matcher said AT THE TIME, not a
      // re-derivation from a matcher that may since have changed (§6.6).
      scalar(
        "SELECT candidates->0->>'productId' FROM resolution_cases WHERE id = 'r-1'"
      ) shouldBe Some("p-a")
      scalar("SELECT candidates->0->>'score' FROM resolution_cases WHERE id = 'r-1'") shouldBe Some(
        "0.81"
      )
      scalar(
        "SELECT jsonb_array_length(candidates) FROM resolution_cases WHERE id = 'r-1'"
      ) shouldBe Some("2")
      scalar(
        "SELECT candidates->0->'notes'->>0 FROM resolution_cases WHERE id = 'r-1'"
      ) shouldBe Some("size conflict")
    }

    "count parked observations, so the queue shows what is waiting on the decision" in {
      val parked = ParkedObservation(
        Money.unsafe(BigDecimal("4.99")),
        Instant.parse("2026-08-26T12:00:00Z"),
        PriceScope.Regional(ChainId("iga"), Area("H2X")),
        Confidence.Certain,
        Confidence.Certain
      )
      on("r-1", ResolutionEvent.ObservationParked(parked))
      on("r-1", ResolutionEvent.ObservationParked(parked))
      scalar("SELECT parked_count FROM resolution_cases WHERE id = 'r-1'") shouldBe Some("2")
    }
  }

  "a decided case" should {

    "leave the pending queue and record what was decided" in {
      on("r-1", ResolutionEvent.ResolutionConfirmed(ProductId("p-a"), Nil))
      scalar("SELECT state FROM resolution_cases WHERE id = 'r-1'") shouldBe Some("resolved")
      scalar("SELECT outcome FROM resolution_cases WHERE id = 'r-1'") shouldBe Some("Confirmed:p-a")
      scalar("SELECT decided_at IS NOT NULL FROM resolution_cases WHERE id = 'r-1'") shouldBe Some(
        "t"
      )
      repo.pendingCaseCount().futureValue shouldBe 0L
    }

    "record each terminal verb distinguishably" in {
      on("r-2", ResolutionEvent.ResolutionProposed(ResolutionId("r-2"), subject, Nil))
      on("r-2", ResolutionEvent.ResolutionRejected(ProductId("p-new"), Nil))
      scalar("SELECT outcome FROM resolution_cases WHERE id = 'r-2'") shouldBe Some(
        "NewProduct:p-new"
      )

      on("r-3", ResolutionEvent.ResolutionProposed(ResolutionId("r-3"), subject, Nil))
      on("r-3", ResolutionEvent.MergeRequested(ProductId("w"), ProductId("l")))
      scalar("SELECT outcome FROM resolution_cases WHERE id = 'r-3'") shouldBe Some("Merged:w<-l")

      on("r-4", ResolutionEvent.ResolutionProposed(ResolutionId("r-4"), subject, Nil))
      on(
        "r-4",
        ResolutionEvent.SplitRequested(ListingKey(StoreId("s-1"), "e-1"), ProductId("p-split"))
      )
      scalar("SELECT outcome FROM resolution_cases WHERE id = 'r-4'") shouldBe Some("Split:p-split")
    }
  }

  "the JSON encoder" should {
    "escape a subject that contains quotes and backslashes" in {
      // Product names come from retailer feeds, so they are hostile input. An
      // unescaped quote here would produce invalid JSON and fail the whole insert.
      val nasty = ScoredCandidate(
        ProductId("""p-"odd"\x"""),
        Confidence.unsafe(0.7),
        List("""a "quoted" note""")
      )
      on("r-5", ResolutionEvent.ResolutionProposed(ResolutionId("r-5"), subject, List(nasty)))
      scalar("SELECT candidates->0->>'productId' FROM resolution_cases WHERE id = 'r-5'") shouldBe
        Some("""p-"odd"\x""")
    }
  }
}

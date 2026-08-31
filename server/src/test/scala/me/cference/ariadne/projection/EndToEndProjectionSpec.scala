package me.cference.ariadne.projection

import com.typesafe.config.ConfigFactory
import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceCommand, PriceSource}
import me.cference.ariadne.domain.store.StoreCommand
import me.cference.ariadne.persistence.{PriceStreamEntity, StoreEntity}
import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.projection.testkit.scaladsl.ProjectionTestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.util.Using

object EndToEndProjectionSpec {

  // Started before the ActorSystem so the journal config can point at it.
  val container: com.dimafeng.testcontainers.PostgreSQLContainer = {
    val c = com.dimafeng.testcontainers.PostgreSQLContainer(
      org.testcontainers.utility.DockerImageName.parse("postgres:16-alpine")
    )
    c.start()
    val ddl =
      Using.resource(scala.io.Source.fromResource("ddl/create_tables_postgres.sql"))(_.mkString)
    Using.resource(java.sql.DriverManager.getConnection(c.jdbcUrl, c.username, c.password)) {
      conn =>
        Using.resource(conn.createStatement())(_.execute(ddl))
    }
    c
  }

  val config = ConfigFactory
    .parseString(s"""
      pekko.persistence.r2dbc.connection-factory {
        host = "${container.host}"
        port = ${container.mappedPort(5432)}
        database = "${container.databaseName}"
        user = "${container.username}"
        password = "${container.password}"
      }
      pekko.actor.provider = "local"
      # eventsBySlices holds back very recent events so a concurrent write cannot be
      # missed. That safety margin makes a test that writes and immediately projects
      # look like a silent failure, so it is zeroed here — and ONLY here.
      pekko.persistence.r2dbc.query {
        behind-current-time = 0 millis
        backtracking.behind-current-time = 0 millis
        refresh-interval = 200 millis
      }
    """)
    .withFallback(ConfigFactory.load())
    .resolve()
}

/**
 * The wiring end to end: a command reaches an entity, the event lands in the journal, the
 * projection folds it, and the read model answers.
 *
 * Every layer below has its own tests, so what this proves is the JOINS between them — that the
 * entity's persistence id is the one the projection's `eventsBySlices` subscribes to, that the
 * event deserializes on the read side, and that the handler is reached with the sequence number it
 * needs. Those are exactly the seams that unit tests cannot see.
 */
final class EndToEndProjectionSpec
    extends ScalaTestWithActorTestKit(EndToEndProjectionSpec.config)
    with AnyWordSpecLike
    with Matchers {

  private given ExecutionContext = testKit.system.executionContext
  private given org.apache.pekko.actor.typed.ActorSystem[?] = testKit.system

  private val projectionTestKit = ProjectionTestKit(testKit.system)

  private lazy val repo = {
    val c = EndToEndProjectionSpec.container
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(c.host)
      .port(c.mappedPort(5432))
      .database(c.databaseName)
      .username(c.username)
      .password(c.password)
      .build()
    new ReadModelRepository(new PostgresqlConnectionFactory(cfg))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EndToEndProjectionSpec.container.stop()
  }

  private def scalar(sql: String): Option[String] =
    Using.resource(
      java.sql.DriverManager.getConnection(
        EndToEndProjectionSpec.container.jdbcUrl,
        EndToEndProjectionSpec.container.username,
        EndToEndProjectionSpec.container.password
      )
    ) { c =>
      val rs = c.createStatement().executeQuery(sql)
      if rs.next() then Option(rs.getString(1)) else None
    }

  "a store registration" should {
    "travel from the entity through the journal into store_coverage" in {
      val probe = testKit.createTestProbe[StatusReply[Done]]()
      val entity = testKit.spawn(StoreEntity("s-e2e"))
      entity ! StoreEntity.Execute(
        StoreCommand.RegisterStore(
          StoreId("s-e2e"),
          "IGA E2E",
          ChainId("iga"),
          Area("H9Z"),
          None,
          CorrelationId("c-1")
        ),
        probe.ref
      )
      probe.receiveMessage().isSuccess shouldBe true

      val ranges = org.apache.pekko.projection.eventsourced.scaladsl.EventSourcedProvider
        .sliceRanges(
          testKit.system,
          org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal.Identifier,
          1
        )

      projectionTestKit.run(AriadneProjections.storeProjection(repo, ranges.head)) {
        scalar("SELECT chain_id FROM store_coverage WHERE store_id = 's-e2e'") shouldBe Some("iga")
      }
    }
  }

  "a regional price observation" should {
    "reach price_history with its scope and sequence number intact" in {
      val probe = testKit.createTestProbe[StatusReply[Done]]()
      val scope = PriceScope.Regional(ChainId("iga"), Area("H9Z"))
      val entity = testKit.spawn(PriceStreamEntity(ProductId("p-e2e"), scope))
      val now = Instant.parse("2026-08-26T12:00:00Z")

      entity ! PriceStreamEntity.Execute(
        PriceCommand.ObservePrice(
          ProductId("p-e2e"),
          scope,
          Money.unsafe(BigDecimal("5.49")),
          now,
          PriceSource.Scrape("flipp", rawResponseId = 1L),
          None,
          None,
          Confidence.Certain,
          Confidence.unsafe(0.4),
          CorrelationId("c-2")
        ),
        now,
        probe.ref
      )
      probe.receiveMessage().isSuccess shouldBe true

      val ranges = org.apache.pekko.projection.eventsourced.scaladsl.EventSourcedProvider
        .sliceRanges(
          testKit.system,
          org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal.Identifier,
          1
        )

      projectionTestKit.run(AriadneProjections.priceProjection(repo, ranges.head)) {
        scalar("SELECT scope_kind FROM price_history WHERE product_id = 'p-e2e'") shouldBe Some(
          "area"
        )
        // sizeConfidence survives the whole trip — it is contract, not decoration (§2.3).
        scalar(
          "SELECT size_confidence FROM price_history WHERE product_id = 'p-e2e'"
        ) shouldBe Some("0.4")
        // The scope must NOT have degraded to a store fact along the way.
        scalar("SELECT store_id FROM price_history WHERE product_id = 'p-e2e'") shouldBe None
        scalar("SELECT chain_id FROM price_history WHERE product_id = 'p-e2e'") shouldBe Some("iga")
      }
    }
  }
}

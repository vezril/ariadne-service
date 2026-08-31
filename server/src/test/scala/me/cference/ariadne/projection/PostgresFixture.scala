package me.cference.ariadne.projection

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.util.Using

/**
 * A real Postgres for the read-model tests.
 *
 * Mocking here would prove nothing: what these tests need to establish is that the actual SQL runs
 * — the CHECK constraints, the exact-over-area resolution, the upsert conflict targets. None of
 * that is exercised by an in-memory stub.
 */
trait PostgresFixture {

  protected lazy val container: PostgreSQLContainer = {
    val c = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
    c.start()
    c
  }

  protected def connection(): Connection =
    DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

  protected def applySchema(): Unit = {
    val ddl = Using.resource(Source.fromResource("ddl/create_tables_postgres.sql"))(_.mkString)
    Using.resource(connection()) { c =>
      Using.resource(c.createStatement())(_.execute(ddl))
    }
  }

  protected def truncateAll(): Unit =
    Using.resource(connection()) { c =>
      Using.resource(c.createStatement()) { st =>
        st.execute(
          """TRUNCATE product_listings, product_aliases, product_gtins, products,
             store_coverage, stores, price_history, current_price,
             match_index, resolution_cases, raw_response, purchase_lines, purchases RESTART IDENTITY CASCADE"""
        )
      }
    }
}

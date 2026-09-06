package me.cference.ariadne.projection

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Using

/**
 * The schema is a projection, so it must apply cleanly from scratch — that IS the recovery story
 * and the schema-evolution story (drop tables, reset offsets, replay).
 */
final class SchemaSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with PostgresFixture {

  override def beforeAll(): Unit = applySchema()
  override def afterAll(): Unit = container.stop()

  "the read-model schema" should {

    "apply from scratch, and be idempotent (CREATE TABLE IF NOT EXISTS)" in {
      // Applying twice is what a redeploy does.
      applySchema()
      Using.resource(connection()) { c =>
        val rs = c.getMetaData.getTables(null, "public", "%", Array("TABLE"))
        val tables =
          Iterator.continually(rs).takeWhile(_.next()).map(_.getString("TABLE_NAME")).toSet
        tables should contain allOf (
          "products",
          "product_gtins",
          "stores",
          "store_coverage",
          "price_history",
          "current_price",
          "purchases",
          "purchase_lines"
        )
      }
    }

    "REFUSE a price_history row whose scope columns contradict its kind" in {
      // The CHECK is the schema-level guard on §2.3.1: an exact row without a store,
      // or an area row without chain+area, is not a coherent price fact and must not
      // be storable at all.
      Using.resource(connection()) { c =>
        val bad = c.prepareStatement(
          """INSERT INTO price_history
             (product_id, scope_kind, observed_at, price_amount, price_currency,
              price_confidence, size_confidence, source, persistence_id, seq_nr)
             VALUES ('p-1', 'exact', now(), 1.00, 'CAD', 1.0, 1.0, 'Manual', 'pid', 1)"""
        )
        an[org.postgresql.util.PSQLException] should be thrownBy bad.execute()
      }
    }

    "accept a well-formed exact row and a well-formed area row" in {
      Using.resource(connection()) { c =>
        c.createStatement()
          .execute(
            """INSERT INTO price_history
             (product_id, scope_kind, store_id, observed_at, price_amount, price_currency,
              price_confidence, size_confidence, source, persistence_id, seq_nr)
             VALUES ('p-1', 'exact', 's-1', now(), 4.99, 'CAD', 1.0, 1.0, 'Manual', 'pid-exact', 1)"""
          )
        c.createStatement()
          .execute(
            """INSERT INTO price_history
             (product_id, scope_kind, chain_id, area, observed_at, price_amount, price_currency,
              price_confidence, size_confidence, source, persistence_id, seq_nr)
             VALUES ('p-1', 'area', 'iga', 'H2X', now(), 5.49, 'CAD', 1.0, 1.0, 'Scrape', 'pid-area', 1)"""
          )
        val rs = c
          .createStatement()
          .executeQuery("SELECT count(*) FROM price_history WHERE product_id = 'p-1'")
        rs.next()
        rs.getInt(1) shouldBe 2
      }
    }

    "make re-delivery a no-op via the journal coordinates" in {
      // At-least-once delivery means the same event WILL arrive twice after a restart.
      // The primary key is what makes that harmless.
      Using.resource(connection()) { c =>
        val insert =
          """INSERT INTO price_history
             (product_id, scope_kind, store_id, observed_at, price_amount, price_currency,
              price_confidence, size_confidence, source, persistence_id, seq_nr)
             VALUES ('p-2', 'exact', 's-1', now(), 1.00, 'CAD', 1.0, 1.0, 'Manual', 'pid-dup', 7)
             ON CONFLICT (persistence_id, seq_nr) DO NOTHING"""
        c.createStatement().execute(insert)
        c.createStatement().execute(insert)
        val rs = c
          .createStatement()
          .executeQuery("SELECT count(*) FROM price_history WHERE persistence_id = 'pid-dup'")
        rs.next()
        rs.getInt(1) shouldBe 1
      }
    }
  }
}

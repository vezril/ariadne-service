package me.cference.ariadne.persistence

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import scala.io.Source
import scala.util.{Try, Using}

/**
 * Applies the bundled schema at startup (Apollo's precedent, `DB_AUTO_MIGRATE`, default on).
 *
 * Every statement is `CREATE ... IF NOT EXISTS`, so running it on every boot is a no-op once the
 * database is current. Self-migration is viable here precisely because the read models are
 * projections: the risky kind of migration — the one that must preserve data it cannot recompute —
 * only applies to the journal tables, which are Pekko's and do not change under us.
 */
object SchemaMigration {

  private val log = LoggerFactory.getLogger(getClass)

  def applyIfEnabled(config: Config): Unit = {
    val enabled = Try(config.getBoolean("ariadne.db.auto-migrate")).getOrElse(true)
    if !enabled then log.info("DB auto-migrate disabled; skipping schema application")
    else {
      val c = config.getConfig("pekko.persistence.r2dbc.connection-factory")
      val url =
        s"jdbc:postgresql://${c.getString("host")}:${c.getInt("port")}/${c.getString("database")}"
      val ddl = Using.resource(Source.fromResource("ddl/create_tables_postgres.sql"))(_.mkString)
      Using.resource(
        DriverManager.getConnection(url, c.getString("user"), c.getString("password"))
      ) { conn =>
        Using.resource(conn.createStatement())(_.execute(ddl))
      }
      log.info("Schema applied (idempotent) at {}", url)
    }
  }
}

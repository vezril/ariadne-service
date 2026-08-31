package me.cference.ariadne.ingest

import io.r2dbc.postgresql.{PostgresqlConnectionConfiguration, PostgresqlConnectionFactory}
import me.cference.ariadne.projection.PostgresFixture
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * The archive is the only insurance against a decoder bug, because flyers expire and there is no
 * re-fetch. These tests are about that guarantee, not about CRUD.
 */
final class RawArchiveSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with PostgresFixture {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Millis))

  private lazy val archive = {
    val cfg = PostgresqlConnectionConfiguration
      .builder()
      .host(container.host)
      .port(container.mappedPort(5432))
      .database(container.databaseName)
      .username(container.username)
      .password(container.password)
      .build()
    new PostgresRawArchive(new PostgresqlConnectionFactory(cfg))
  }

  override def beforeAll(): Unit = applySchema()
  override def afterAll(): Unit = container.stop()

  // Anchored to the clock, never to a literal date. Demeter's DailyRunSpec went red at
  // midnight on a hardcoded 2026-08-30 window, on a docs-only PR, taking five tests with
  // it (demeter-service #49). Time-sensitive fixtures holding absolute instants are a bomb
  // with a date on it, and this module ports code from exactly that lineage.
  private val now = Instant.now()

  private def raw(runId: String, body: String, kind: String = "flyer_items") =
    RawResponse(
      runId,
      "flipp",
      kind,
      s"https://flipp.example/$kind",
      Some("H2X1Y4"),
      Some("en-ca"),
      now,
      "application/json",
      body.getBytes("UTF-8")
    )

  "the archive" should {

    "return an id and give the bytes back byte-for-byte" in {
      val body = """{"items":[{"name":"Salted butter","price":"4.99"}]}"""
      val archived = archive.archive(raw("run-1", body)).futureValue
      archived.id should be > 0L
      // Read back through a FRESH call, not the in-memory value: what matters is what
      // survived the round trip, not what was passed in.
      archive.get(archived.id).futureValue.map(_.bodyString) shouldBe Some(body)
    }

    "preserve non-ASCII bytes exactly — French names are the normal case here" in {
      val body = """{"name":"Crème glacée à l'érable, bœuf haché"}"""
      val archived = archive.archive(raw("run-utf8", body)).futureValue
      archive.get(archived.id).futureValue.map(_.bodyString) shouldBe Some(body)
    }

    "replay a whole run in fetch order" in {
      // Replay re-derives the observation set from these bytes, so order matters: the
      // decoders ran in this order and a re-derivation has to match.
      val ids =
        (1 to 5).map(i => archive.archive(raw("run-ordered", s"""{"n":$i}""")).futureValue.id)
      val replayed = archive.replay("run-ordered").futureValue
      replayed.map(_.id) shouldBe ids.toList
      replayed.map(_.bodyString) shouldBe (1 to 5).map(i => s"""{"n":$i}""").toList
    }

    "keep runs separate, so replaying one cannot pull in another's bytes" in {
      archive.archive(raw("run-a", """{"a":1}""")).futureValue
      archive.archive(raw("run-b", """{"b":1}""")).futureValue
      archive.replay("run-a").futureValue.map(_.bodyString) shouldBe List("""{"a":1}""")
      archive.replay("run-b").futureValue.map(_.bodyString) shouldBe List("""{"b":1}""")
    }

    "return an empty replay for an unknown run rather than failing" in {
      archive.replay("never-happened").futureValue shouldBe Nil
    }

    "archive an IDENTICAL body again rather than deduplicating it away" in {
      // The same flyer fetched twice is two facts about the world — the second fetch is
      // evidence the flyer was still live. Suppressing it would erase that. The dedup
      // index exists for lookup, not for suppression.
      val body = """{"same":"bytes"}"""
      val a = archive.archive(raw("run-dup", body)).futureValue
      val b = archive.archive(raw("run-dup", body)).futureValue
      a.id should not be b.id
      archive.replay("run-dup").futureValue should have size 2
    }

    "hash the body, so a corrupted read is detectable" in {
      val body = """{"check":"sum"}"""
      raw("run-hash", body).sha256 should have length 32
      // Same bytes, same digest — the property the column exists for.
      raw("run-hash", body).sha256.toList shouldBe raw("other-run", body).sha256.toList
    }
  }
}

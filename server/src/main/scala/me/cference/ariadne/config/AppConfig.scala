package me.cference.ariadne.config

import com.typesafe.config.Config
import me.cference.ariadne.domain.ChainId
import me.cference.ariadne.ingest.flipp.{MerchantId, PostalCode, ScrapeSource}
import me.cference.ariadne.text.Locale

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.*

/** Typed view over the `ariadne.http` config block. */
final case class HttpConfig(host: String, port: Int)

/**
 * Typed view over `ariadne.scrape`.
 *
 * `sources` is parsed EAGERLY at boot, including when scraping is disabled, so a malformed postal
 * code or an unknown locale fails the process on startup rather than six hours later inside a timer
 * — where the only symptom would be a scrape that quietly never ran.
 */
final case class ScrapeConfig(
    enabled: Boolean,
    interval: FiniteDuration,
    initialDelay: FiniteDuration,
    sources: List[ScrapeSource]
)

final case class AppConfig(http: HttpConfig, scrape: ScrapeConfig)

object AppConfig:

  /** Read + type the operational config. Fails fast (Typesafe Config throws) on a missing key. */
  def load(raw: Config): AppConfig =
    val http = raw.getConfig("ariadne.http")
    AppConfig(
      HttpConfig(http.getString("host"), http.getInt("port")),
      scrape(raw.getConfig("ariadne.scrape"))
    )

  private def scrape(c: Config): ScrapeConfig =
    ScrapeConfig(
      enabled = c.getBoolean("enabled"),
      interval = c.getDuration("interval").toMillis.millis,
      initialDelay = c.getDuration("initial-delay").toMillis.millis,
      sources = c.getConfigList("sources").asScala.toList.map(source)
    )

  private def source(c: Config): ScrapeSource =
    val name = c.getString("name")
    ScrapeSource(
      name = name,
      baseUrl = c.getString("base-url"),
      // `unsafe` is right here and only here: a bad postal code in config is an
      // operator error that should stop the boot, not a value to recover from.
      postal = PostalCode.unsafe(c.getString("postal-code")),
      locale = locale(c.getString("locale"), name),
      chains = c
        .getConfig("chains")
        .entrySet()
        .asScala
        .map { e =>
          // Typesafe Config quotes numeric-looking keys; strip them back off.
          val merchant = e.getKey.stripPrefix("\"").stripSuffix("\"")
          MerchantId(merchant.toInt) -> ChainId(e.getValue.unwrapped().toString)
        }
        .toMap
    )

  private def locale(raw: String, source: String): Locale =
    Locale.values
      .find(_.queryValue.equalsIgnoreCase(raw))
      .getOrElse(
        throw new IllegalArgumentException(
          s"scrape source '$source': unknown locale '$raw' — expected one of " +
            Locale.values.map(_.queryValue).mkString(", ")
        )
      )

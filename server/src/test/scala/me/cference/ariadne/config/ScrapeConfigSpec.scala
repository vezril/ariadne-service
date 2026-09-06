package me.cference.ariadne.config

import com.typesafe.config.ConfigFactory
import me.cference.ariadne.domain.ChainId
import me.cference.ariadne.ingest.flipp.MerchantId
import me.cference.ariadne.text.Locale
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

/**
 * Config is where the scraper's real-world behaviour is decided, and every value here is one a typo
 * makes wrong-but-plausible: a mistyped postal code scrapes a real, wrong city; a missing merchant
 * mapping quietly drops a chain. Parsing is therefore eager and loud, and this pins that.
 */
final class ScrapeConfigSpec extends AnyWordSpec with Matchers {

  private def parse(hocon: String) =
    AppConfig
      .load(ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()))
      .scrape

  "the shipped defaults" should {

    "have scraping OFF with no sources" in {
      // Not caution — a deployed service must not start fetching from a live
      // third-party endpoint merely because someone rolled it out.
      val c = parse("")
      c.enabled shouldBe false
      c.sources shouldBe empty
    }
  }

  "a configured source" should {

    "parse fully, including the numeric merchant keys HOCON quotes" in {
      val c = parse("""
        ariadne.scrape {
          enabled = true
          interval = 4h
          initial-delay = 30s
          sources = [{
            name = "flipp"
            base-url = "https://flipp.test/api"
            postal-code = "h2x 1y4"
            locale = "fr-ca"
            chains = { "42" = "iga", "7" = "metro" }
          }]
        }
      """)

      c.enabled shouldBe true
      c.interval shouldBe 4.hours
      c.initialDelay shouldBe 30.seconds

      c.sources should have size 1
      val s = c.sources.head
      s.name shouldBe "flipp"
      s.locale shouldBe Locale.FrCa
      // Normalised and split: the FSA is what a Regional price scope is keyed on.
      s.postal.fsa shouldBe "H2X"
      s.postal.canonical shouldBe "H2X1Y4"
      s.chains shouldBe Map(MerchantId(42) -> ChainId("iga"), MerchantId(7) -> ChainId("metro"))
    }
  }

  "a malformed source" should {

    // Each of these fails the BOOT rather than the first scrape six hours later, where
    // the only symptom would be a run that silently never happened.

    "reject a postal code that is not one" in {
      an[IllegalArgumentException] should be thrownBy parse("""
        ariadne.scrape.sources = [{
          name = "flipp", base-url = "https://x", postal-code = "nope",
          locale = "en-ca", chains = {}
        }]
      """)
    }

    "reject a locale Flipp does not serve" in {
      val e = the[IllegalArgumentException] thrownBy parse("""
        ariadne.scrape.sources = [{
          name = "flipp", base-url = "https://x", postal-code = "H2X1Y4",
          locale = "en-us", chains = {}
        }]
      """)
      e.getMessage should include("en-us")
      // The message has to say what IS acceptable; "invalid locale" just sends an
      // operator to read the source.
      e.getMessage should include("fr-ca")
    }

    "be rejected even while scraping is DISABLED" in {
      // The whole point of eager parsing: a broken source must not lie dormant until
      // the day someone flips the flag.
      an[IllegalArgumentException] should be thrownBy parse("""
        ariadne.scrape {
          enabled = false
          sources = [{
            name = "flipp", base-url = "https://x", postal-code = "nope",
            locale = "en-ca", chains = {}
          }]
        }
      """)
    }
  }
}

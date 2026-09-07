package me.cference.ariadne.hermes

import com.typesafe.config.Config

/**
 * Where the broker is, and what the topics are called (§5).
 *
 * OFF by default. Publishing is a downstream courtesy rather than part of recording a fact, so a
 * service that cannot reach Hermes must still scrape, resolve and serve — and a service deployed
 * without a broker should not spend its life retrying into one.
 */
final case class HermesConfig(
    enabled: Boolean,
    host: String,
    port: Int,
    tls: Boolean,
    topics: CatalogMessages.Topics
)

object HermesConfig {

  def load(c: Config): HermesConfig =
    HermesConfig(
      enabled = c.getBoolean("enabled"),
      host = c.getString("host"),
      port = c.getInt("port"),
      tls = c.getBoolean("tls"),
      topics = CatalogMessages.Topics(
        productRegistered = c.getString("topics.product-registered"),
        priceObserved = c.getString("topics.price-observed"),
        purchaseRecorded = c.getString("topics.purchase-recorded")
      )
    )
}

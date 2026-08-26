package me.cference.ariadne.config

import com.typesafe.config.Config

/** Typed view over the `ariadne.http` config block. */
final case class HttpConfig(host: String, port: Int)
final case class AppConfig(http: HttpConfig)

object AppConfig:

  /** Read + type the operational config. Fails fast (Typesafe Config throws) on a missing key. */
  def load(raw: Config): AppConfig =
    val http = raw.getConfig("ariadne.http")
    AppConfig(HttpConfig(http.getString("host"), http.getInt("port")))

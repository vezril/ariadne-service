package me.cference.ariadne.http

import me.cference.ariadne.Greeting
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

/** `GET /` -> a plain-text hello, sourced from the pure `core` Greeting. */
object HelloRoutes:

  def apply(): Route =
    pathEndOrSingleSlash {
      get {
        complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, Greeting.message()))
      }
    }

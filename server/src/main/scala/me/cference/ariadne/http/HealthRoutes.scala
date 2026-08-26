package me.cference.ariadne.http

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

/**
 * The HTTP health surface. `GET /health` reports `UP` (200) while ready and `DOWN` (503) once
 * readiness is withdrawn (during coordinated shutdown). Any other route falls through to a 404 (via
 * the sealed route in the server), leaving the connection healthy.
 */
object HealthRoutes:

  /**
   * @param version
   *   build version reported in the body
   * @param isReady
   *   readiness probe; false => 503 DOWN
   */
  def apply(version: String, isReady: () => Boolean): Route =
    path("health") {
      get {
        if isReady() then complete(response(StatusCodes.OK, "UP", version))
        else complete(response(StatusCodes.ServiceUnavailable, "DOWN", version))
      }
    }

  private def response(status: StatusCode, label: String, version: String): HttpResponse =
    val body = s"""{"status":"$label","service":"ariadne","version":"$version"}"""
    HttpResponse(status = status, entity = HttpEntity(ContentTypes.`application/json`, body))

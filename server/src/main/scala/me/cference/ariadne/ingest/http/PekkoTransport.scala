package me.cference.ariadne.ingest.http

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpRequest, Uri}
import org.apache.pekko.http.scaladsl.model.headers.RawHeader

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
 * The real HTTP GET, on Pekko HTTP.
 *
 * Deliberately thin. Every decision that could make Ariadne a rude client — how long to wait
 * between requests, how many attempts, which headers, what counts as a bot wall — lives in
 * `PoliteFetcher` and the pure policy functions it calls. This class knows only how to perform one
 * request and hand back its bytes, so there is no second place where politeness could quietly
 * differ from the policy.
 *
 * The entity is ALWAYS consumed, including on a non-2xx response. Pekko HTTP's connection pool
 * blocks on an undrained entity, so a run that discarded error bodies without reading them would
 * work until the first sustained run of errors and then hang — the failure mode being exactly the
 * one that shows up under upstream trouble, when the scraper most needs to keep working.
 */
final class PekkoTransport(system: ActorSystem[?]) extends Transport {

  private given ExecutionContext = system.executionContext
  private given classic: org.apache.pekko.actor.ActorSystem =
    system.classicSystem

  def get(
      url: String,
      headers: Map[String, String],
      timeout: FiniteDuration
  ): Future[HttpOutcome] = {
    val request = HttpRequest(
      uri = Uri(url),
      headers = headers.map { case (k, v) => RawHeader(k, v): HttpHeader }.toList
    )
    Http()
      .singleRequest(request)
      .flatMap { response =>
        response.entity
          .toStrict(timeout)
          .map { strict =>
            HttpOutcome(
              status = response.status.intValue,
              body = strict.data.utf8String,
              contentType = strict.contentType.toString
            )
          }
      }
  }
}

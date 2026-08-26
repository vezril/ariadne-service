package me.cference.ariadne.http

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.server.Route

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
 * Binds the HTTP surface and wires readiness into Coordinated Shutdown so that `/health` flips to
 * `DOWN` before the port unbinds (graceful drain, then terminate).
 */
object HttpServer:

  /**
   * Attempt to bind. The returned Future fails fast if the port is unavailable; callers decide exit
   * semantics.
   */
  def bind(routes: Route, host: String, port: Int)(using
      system: ActorSystem[?]
  ): Future[ServerBinding] =
    Http()(system).newServerAt(host, port).bind(Route.seal(routes))

  /**
   * Register readiness withdrawal (before unbind) and the binding's own unbind + terminate tasks
   * with Coordinated Shutdown, so a SIGTERM drains in-flight requests and exits cleanly.
   */
  def wireShutdown(binding: ServerBinding, readiness: AtomicBoolean)(using
      system: ActorSystem[?]
  ): Unit =
    val cs = CoordinatedShutdown(system)
    cs.addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "withdraw-readiness") { () =>
      readiness.set(false)
      Future.successful(Done)
    }
    binding.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds)
    ()

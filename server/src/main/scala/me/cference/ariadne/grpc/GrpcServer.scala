package me.cference.ariadne.grpc

import ariadne.v1.AriadneCatalogHandler
import me.cference.ariadne.config.GrpcConfig
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.settings.ServerSettings

import scala.concurrent.Future

/**
 * Binds the gRPC surface, on its own port, when it is enabled.
 *
 * Separate from the REST binding because gRPC needs HTTP/2 end to end. Sharing the REST port would
 * mean an h2c upgrade in front of the browser surface for no benefit, and `Route.seal` — which the
 * REST side wants — is the wrong error shape for a gRPC client.
 *
 * Returns `None` when disabled, so a caller can tell "off by configuration" from "bound". A failed
 * bind is a failed Future: a port that is already taken must stop the process rather than leave the
 * service up and silently unreachable on half its surface.
 */
object GrpcServer {

  def bind(service: AriadneCatalogService, cfg: GrpcConfig)(using
      system: ActorSystem[?]
  ): Option[Future[ServerBinding]] =
    Option.when(cfg.enabled) {
      val handler: HttpRequest => Future[HttpResponse] =
        AriadneCatalogHandler(service)(using system)
      // HTTP/2 is turned on for THIS binding only, through settings rather than the global
      // `pekko.http.server` block. gRPC needs h2c with prior knowledge; the REST server has no
      // use for it, and flipping it globally would change the browser surface's protocol
      // negotiation as a side effect of adding an unrelated port.
      val base = ServerSettings(system)
      val http2 = base.withPreviewServerSettings(
        base.previewServerSettings.withEnableHttp2(true)
      )
      Http()(system)
        .newServerAt(cfg.host, cfg.port)
        .withSettings(http2)
        .bind(handler)
    }
}

package me.cference.ariadne.http

import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

/**
 * Self-hosted API docs (§4, the Apollo precedent).
 *
 * Everything is served from the CLASSPATH: the OpenAPI document from our own resources, Swagger UI
 * from the webjar in the image. No CDN, no egress — this runs behind the Authelia gate on a homelab
 * cluster, and a docs page that silently needs the public internet is a docs page that is broken
 * exactly when someone is debugging why the internet is unreachable.
 */
final class DocsRoutes(swaggerUiVersion: String = "5.17.14") {

  private val SpecPath = "openapi/ariadne.yaml"

  val routes: Route =
    pathPrefix("docs") {
      concat(
        // The spec itself, so tooling (and the Insomnia collection) can consume it directly.
        path("openapi.yaml") {
          getFromResource(SpecPath, ContentTypes.`text/plain(UTF-8)`)
        },
        // Swagger UI's own index hard-codes the petstore example, so it is replaced with
        // a minimal page pointed at our spec rather than shipping a docs page that
        // documents someone else's API.
        (pathEndOrSingleSlash & get) {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, IndexHtml))
        },
        // The rest of the UI's assets, straight from the webjar.
        get {
          extractUnmatchedPath { path =>
            val asset = path.toString.stripPrefix("/")
            if asset.isEmpty || asset.contains("..") then complete(StatusCodes.NotFound)
            else getFromResource(s"META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$asset")
          }
        }
      )
    }

  private val IndexHtml: String =
    """<!DOCTYPE html>
      |<html lang="en">
      |<head>
      |  <meta charset="utf-8">
      |  <title>Ariadne API</title>
      |  <link rel="stylesheet" href="/docs/swagger-ui.css">
      |</head>
      |<body>
      |  <div id="swagger-ui"></div>
      |  <script src="/docs/swagger-ui-bundle.js"></script>
      |  <script>
      |    window.ui = SwaggerUIBundle({ url: '/docs/openapi.yaml', dom_id: '#swagger-ui' });
      |  </script>
      |</body>
      |</html>
      |""".stripMargin
}

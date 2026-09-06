package me.cference.ariadne.http

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The docs must work with no network at all — they are consulted precisely when something is
 * broken, and a page that quietly needs a CDN fails at the worst moment.
 */
final class DocsRoutesSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private val routes = new DocsRoutes().routes

  "the docs surface" should {

    "serve the OpenAPI document from our own resources" in {
      Get("/docs/openapi.yaml") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val body = responseAs[String]
        body should include("openapi: 3.0.3")
        body should include("/api/v1/resolutions/{id}/confirm")
      }
    }

    "serve an index that points at OUR spec, not Swagger's petstore example" in {
      Get("/docs") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        val html = responseAs[String]
        html should include("/docs/openapi.yaml")
        html.toLowerCase should not include "petstore"
      }
    }

    "serve Swagger UI assets from the CLASSPATH, with no external reference" in {
      // If the webjar were missing, this 404s and the docs page would silently render
      // blank — which looks like a working deploy until someone opens it.
      Get("/docs/swagger-ui.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }
      Get("/docs/swagger-ui-bundle.js") ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "reference NOTHING off-host — zero CDN egress is the requirement" in {
      Get("/docs") ~> routes ~> check {
        val html = responseAs[String]
        html should not include "http://"
        html should not include "https://"
        html should not include "unpkg"
        html should not include "cdn"
      }
    }

    "refuse a path traversal attempt" in {
      Get("/docs/..%2F..%2Fapplication.conf") ~> routes ~> check {
        status should (be(StatusCodes.NotFound) or be(StatusCodes.BadRequest))
      }
    }
  }
}

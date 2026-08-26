package me.cference.ariadne.http

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Route tests for the health surface. Readiness is an injected `() => Boolean` so the test controls
 * UP/DOWN without starting real probes.
 */
final class HealthRoutesSpec extends AnyFunSuite with Matchers with ScalatestRouteTest:

  private val version = "1.2.3-test"

  test("GET /health returns 200 UP with service + version when ready") {
    val route = HealthRoutes(version, () => true)
    Get("/health") ~> route ~> check {
      status shouldBe StatusCodes.OK
      val body = responseAs[String]
      body should include(""""status":"UP"""")
      body should include(""""service":"ariadne"""")
      body should include(s""""version":"$version"""")
    }
  }

  test("GET /health returns 503 DOWN when readiness is withdrawn") {
    val route = HealthRoutes(version, () => false)
    Get("/health") ~> route ~> check {
      status shouldBe StatusCodes.ServiceUnavailable
      responseAs[String] should include(""""status":"DOWN"""")
    }
  }

  test("unknown route returns 404 via the sealed route") {
    val route = HealthRoutes(version, () => true)
    Get("/nope") ~> Route.seal(route) ~> check {
      status shouldBe StatusCodes.NotFound
    }
  }

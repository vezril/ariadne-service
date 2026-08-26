package me.cference.ariadne.http

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class HelloRoutesSpec extends AnyFunSuite with Matchers with ScalatestRouteTest:

  test("GET / returns 200 with the hello body") {
    Get("/") ~> HelloRoutes() ~> check {
      status shouldBe StatusCodes.OK
      responseAs[String] shouldBe "Hello, World!"
    }
  }

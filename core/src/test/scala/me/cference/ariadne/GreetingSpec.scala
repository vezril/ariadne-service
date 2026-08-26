package me.cference.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class GreetingSpec extends AnyFunSuite with Matchers:

  test("message greets the World by default") {
    Greeting.message() shouldBe "Hello, World!"
  }

  test("message greets a given name") {
    Greeting.message("ariadne") shouldBe "Hello, ariadne!"
  }

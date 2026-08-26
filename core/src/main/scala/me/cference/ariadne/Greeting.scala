package me.cference.ariadne

/** Pure domain logic — no Pekko, exhaustively unit-testable. */
object Greeting:

  /** Build a greeting for the given name (defaults to "World"). */
  def message(name: String = "World"): String =
    s"Hello, $name!"

package me.cference.ariadne.domain

import scala.math.BigDecimal.RoundingMode

/** ISO-4217, closed to what the catalog actually sees. */
enum Currency {
  case CAD
  case USD
}

/**
 * A positive amount of money.
 *
 * Prices are facts, and a non-positive price is not a fact but a parse error, so the constructor
 * refuses it rather than storing a lie. Amounts carry two decimal places; construction rounds
 * HALF_UP once, and arithmetic keeps the scale so sums of lines compare cleanly against a stated
 * total.
 */
final case class Money private (amount: BigDecimal, currency: Currency) {

  def +(that: Money): Either[DomainError, Money] =
    if currency != that.currency then
      Left(DomainError.CurrencyMismatch(currency.toString, that.currency.toString))
    else Right(new Money(amount + that.amount, currency))

  def *(factor: BigDecimal): Money =
    new Money((amount * factor).setScale(2, RoundingMode.HALF_UP), currency)

  override def toString: String = s"${amount.setScale(2, RoundingMode.HALF_UP)} $currency"
}

object Money {

  def apply(amount: BigDecimal, currency: Currency): Either[DomainError, Money] = {
    val scaled = amount.setScale(2, RoundingMode.HALF_UP)
    if scaled <= 0 then Left(DomainError.NonPositiveAmount(amount.toString))
    else Right(new Money(scaled, currency))
  }

  def cad(amount: BigDecimal): Either[DomainError, Money] = apply(amount, Currency.CAD)

  def unsafe(amount: BigDecimal, currency: Currency = Currency.CAD): Money =
    apply(amount, currency).fold(e => throw new IllegalArgumentException(e.message), identity)

  /** Sum a non-empty list, failing on any currency mismatch. */
  def sum(monies: List[Money]): Either[DomainError, Option[Money]] =
    monies match {
      case Nil => Right(None)
      case head :: tail =>
        tail
          .foldLeft[Either[DomainError, Money]](Right(head))((acc, m) => acc.flatMap(_ + m))
          .map(Some(_))
    }
}

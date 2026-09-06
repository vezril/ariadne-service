package me.cference.ariadne.domain

import scala.math.BigDecimal.RoundingMode

/**
 * What kind of thing a unit measures. Comparisons only ever make sense within a dimension — 750 mL
 * and 1 kg are not "far apart", they are incomparable, and the scorer (§6.3) must treat those two
 * cases differently.
 */
enum Dimension {
  case Mass
  case Volume
  case Count
}

/**
 * A unit of pack size, with its factor to the dimension's base unit (grams for Mass, millilitres
 * for Volume, each for Count).
 */
enum MeasureUnit(val dimension: Dimension, val toBaseFactor: BigDecimal, val label: String) {
  case Gram extends MeasureUnit(Dimension.Mass, BigDecimal(1), "g")
  case Kilogram extends MeasureUnit(Dimension.Mass, BigDecimal(1000), "kg")
  case Millilitre extends MeasureUnit(Dimension.Volume, BigDecimal(1), "mL")
  case Litre extends MeasureUnit(Dimension.Volume, BigDecimal(1000), "L")
  case Each extends MeasureUnit(Dimension.Count, BigDecimal(1), "ea")
}

/**
 * A pack size: 750 mL, 1 kg, 12 ea.
 *
 * Load-bearing for two things — unit-price normalisation, and the size signal in fuzzy matching.
 * Per §6.7 size is part of identity, not a description of it: a 454 g and a 250 g of the same brand
 * are different products, so an incompatible size is a strong NEGATIVE match signal.
 */
final case class Quantity private (amount: BigDecimal, unit: MeasureUnit) {

  def dimension: Dimension = unit.dimension

  /**
   * Amount expressed in the dimension's base unit — the only sane axis for comparing 750 mL against
   * 0.75 L.
   */
  def inBase: BigDecimal = amount * unit.toBaseFactor

  def sameDimensionAs(that: Quantity): Boolean = dimension == that.dimension

  /**
   * Equal size within a relative tolerance, comparing in base units. `Left` when the dimensions
   * differ — incomparable is not the same as unequal.
   */
  def isCloseTo(
      that: Quantity,
      tolerance: BigDecimal = BigDecimal("0.02")
  ): Either[DomainError, Boolean] =
    if !sameDimensionAs(that) then Left(DomainError.IncompatibleUnits(unit.label, that.unit.label))
    else {
      val (a, b) = (inBase, that.inBase)
      val larger = a.max(b)
      Right(larger == 0 || ((a - b).abs / larger) <= tolerance)
    }

  override def toString: String =
    s"${amount.bigDecimal.stripTrailingZeros.toPlainString} ${unit.label}"
}

object Quantity {

  def apply(amount: BigDecimal, unit: MeasureUnit): Either[DomainError, Quantity] =
    if amount <= 0 then Left(DomainError.NonPositiveQuantity(amount.toString))
    else Right(new Quantity(amount, unit))

  def unsafe(amount: BigDecimal, unit: MeasureUnit): Quantity =
    apply(amount, unit).fold(e => throw new IllegalArgumentException(e.message), identity)
}

/**
 * Price normalised per reference amount — $/100 g, $/L, $/ea — so two pack sizes of the same
 * product become comparable. Computed upstream during fact extraction (§2.6, ported from Demeter's
 * `UnitPriceCalculator`).
 */
final case class UnitPrice private (amount: BigDecimal, currency: Currency, per: Quantity) {
  override def toString: String = s"$amount $currency / $per"
}

object UnitPrice {

  /**
   * Reference amounts per dimension: mass is quoted per 100 g, volume per litre, count per item —
   * the conventions Canadian shelf tags use.
   */
  private def reference(dimension: Dimension): Quantity = dimension match {
    case Dimension.Mass => Quantity.unsafe(BigDecimal(100), MeasureUnit.Gram)
    case Dimension.Volume => Quantity.unsafe(BigDecimal(1), MeasureUnit.Litre)
    case Dimension.Count => Quantity.unsafe(BigDecimal(1), MeasureUnit.Each)
  }

  /**
   * Trusted rehydration for the journal. The value was validated before it was ever persisted, so
   * replay reconstructs rather than re-derives it — re-deriving would silently "fix" history if the
   * formula ever changed, which is the opposite of what an event store is for.
   */
  def rehydrate(amount: BigDecimal, currency: Currency, per: Quantity): UnitPrice =
    new UnitPrice(amount, currency, per)

  /** Derive the unit price of `price` for a pack of `size`. */
  def from(price: Money, size: Quantity): UnitPrice = {
    val ref = reference(size.dimension)
    val perBase = price.amount / size.inBase
    val amount = (perBase * ref.inBase).setScale(4, RoundingMode.HALF_UP)
    new UnitPrice(amount, price.currency, ref)
  }
}

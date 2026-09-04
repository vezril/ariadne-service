package me.cference.ariadne.ingest.flipp

import me.cference.ariadne.domain.{Area, ChainId, Confidence, Money, PriceScope}
import me.cference.ariadne.domain.resolution.MatchSubject

/**
 * The wire/domain boundary — where Flipp's model becomes Ariadne's, and where the disagreements
 * between them are resolved VISIBLY rather than inside a decoder.
 *
 * There is exactly one such disagreement and it matters: `FlippMoney` permits zero (as Demeter's
 * `Money` does), Ariadne's `Money` refuses non-positive. A 0.00 flyer item is a real advertised
 * price — carrier handsets on postpaid contracts, 3 rows in 35,088 measured — so the number is TRUE
 * and using it as a price would be FALSE. It is refused here, and refused under its own counted
 * reason so it never hides inside a decode-failure total.
 */
object FlippMapper {

  /** What a mapped item becomes: everything needed to observe a price, except the product. */
  final case class Observation(
      subject: MatchSubject,
      price: Money,
      scope: PriceScope,
      observedAt: java.time.Instant,
      priceConfidence: Confidence,
      sizeConfidence: Confidence
  )

  /**
   * A scraped price is a `Regional(chain, area)` fact and never an `Exact` one (§2.3.1).
   *
   * The feed carries no franchise identifier at all — `merchant_id` plus the QUERIED postal code is
   * the finest grain it offers — so attributing one to a store would fabricate precision that was
   * never observed.
   */
  def scopeFor(chain: ChainId, postal: PostalCode): PriceScope =
    PriceScope.Regional(chain, Area(postal.fsa))

  def map(
      item: FlyerItem,
      chain: ChainId,
      postal: PostalCode,
      priceConfidence: Confidence,
      sizeConfidence: Confidence
  ): Either[SkipReason, Observation] =
    item.currentPrice match {
      case None => Left(SkipReason.NoPrice)
      case Some(flipp) =>
        Money.cad(BigDecimal(flipp.cents) / 100) match {
          // Money refuses non-positive, which for a flyer means a genuine 0.00.
          case Left(_) => Left(SkipReason.ZeroPriced)
          case Right(money) =>
            Right(
              Observation(
                // rawName, not the split name: the bilingual split is the matcher's
                // business and the decoder deliberately leaves `name` empty.
                subject = MatchSubject(name = item.rawName),
                price = money,
                scope = scopeFor(chain, postal),
                observedAt = item.validFrom,
                priceConfidence = priceConfidence,
                sizeConfidence = sizeConfidence
              )
            )
        }
    }
}

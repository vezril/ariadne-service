package me.cference.ariadne.ingest.flipp

/**
 * Quirk #1, as its own named function rather than a line inside an orchestrator.
 *
 * Per-flyer item responses carry NO merchant — merchant is a property of the FLYER, not the item —
 * so every item must be re-stamped from the flyer that owned the response, which is the
 * authoritative source.
 *
 * This is the single most dangerous thing in the port. Miss it and every item decodes to
 * `UnresolvedMerchant` (merchant 0), every product collides into one identity, and the run reports
 * **correct row counts** the whole way. Demeter's own words: "a corrupt corpus that looks fine".
 *
 * It lives here, alone and tested, because a step that must never be skipped should not be a line
 * someone can drop while refactoring a for-comprehension. In demeter it is `DailyRun.scala:136`;
 * the port makes it callable and provable instead.
 */
object MerchantRestamp {

  /** Re-stamp every item with the flyer's merchant. */
  def apply(flyer: Flyer, items: List[FlyerItem]): List[FlyerItem] =
    items.map(_.copy(merchantId = flyer.merchantId))

  /**
   * True when a batch still carries the unresolved sentinel — i.e. the re-stamp did not happen.
   *
   * Exposed so the pipeline can ASSERT rather than hope. Merchant 0 reaching the resolver is not a
   * degraded run, it is a corrupt one, and it must fail loudly at the point of detection.
   */
  def hasUnresolved(items: List[FlyerItem]): Boolean =
    items.exists(_.merchantId == FlippDecoders.UnresolvedMerchant)
}

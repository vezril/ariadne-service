package me.cference.ariadne.domain

/**
 * How precisely a price fact is scoped (§2.3.1).
 *
 * NOT a confidence. A confidence says "we might have misread it"; a scope says "here is exactly
 * what we observed and where it holds." Conflating them would let a precise reading of an imprecise
 * fact look like a precise fact.
 *
 * The distinction exists because the flyer feed cannot express a franchise: Flipp scopes everything
 * by `(merchant_id, postal_code)`, so a scraped price is a claim about a CHAIN in a REGION — a set
 * of franchises — while a receipt is a claim about one store. Writing a flyer price as N per-store
 * facts would fabricate N observations from one, which is exactly the inference the facts-only
 * charter pushes downstream to Demeter.
 *
 * Naming note: DESIGN §2.3.1 sketches the second case as `Area(chainId, area)`. That does not
 * compile — the case would shadow the `Area` type in its own parameter list — so it is `Regional`
 * here, and the doc's snippet says so too.
 */
enum PriceScope {

  /** A receipt, a manual entry, or a store-specific promo: one store, no inference. */
  case Exact(storeId: StoreId)

  /** A flyer: this chain, this region, however many franchises that covers. */
  case Regional(chainId: ChainId, area: Area)

  /**
   * The stream discriminator — one price stream per product x scope. Stable and collision-free,
   * since ids and postal prefixes exclude '|'.
   */
  def key: String = this match {
    case Exact(storeId) => s"store:${storeId.value}"
    case Regional(chainId, area) => s"area:${chainId.value}:${area.postalPrefix}"
  }

  /**
   * True when this fact speaks for exactly one store — the read side ranks these above regional
   * ones when both cover the same store (§2.3.1).
   */
  def isExact: Boolean = this match {
    case _: Exact => true
    case _: Regional => false
  }
}

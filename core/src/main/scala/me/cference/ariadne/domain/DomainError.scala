package me.cference.ariadne.domain

/**
 * Every way a `decide` can refuse a command.
 *
 * Total and enumerated on purpose: `decide` returns `Either[DomainError, List[Event]]`, never
 * throws, so refusals are values the caller must handle.
 */
enum DomainError(val message: String) {
  case EmptyName extends DomainError("product name must not be blank")
  case EmptyStoreName extends DomainError("store name must not be blank")
  case InvalidConfidence(got: String)
      extends DomainError(s"confidence must be within [0,1], got $got")
  case InvalidGtin(got: String, why: String) extends DomainError(s"invalid GTIN '$got': $why")
  case NonPositiveAmount(got: String) extends DomainError(s"amount must be positive, got $got")
  case NonPositiveQuantity(got: String) extends DomainError(s"quantity must be positive, got $got")
  case CurrencyMismatch(a: String, b: String) extends DomainError(s"currency mismatch: $a vs $b")
  case IncompatibleUnits(a: String, b: String) extends DomainError(s"incompatible units: $a and $b")

  case AlreadyRegistered extends DomainError("aggregate is already registered")
  case NotRegistered extends DomainError("aggregate does not exist yet")
  case ProductIsTombstone(canonical: ProductId)
      extends DomainError(
        s"product is merged into ${canonical.value}; writes must target the canonical id"
      )
  case CannotMergeIntoSelf extends DomainError("a product cannot be merged into itself")
  case AlreadyDeprecated extends DomainError("product is already deprecated")

  case ObservationInFuture(at: String) extends DomainError(s"observedAt is in the future: $at")
  case PurchaseInFuture(at: String) extends DomainError(s"purchasedAt is in the future: $at")
  case EmptyPurchase extends DomainError("a purchase must have at least one line")
  case PurchaseTotalMismatch(stated: String, computed: String)
      extends DomainError(s"stated total $stated does not match the sum of lines $computed")
  case AlreadyResolved
      extends DomainError("this resolution case is already decided; a change of mind is a new case")
  case NotACandidate(productId: ProductId)
      extends DomainError(
        s"${productId.value} was not among the offered candidates; use Reject to create a different product"
      )
  case AlreadyVoided extends DomainError("purchase is already voided")
}

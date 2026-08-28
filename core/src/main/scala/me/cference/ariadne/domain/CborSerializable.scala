package me.cference.ariadne.domain

/**
 * Marker for everything that goes into the journal or over the wire between nodes.
 *
 * A PLAIN trait on purpose: `core` has zero Pekko dependencies (DESIGN §1), so the binding from
 * this marker to the CBOR serializer lives in the server's `serialization.conf`, not here. The
 * domain says "this is persisted"; the runtime decides how.
 */
trait CborSerializable

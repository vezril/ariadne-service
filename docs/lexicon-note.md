# Note to the Lexicon session — the Product Catalog's async contract

**From:** the Ariadne session (`ariadne-service`)
**Date:** 2026-09-07
**Re:** (1) a namespace collision that should be settled before either side ships, and
(2) a proposed contract for Ariadne's three published topics.

Per the working agreements this is a **proposal, not a landing**. Contracts ripple everywhere;
Ariadne is one producer with two prospective consumers, and the shape below is what it actually
emits today, offered as a starting point rather than an answer. Nothing here is a request to
change a contract another service already depends on.

---

## 1. Two different things are called Ariadne

`messages/src/main/protobuf/codex/messages/v1/catalog.proto` says:

> RESERVED for the future Ariadne extraction — a planned analytics consumer of `catalog.events`
> (Artemis emits; Ariadne consumes, rebuildable from history).

Its shapes are `PostCreated`, `TagsChanged`, `PostPurged`. That is **Artemis's media catalog**, with
an analytics consumer someone planned to name Ariadne.

**This service is a different Ariadne.** It is the **Product Catalog** — products, prices,
purchases — EventStorming-validated and seeded 2026-08-26 per `codex/docs/product-catalog.md`, and
extracted from Dionysus. It is a *producer*, not a consumer of Artemis. Demeter and Dionysus consume
it.

So the Lexicon currently has one file named `catalog` reserving a namespace for a service that
doesn't exist, under a name that now belongs to a service that does — and the real Product Catalog
has no schema at all.

**We have no view on which one should be renamed.** The Artemis reservation predates our naming, so
the older claim may well be the one to keep. But it should be settled deliberately by whoever owns
the namespace rather than resolved by whichever of us files first, and it is easier now than after
either ships. Flagging it, not proposing a fix.

## 2. What Ariadne publishes

Three topics, self-provisioned at startup, published from an event-sourced outbox projection over
the journal (offsets commit after a successful publish, so at-least-once; the deterministic
`{persistenceId}:{seqNr}` rides as the broker's `idempotency_key`, so a republish collapses at the
source rather than making every consumer responsible for detecting duplicates).

| Topic | Event | Required consumer |
|---|---|---|
| `product.price.observed` | `PriceObserved` | **Demeter** — deal evaluation. This is the one with a hard dependency. |
| `product.registered` | `ProductRegistered` (+ merges as `status=MERGED_INTO`) | Demeter / Dionysus, both optional in v1 |
| `purchase.recorded` | `PurchaseRecorded` | none in v1; future budgeting (Plutus) |

The proposed shapes are in **`docs/lexicon-proposal-catalog.proto`** in this repo. Payloads are
currently hand-encoded to that proto's canonical protobuf JSON, so adopting the contract swaps the
encoder without changing a byte on the wire.

### Three shape decisions worth arguing about

These are the ones where getting it wrong is *silent* downstream, so we'd rather have them
challenged now.

**Price scope is a tagged union, never a bare store id.** A flyer feed cannot name an individual
franchise — merchant plus queried postal code is the finest grain it offers — so a scraped price is
an `area{chain_id, area}` fact, while a receipt is `exact{store_id}`. Recording an area price as one
store's price under-counts coverage; expanding it to N member stores fabricates N facts. Deal
alerting is wrong in both directions without the tag, and wrong *quietly*.

**Both confidences ride every price.** `size_confidence` in particular is not decoration: Demeter's
match confidence is the weakest link of its own split confidence and this one, so omitting it lets
downstream judgment read too high.

**Money is a decimal string.** `4.99` has no exact binary representation; a consumer comparing
prices for a deal would be comparing rounding artefacts.

## 3. One open question we are deliberately not deciding

Merges publish on `product.registered` with `status = MERGED_INTO` and `merged_into` set, so a
consumer holding a product id learns to re-point. Whether that overloads the topic's semantics, or
wants a dedicated `product.merged`, is Ariadne's DESIGN §10.1 — **a question for the Lexicon rather
than a call Ariadne should make alone**, and one with no urgency until a consumer actually needs
merges.

## 4. What we need, and what we don't

**Don't need:** anything to keep building. The publisher is implemented and tested against a stubbed
transport.

**Do need, eventually:** a ruling on the name collision, and the contract landed — because until it
is, **a consumer building against these payloads is building against a proposal**, and Demeter
should know that before writing a consumer rather than after.

**Unrelated but adjacent, for the Codex session rather than yours:** `ariadne-service` cannot
resolve `io.codex` artifacts in CI (no `LEXICON_TOKEN` secret on the repo), which is why the
publisher's PR is currently red. That is a credentials issue on our side, not a contract one.

---

*Working agreements: this note is a lead and a coordination plan. Sequencing lands on Calvin's word,
and you own your repo.*

# Ariadne — service technical design

**Status:** DESIGN (2026-08-26). No code exists yet; this doc is the implementation design for
`ariadne-service`. The **domain source of truth is `~/Code/codex/docs/product-catalog.md`** (the
EventStorming-validated Product Catalog design — aggregates, events, policies, the two hot spots).
This doc does not restate that design; it builds on it and cross-references it. Where the two ever
disagree, the codex doc wins and this one gets fixed.

**The two load-bearing rules** (from `README.md` / `AGENTS.md`, non-negotiable):

1. **Facts-only — never a God-object.** Ariadne owns identity + market facts (Product, Store,
   PriceObservation, Purchase). Nutrition stays in Dionysus (by-id reference); deal-logic/CPI stays
   in Demeter. No consumer-specific attributes, ever.
2. **Identity resolution is the hard part and the point** (hot spot #1). It is designed
   first-class here (§6), not bolted on.

---

## 1. Stack & shape

Standard constellation service (the Apollo/Artemis idiom):

- **Scala 3 (3.3.x LTS) + Apache Pekko**, sbt-dynver versioning, two modules:
  - `core/` — pure domain, zero Pekko deps: value types, state/command/event ADTs,
    `decide`/`evolve`, the **matching algebra** (normalizers + scorer — pure functions,
    exhaustively unit-tested). Following `apollo-storage/core`.
  - `server/` — Pekko runtime: event-sourced entities (Pekko Persistence, Postgres journal via the
    shared **pg-service** chart), Pekko Projections (read models + Hermes publisher), gRPC server,
    small REST surface + self-hosted `/docs`, `/health` + `/metrics`, correlation-id tracing.
- **HermesMQ** for async events, using the **official Scala `hermesmq-client`** from the `hermesmq`
  repo, **git-installed pinned to the broker tag `@v1.13.0`** (bump when the broker's
  client-pinning tag advances). v1.13.0 brings correlation-id tracing: we **adopt** an incoming
  `correlationId` when present, **mint** one when absent, **journal** it with every event, and
  **echo** it on every Hermes publish and gRPC/REST response (§8).
- **Contract in the Lexicon.** The `ariadne.v1` proto in §4 and the Hermes message schemas in §5
  are **proposals to the Lexicon session — propose, don't land**. Ariadne consumes the published
  stubs (GitHub Packages, the Apollo precedent).
- **Topics are self-provisioned** at startup, idempotently (409 = already exists), names in
  Ariadne's own chart/config (§5).
- Helm chart + `codex/apps/ariadne/ariadne.yaml` pin authored in the codex repo (Codex session's
  tree — coordinate, don't write there). Insomnia collection at `insomnia/ariadne.yaml`, kept live
  as the API evolves.

---

## 2. Event-sourcing model

All aggregates follow the constellation shape:

```scala
def decide(state: State, cmd: Command): Either[DomainError, List[Event]]
def evolve(state: State, event: Event): State
```

Pekko Persistence `EventSourcedBehavior` per entity, Postgres journal (pg-service chart,
r2dbc plugin — the Apollo alignment: pekko 1.2.x / r2dbc 1.1.x / projection 1.1.x). Events are
`CborSerializable` ADTs; every event envelope carries `correlationId` and `observedAt`/`recordedAt`
as domain time distinct from journal time.

Two aggregates are *decision-heavy* (Product, ResolutionCase); three are *fact-append* (Store is
near-trivial; **PriceObservation is literally an append-only stream** and **Purchase is an
immutable fact** — for these, `decide` is mostly validation and `evolve` is mostly bookkeeping.
That asymmetry is expected and correct: the facts are the easy part; identity is the hard part.)

### 2.1 Product

Identity + market description. **No nutrition, no deal fields.**

```scala
// core — illustration, not final code
final case class ProductId(value: String)          // ULID
final case class Gtin(value: String)               // validated GTIN-8/12/13/14, check digit
final case class ListingKey(storeId: StoreId, externalId: String) // retailer's own listing id

enum ProductStatus:
  case Provisional   // auto-created from a scraped listing, unreviewed
  case Active        // confirmed (human or strong-key)
  case MergedInto(canonical: ProductId)  // tombstone redirect
  case Deprecated

final case class ProductState(
    id: ProductId,
    name: String,                 // canonical display name
    brand: Option[String],
    category: Option[String],     // coarse market category (produce/dairy/…), NOT food-role
    size: Option[Quantity],       // e.g. 750 mL, 1 kg — needed for unit-price
    gtins: Set[Gtin],             // strong keys (a product can carry several pack GTINs)
    aliases: Set[String],         // normalized alternate names seen in the wild
    listings: Set[ListingKey],    // retailer listings resolved to this product
    status: ProductStatus
)

enum ProductCommand:
  case RegisterProduct(name: String, brand: Option[String], size: Option[Quantity],
                       gtin: Option[Gtin], origin: Origin, correlationId: CorrelationId)
  case AddIdentifier(gtin: Gtin, ...)
  case AddAlias(alias: String, ...)
  case LinkListing(key: ListingKey, resolution: ResolutionRef, ...)
  case MergeInto(canonical: ProductId, resolution: ResolutionRef, ...)   // this id becomes tombstone
  case Deprecate(reason: String, ...)

enum ProductEvent:
  case ProductRegistered(id: ProductId, name: String, brand: Option[String],
                         size: Option[Quantity], gtin: Option[Gtin],
                         origin: Origin, status: ProductStatus)   // → Hermes product.registered
  case ProductIdentifierAdded(gtin: Gtin)
  case ProductAliasAdded(alias: String)
  case ListingLinked(key: ListingKey, confidence: Confidence, how: MatchMethod)
  case ProductMerged(into: ProductId)                             // tombstone written on the LOSER
  case ProductAbsorbed(loser: ProductId, gtins: Set[Gtin],
                       aliases: Set[String], listings: Set[ListingKey]) // written on the WINNER
  case ProductDeprecated(reason: String)
```

`decide` guards: no writes to a `MergedInto` tombstone except reads-with-redirect; GTIN uniqueness
is enforced via the resolver index (§6.4), not the aggregate (cross-entity invariant → checked at
resolve time, reconciled by merge if a race slips one through — the EventStorming stance: facts
first, repair explicitly).

**`Origin`** records where a product came from: `Manual` (Calvin), `Scrape(ListingKey)` (auto from
an unmatched listing), `Migration(source)` (Demeter/Dionysus backfill). Provisional products come
from `Scrape` and surface in the review queue (§6.5).

### 2.2 Store

Small reference aggregate — a retailer/banner + optional location.

```scala
final case class StoreId(value: String)
final case class StoreState(id: StoreId, name: String, chain: Option[String],
                            location: Option[String], active: Boolean)

enum StoreCommand: case RegisterStore(...); case UpdateStoreDetails(...); case DeactivateStore(...)
enum StoreEvent:   case StoreRegistered(...); case StoreDetailsUpdated(...); case StoreDeactivated(...)
```

Nothing clever here on purpose. Stores are also where scraper source config attaches
(which Flipp merchant / site maps to which StoreId) — config, not domain state.

### 2.3 PriceObservation — an append-only stream

**This aggregate IS its event stream.** Entity id = `price|{productId}|{storeId}` (one stream per
product×store pair — keeps entities small, recovery fast, and the natural query axis aligned with
the stream). State is only what validation needs:

```scala
final case class PriceStreamState(
    productId: ProductId, storeId: StoreId,
    lastObserved: Option[(Instant, Money)],   // dedup window
    count: Long
)

enum PriceCommand:
  case ObservePrice(price: Money, observedAt: Instant, source: PriceSource,
                    unitPrice: Option[UnitPrice],  // normalized $/100g, $/L … computed upstream
                    promo: Option[PromoFlag],      // was-this-a-sale FACT (not a judgment)
                    correlationId: CorrelationId)

enum PriceEvent:
  case PriceObserved(productId: ProductId, storeId: StoreId, price: Money,
                     unitPrice: Option[UnitPrice], promo: Option[PromoFlag],
                     observedAt: Instant, source: PriceSource)   // → Hermes product.price.observed

enum PriceSource:
  case Scrape(scraper: String)     // flyer/site scrape (the policy that moved in from Demeter)
  case Purchase(purchaseId: PurchaseId)  // actual price paid — the §2.4 policy
  case Manual                      // Calvin typed it
  case Backfill(origin: String)    // migration replay (carries ORIGINAL observedAt)
```

`decide` = validate (positive money, known currency, sane timestamp, dedup: identical
price+source within the same calendar day is a no-op). **There is no update/delete** — a wrong
observation is corrected by a subsequent `PriceObservationRetracted(reason)` event (rare, manual),
never by mutation. `promo` is a *fact* ("flyer said 30% off"); whether that's a *good deal* is
Demeter's judgment — the line stays.

The **scraping policy lives here** (moved in from Demeter): a scheduled Pekko-actor per source
(`whenever schedule due then ObservePrice`) runs the Flipp/retailer sources, pushes raw listings
through the resolver (§6), and appends observations. Scraper sources are pluggable
(`PriceSourceAdapter`), config-driven, with per-source schedules in the chart.

### 2.4 Purchase — immutable fact

```scala
final case class PurchaseId(value: String)
final case class PurchaseLine(productId: ProductId, quantity: BigDecimal,
                              pricePaid: Money, lineTotal: Money)

final case class PurchaseState(id: PurchaseId, storeId: StoreId, purchasedAt: Instant,
                               lines: List[PurchaseLine], total: Money,
                               source: PurchaseSource, voided: Boolean)

enum PurchaseCommand:
  case RecordPurchase(storeId: StoreId, purchasedAt: Instant, lines: List[PurchaseLine],
                      total: Money, source: PurchaseSource, correlationId: CorrelationId)
  case VoidPurchase(reason: String, ...)   // corrections are new facts, not edits

enum PurchaseEvent:
  case PurchaseRecorded(...)               // → Hermes purchase.recorded
  case PurchaseVoided(reason: String)
```

**Purchases are immutable facts** — one `PurchaseRecorded` per receipt; a mistake is voided and
re-recorded (both events persist; the audit trail is the point). Two standing policies:

- **whenever `PurchaseRecorded` then** append each line's actual-price-paid to the matching
  PriceObservation stream as `PriceObserved(source = Purchase(id))` — actual prices are the
  highest-quality price facts we ever get (implemented as a projection-driven process manager,
  §3, so it's at-least-once + idempotent, not a fragile in-band side effect).
- `purchase.recorded` on Hermes is the **future budgeting feed** (Plutus-shaped, per the codex
  doc) — nothing to build now, but the event carries everything budgeting needs (store, time,
  lines, totals) so we never have to re-model it.

### 2.5 ResolutionCase — the review aggregate (supports hot spot #1)

The four fact aggregates above are deliberately dumb. The fifth aggregate carries the workflow
state of ambiguous identity matches (full design §6.5):

```scala
enum ResolutionState:
  case Pending(subject: MatchSubject, candidates: List[ScoredCandidate])
  case Resolved(outcome: ResolutionOutcome)   // Confirmed(productId) | NewProduct(productId)
                                              // | MergedProducts(winner, loser) | Rejected

enum ResolutionCommand: case ProposeResolution(...); case Confirm(...); case Reject(...)
                        case RequestMerge(...); case RequestSplit(...)
enum ResolutionEvent:   case ResolutionProposed(...); case ResolutionConfirmed(...)
                        case ResolutionRejected(...); case MergeRequested(...); case SplitRequested(...)
```

Confirm/merge decisions fan out as commands to the Product aggregates (process manager over the
ResolutionCase journal). This keeps human-review state OUT of Product — Product stays facts.

---

## 3. Projections (read models)

All read models are **Pekko Projections over the Postgres journal — rebuildable from scratch by
replay** (drop tables + reset offsets = full rebuild; this is the recovery story and also the
schema-evolution story for read models). Offsets in the standard projection offset table.

| Projection | Source streams | Tables (sketch) | Serves |
|---|---|---|---|
| **product-catalog** | Product, Store | `products(id, name, brand, category, size, status, merged_into)` · `product_gtins(gtin→product_id)` · `product_aliases` · `product_listings(store_id, external_id → product_id)` · `stores` | GetProduct / ListProducts / SearchProducts; redirect-following for merged ids |
| **price-history** | PriceObservation | `price_history(product_id, store_id, observed_at, price, unit_price, promo, source, correlation_id)` — the shared read model from the EventStorming wall | GetPriceHistory (product × store × time) |
| **current-price** | PriceObservation | `current_price(product_id, store_id, price, unit_price, observed_at, source)` — one row per pair, last-write-wins by `observedAt` | GetCurrentPrice (the shopping-list NOW call) |
| **purchase-history** | Purchase | `purchases`, `purchase_lines` | ListPurchases; future budgeting queries |
| **resolver/match index** | Product | `match_index(product_id, normalized_name, name_tokens, trigrams tsvector/pg_trgm, brand_norm, size_class)` + the gtin + listing tables above | ResolveProduct scoring (§6.4); the GTIN-uniqueness guard |
| **review-queue** | ResolutionCase | `resolution_cases(id, state, subject, candidates_json, created_at)` | ariadne-ui review screens |
| **hermes-publisher** | Product, PriceObservation, Purchase | (offset only) | §5 — the outbox projection |
| **price-append process manager** | Purchase | (offset only) | issues `ObservePrice(source=Purchase)` per line (§2.4) |

Notes: `pg_trgm` + a normalized-token column is enough for v1 fuzzy matching — no external search
engine; keep it in the one Postgres. Projections are tagged (`product`, `price`, `purchase`,
`resolution`) for parallelism if streams grow.

---

## 4. Synchronous read surface — gRPC (contract → Lexicon, PROPOSAL)

**Rule applied:** blocked-and-waiting → gRPC (typed, service-to-service); reaction → Hermes.
The canonical awaiting caller: **Dionysus generating a shopping list needs product identity and
current price NOW.** Proto shape to **propose to the Lexicon session** (`ariadne/v1/ariadne.proto`
— do not land unilaterally):

```protobuf
service AriadneCatalog {
  // Identity
  rpc GetProduct        (GetProductRequest)        returns (GetProductResponse);      // follows merge redirects; response says so
  rpc ListProducts      (ListProductsRequest)      returns (ListProductsResponse);    // paged; filter by status/category
  rpc SearchProducts    (SearchProductsRequest)    returns (SearchProductsResponse);  // fuzzy, for UI/typeahead
  rpc ResolveProduct    (ResolveProductRequest)    returns (ResolveProductResponse);  // THE resolver (§6): gtin and/or name/brand/size in →
                                                                                      // Matched{id, confidence} | Ambiguous{candidates} | NoMatch
  // Prices
  rpc GetCurrentPrice   (GetCurrentPriceRequest)   returns (GetCurrentPriceResponse); // product_id (+optional store_id) → best/current per store
  rpc GetPriceHistory   (GetPriceHistoryRequest)   returns (stream PricePoint);       // product × optional store × time range; server-streamed
  // Purchases
  rpc ListPurchases     (ListPurchasesRequest)     returns (ListPurchasesResponse);   // time range, paged
  // Writes that a caller awaits (thin command surface)
  rpc RegisterProduct   (RegisterProductRequest)   returns (RegisterProductResponse);
  rpc RecordPurchase    (RecordPurchaseRequest)    returns (RecordPurchaseResponse);
}
```

Every request/response carries `correlation_id` (echoed; minted if absent). `ResolveProduct` is
deliberately in the sync surface: Dionysus resolving an ingredient is a blocked-and-waiting call.
Reads hit projections only, never entities (CQRS held strictly); the two write RPCs go through the
entity `decide`.

**Consumers of gRPC:** Dionysus (GetProduct, ResolveProduct, GetCurrentPrice — shopping list &
ingredient linking), Demeter (GetProduct, GetPriceHistory — on-demand history for scoring
context), ariadne-ui's BFF, future Plutus (ListPurchases).

### REST + `/docs`

A **small REST surface exists for the browser** (constellation rule: gRPC = internal, REST =
browser/BFF), serving **ariadne-ui only**: the review queue (list/confirm/reject/merge/split),
product CRUD + search, manual purchase entry, manual price entry. It is a thin mirror of the same
commands/queries — generated from the same Lexicon contract, no second behavior. With it, per the
Apollo v0.13.0 precedent: **self-hosted Swagger `/docs`** (OpenAPI on-classpath, zero
CDN/egress), and the maintained **Insomnia collection** covering every REST endpoint. `/health`
(+readiness: journal reachable, projections not stalled, Hermes reachable-or-degraded) and
Prometheus `/metrics` with Hera scrape annotations round out the HTTP server.

---

## 5. HermesMQ publishing (async surface)

### Topics — self-provisioned at startup, idempotently

| Topic | Domain event | Payload (Lexicon schema, PROPOSAL) | Subscribers |
|---|---|---|---|
| `product.registered` | `ProductRegistered` (and `ProductMerged` tombstones — see below) | product id, name, brand, size, gtin?, status, origin, `correlationId` | **Demeter** (optional: refresh watchable-product cache) · **Dionysus** (optional: new-product awareness for ingredient linking); both may ignore it in v1 |
| `product.price.observed` | `PriceObserved` | product id, store id, price, unit price, promo?, observed_at, source, `correlationId` | **Demeter** (REQUIRED — the deal-evaluation policy: *whenever PriceObserved on a watched product then evaluate*) |
| `purchase.recorded` | `PurchaseRecorded` | purchase id, store id, purchased_at, lines[], total, source, `correlationId` | none in v1 · **future budgeting (Plutus)** · Dionysus MAY subscribe later for pantry restock |

Topic names live in Ariadne's chart/config (env-overridable, the Artemis idiom); the service
creates them at startup via the client, treating already-exists/409 as success. Never hand-created
in the cluster.

**Merge propagation:** `ProductMerged` publishes on `product.registered` as a
`status=merged_into` message (consumers holding a product id learn to re-point). If that overloads
the topic semantics in practice, propose a dedicated `product.merged` topic to the Lexicon as
v1.1 — flagged as an open question, not landed.

### The publisher — an event-sourced outbox projection

No dual-write. The Hermes publisher is a **Pekko Projection over the journal** (`hermes-publisher`
in §3): it reads committed events by tag, maps domain event → Lexicon message, publishes via the
pinned `hermesmq-client` (@v1.13.0), and only then advances its offset.

- **At-least-once, restart-safe:** offset commits after successful publish → crash between
  publish and commit ⇒ re-publish, never a lost event. Consumers must be idempotent (they already
  must be, constellation-wide); every message carries a deterministic `messageId`
  (`{persistenceId}:{seqNr}`) so duplicates are detectable.
- **Ordering:** per-entity order is guaranteed by the journal; that's the only ordering promised.
- **Correlation:** the journaled `correlationId` is carried onto the published message (v1.13.0
  tracing) — a scrape → resolve → observe → deal-alert chain is traceable end-to-end across
  Ariadne → Hermes → Demeter.
- **Backpressure/outage:** Hermes down ⇒ the projection retries with backoff and simply lags
  (offset doesn't advance); the write side is unaffected. `/health` readiness reports the lag;
  a `ariadne_hermes_publisher_lag` gauge feeds Hera.

### Consumer map (the whole picture)

- **Demeter** ← `product.price.observed` (Hermes, required) · GetPriceHistory/GetProduct (gRPC, on demand).
- **Dionysus** → mostly **pulls gRPC** (GetProduct, ResolveProduct, GetCurrentPrice); MAY subscribe `product.registered`.
- **Plutus (future)** ← `purchase.recorded` + ListPurchases.
- **ariadne-ui** → REST only.

---

## 6. Identity resolution — HOT SPOT #1, the heart of the service

The thread through the labyrinth. Two entry paths, one engine:

- **Path A — scraped listing → Product** (continuous, automated): every scraper listing must
  become, or link to, exactly one Product before its price can be observed.
- **Path B — Dionysus ingredient → Product** (interactive): `ResolveProduct` over gRPC; Dionysus
  stores the returned id on its Ingredient. Same engine, sync caller.

### 6.1 The strong key: GTIN

Barcode/GTIN (validated GTIN-8/12/13/14, check-digit, normalized to 14) is the **only key trusted
for automatic identity**. GTIN present on both sides + equal ⇒ match, confidence 1.0, no review.
A product can hold multiple GTINs (pack variants) — `AddIdentifier`. The `product_gtins` index
enforces one-product-per-GTIN; a violation discovered late is repaired by merge, not prevented by
a distributed lock (facts-first, repair-explicitly).

### 6.2 The second key: listing identity

`ListingKey(storeId, externalId)` — the retailer's own stable id for a listing. Once a listing is
resolved (auto or human), the link is remembered (`ListingLinked`); **every subsequent scrape of
that listing short-circuits the matcher entirely.** This is what makes the pipeline cheap in
steady state: fuzzy matching runs once per new listing, not once per observation.

### 6.3 Fuzzy fallback — normalize, then score

Pure functions in `core` (property-tested; this is the code that earns its keep):

1. **Normalize** both sides: lowercase; strip punctuation/accents; extract and remove the
   **size/quantity** (`750ml`, `2 x 1L`, `454 g`) into a structured `Quantity`; extract the
   **brand** against the known-brand list (from the catalog itself); expand domain abbreviations
   (`pc`, `wh wht`); token-sort the remainder.
2. **Score** — weighted combination in `[0,1]`:
   - name similarity: trigram similarity + token-set overlap on normalized names (pg_trgm gets
     the candidate set; core recomputes exactly on the shortlist);
   - brand: exact-normalized match strong positive; *conflicting* known brands strong negative;
   - size compatibility: same `Quantity` class within tolerance positive; incompatible
     (750 mL vs 4 L) strong negative — sizes distinguish products, not just describe them;
   - store priors: this store's other listings' resolution history (weak signal, v1.1).
3. Candidate retrieval is the `match_index` projection (§3): GTIN exact → listing exact →
   trigram top-K (K≈10) → core scorer on the shortlist.

### 6.4 Thresholds & outcomes

| Score | Path A (scraped listing) | Path B (ingredient resolve) |
|---|---|---|
| GTIN/listing match | auto-link, confidence 1.0 | `Matched(id, 1.0)` |
| ≥ 0.92 | auto-link (`ListingLinked` with method+confidence recorded — auditable, reversible by split) | `Matched(id, score)` — caller may still ask the user |
| 0.60 – 0.92 | **ResolutionCase → human review**; observation is **parked** (buffered against the case, appended on confirm — no facts recorded against a guessed identity) | `Ambiguous(candidates)` — Dionysus shows a picker; the pick can flow back as `Confirm` |
| < 0.60 | **auto-create Provisional Product** (`Origin.Scrape`), link, observe — prices flow immediately; the provisional surfaces in a low-priority review lane for naming/merging | `NoMatch` — Dionysus may then call `RegisterProduct` deliberately |

Thresholds are config, not code; tune against the review queue's accept-rate.

### 6.5 Human review — ariadne-ui's reason to exist

The review queue (ResolutionCase aggregate → review-queue projection → REST) offers four verbs:

- **Confirm** — link subject to the chosen candidate (releases parked observations).
- **Reject** — none of these; creates a new Product from the subject.
- **Merge** — two Products are the same thing: loser gets `ProductMerged(into)` tombstone; winner
  absorbs GTINs/aliases/listings (`ProductAbsorbed`); loser's id **keeps resolving** via redirect
  forever (Dionysus/Demeter hold ids — ids never die, they forward). Merge notice goes out on
  Hermes (§5).
- **Split** — a listing was linked to the wrong product: unlink it, spawn a new
  Provisional Product with it, and **retract the price observations that arrived through that
  link** (`PriceObservationRetracted`) so bad facts don't poison history.

Every auto-link records method + confidence, so "why is this listing on this product" is always
answerable — and reversible. That audit trail is what makes aggressive auto-linking safe.

### 6.6 What resolution is NOT

Not enrichment (no nutrition lookup — Dionysus), not categorization for deals (Demeter), not a
general entity-resolution platform. One engine, two callers, human backstop.

---

## 7. Purchase ingestion — HOT SPOT #2

Three options, phased; **v1 recommendation: manual entry**, built so the later options are new
*adapters*, not new models (all paths converge on the same `RecordPurchase` command).

| Option | Effort | Data quality | Verdict |
|---|---|---|---|
| **Manual entry** (ariadne-ui: pick store → add lines with product typeahead via SearchProducts, price prefilled from `current_price`, correct to actual) | small | perfect line-level (product id + actual price) | **v1 — build now.** Prefill makes it a 30-second chore; every line feeds a `Purchase`-source price fact |
| **Receipt scan** (photo → blob in **Apollo** → OCR worker (Argus-pattern) → parsed lines → each line through the **resolver** (§6) → prefilled draft purchase, human confirms) | medium | high after confirm | **v2.** Note the receipt-text→product problem *is* identity resolution — the engine is already built; a receipt line is just another `MatchSubject`. Draft-confirm flow reuses the review model |
| **Bank import** (CSV/API) | medium-high | **totals only — no line items**, so no product-level price facts | **v3 / maybe-never for Ariadne.** Amounts-per-store-per-day is a *budgeting* fact, not a catalog fact — it likely belongs to Plutus later, cross-checked against `purchase.recorded` |

Standing facts regardless of path (§2.4): every purchase line feeds actual-price-paid into price
history (`PriceSource.Purchase` — the best facts we get), and `purchase.recorded` is the future
budgeting feed.

---

## 8. Cross-cutting

- **Correlation-id (HermesMQ v1.13.0 discipline):** adopt from incoming gRPC metadata / REST
  header / Hermes message when present, mint (ULID) at every edge otherwise; journal it on every
  event; echo it on every response and every published message; MDC-propagated into logs
  (Apollo's `tracing/CorrelationId` + `MdcPropagatingExecutionContext` pattern). A flyer scrape
  is an edge → each scrape run mints one cid, so listing→resolution→observation→Demeter-alert is
  one traceable thread.
- **Health/metrics:** `/health` liveness + readiness (journal, projection lag, Hermes lag);
  `/metrics` Prometheus with Hera scrape annotations. Key gauges/counters: observations appended
  (by source), resolver outcomes (by band — the auto-link/review/new-product mix is THE service
  health signal), review-queue depth + age, publisher lag, scrape-run success.
- **Persistence ops:** Postgres via pg-service chart; pg-dump→S3 backups; journal is the source
  of truth, every read model rebuildable (§3).
- **Secrets:** SOPS+age → k8s Secret (Harpocrates story) for scraper credentials/API keys, Hermes
  auth if/when it lands.
- **ariadne-ui** (later): Next.js, dark-only, the god mark (pending — constellation-logo
  pipeline), per UI-PLAYBOOK; screens = review queue (§6.5), product catalog browse/edit, price
  history charts, purchase entry (§7); surfaces the service `/docs`.

## 9. Interaction split — the explicit table

| Surface | Protocol | Why |
|---|---|---|
| Dionysus: shopping list needs identity + current price NOW; ingredient→product resolve | **gRPC** (GetProduct / ResolveProduct / GetCurrentPrice) | blocked-and-waiting |
| Demeter: deal evaluation on every new price | **Hermes** `product.price.observed` | reaction / fan-out |
| Demeter: history/context on demand | **gRPC** (GetPriceHistory / GetProduct) | blocked-and-waiting |
| New-product / merge awareness (both consumers, optional) | **Hermes** `product.registered` | reaction |
| Future budgeting | **Hermes** `purchase.recorded` (+ gRPC ListPurchases) | reaction (+ pull) |
| ariadne-ui | **REST** (+ self-hosted `/docs`, Insomnia collection) | browser/BFF rule |
| Scraper → Ariadne | in-process (the policy lives inside Ariadne) | facts belong with facts |

## 10. Open questions (tracked, not blocking)

1. Dedicated `product.merged` topic vs `status=merged_into` on `product.registered` (§5) —
   decide with the Lexicon session when the first consumer needs merges.
2. Fuzzy thresholds (0.60/0.92) are guesses — tune against real Flipp data during the Demeter
   backfill (the backfill doubles as the matcher's test corpus).
3. Store granularity: chain vs individual location (prices differ by location for some chains).
   v1: chain-level with optional location; revisit when it hurts.
4. Receipt-OCR worker placement (Argus-style worker vs in-service) — v2 question.

## 11. Build order (suggested)

1. Seed repo per `new-scala-pekko-service` (core/server split), pg-service wiring, health/metrics.
2. `core`: value types + the four fact aggregates' decide/evolve + tests.
3. `core`: normalizer + scorer (§6.3) + property tests — the hard part, do it early.
4. `server`: persistence entities + product-catalog/price-history/current-price projections.
5. gRPC surface (stub the Lexicon contract locally while the proposal is reviewed).
6. Hermes publisher projection + topic self-provisioning (client pinned @v1.13.0).
7. ResolutionCase + review queue + REST + `/docs` + Insomnia.
8. Scraper adapter (Flipp first — port, don't rewrite, Demeter's source; see
   `migration-demeter.md`).
9. Purchase v1 (manual) + the price-append process manager.
10. Migrations per `migration-demeter.md` / `migration-dionysus.md`.

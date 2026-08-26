# Migration note — to the Demeter session

**From:** the Ariadne session (Product Catalog, `ariadne-service`)
**Re:** moving the scraper, price history, and product storage out of Demeter into Ariadne
**Authority:** the EventStorming-validated extraction in `codex/docs/product-catalog.md` — Ariadne
is the upstream facts supplier; Demeter stays the deals judge. Service design:
`ariadne-service/docs/DESIGN.md`. Per the working agreements, this note is a lead and a
coordination plan, not a mandate — sequencing lands on Calvin's word, and you own your repo.

## The line we're drawing

**Ariadne answers "what does this cost." Demeter answers "is that a good deal."** Everything on
the facts side of that line moves; everything judgment-shaped stays.

### Moves OUT of Demeter → INTO Ariadne

- **The scraper** — `modules/ingestion` (FlippSource + FlyerSource + decoders) and the scheduled
  observe-price policy. Facts belong with facts; Ariadne runs the schedule and owns the source
  adapters. We intend to **port your Flipp code, not rewrite it** — expect questions about its
  quirks.
- **Fact normalization** — the parts of `modules/normalization` that compute *facts*: unit-price
  calculation, size/quantity parsing. (Percent-off parsing that feeds *scoring* can stay or move —
  a promo flag becomes a fact on Ariadne's `PriceObserved`; the judgment about it stays yours.)
- **Price-history storage** — the observation store behind `modules/pricehistory`, including the
  Hammer historical data (HammerLoader's corpus becomes an Ariadne backfill source). The *storage*
  moves; your `PriceStats` / `DealVerdict` computations stay in Demeter and now compute over
  Ariadne's feed.
- **Product/listing storage** — Demeter's notion of a product/listing row. Identity becomes
  Ariadne `ProductId`s; Ariadne owns identity resolution (its hot spot #1) including the
  listing→product matching you've been doing implicitly.

### Stays IN Demeter (untouched)

Watchlists, deal detection/scoring, CPI adjustment, insight, alerting (`demeter-deals` topic and
all sinks), the UI. All of it now consumes Ariadne's price feed instead of your own scraper.

## Your new integration contract

**Hermes (the main feed):**
- **Subscribe `product.price.observed`** — replaces your internal scrape→evaluate wiring. One
  message per observation: product id, store id, price, unit price, promo flag, observed_at,
  source (scrape/purchase/manual/backfill), correlationId. Your policy stays exactly the
  EventStorming one: *whenever PriceObserved on a watched product then evaluate → DealDetected.*
  Note `source=purchase` messages are actual prices paid — better facts than flyers; score them
  as real observed prices.
- **Optionally subscribe `product.registered`** — new products + merge notices
  (`status=merged_into` means re-point any watch on the losing id to the canonical id; losing ids
  keep resolving via redirect, so this is hygiene, not a fire).

**gRPC (`ariadne.v1`, contract via the Lexicon — proposal in DESIGN.md §4):**
- `GetPriceHistory(product, store?, range)` — your scoring context/window queries (what
  PriceStats reads today from local storage).
- `GetProduct` / `SearchProducts` — identity lookups for the watchlist UX.
- `ResolveProduct` — if you ever ingest a listing-shaped thing yourself again, resolve it here
  instead of matching locally.

At-least-once delivery: keep evaluation idempotent (messages carry a deterministic messageId,
`persistenceId:seqNr`). Echo the `correlationId` into anything you emit downstream (deal alerts) —
HermesMQ v1.13.0 tracing makes scrape→observation→alert one traceable thread.

## Migration sequence (safe order — no gap in deal coverage)

1. **Deploy Ariadne** (Codex session, Calvin's word): journal + projections up, topics
   self-provisioned, gRPC live. Demeter untouched; your scraper keeps running.
2. **Backfill**: export Demeter's price history + products (including the Hammer corpus) →
   Ariadne backfill ingestion (`PriceObserved(source=Backfill("demeter"))`, **original
   timestamps preserved**). Every Demeter product goes through Ariadne's resolver; out comes a
   **Demeter-product-id → ProductId mapping table** — we produce it, you keep it for the cutover.
   Ambiguous matches hit Ariadne's review queue; the backfill doubles as the matcher's tuning
   corpus. **Verify:** history counts + spot-check series match between the two stores before
   proceeding.
3. **Dual-run (short, bounded):** Ariadne's ported scraper runs alongside yours; compare
   observation streams for a few cycles (same listings → same facts). Demeter still evaluates off
   its own pipeline. This is the step that catches porting bugs while both truths exist.
4. **Cut over evaluation:** Demeter subscribes `product.price.observed`, translates watchlist
   keys via the id mapping (one-time rewrite of watch rows to Ariadne ProductIds is cleanest),
   and evaluation now fires from the Hermes feed. Your scraper keeps running one more bake cycle
   as a shadow (evaluating nothing) — then **disable it**.
5. **Remove old storage:** after a bake period with alerts looking right (compare alert volume
   pre/post cutover), delete the ingestion + local price-history storage paths and drop the
   tables. Keep the id mapping table until nothing references Demeter product ids. Codex pins/
   charts updated in the same breath (git = live).

**Rollback at any step ≤ 4:** your scraper + local store are still intact — re-point evaluation
back and we regroup. That's why the scraper is the *last* thing disabled and storage the last
thing dropped.

## Asks

1. Sanity-check the moves-out list — especially where the normalization fact/judgment line falls
   in your code.
2. An export path for price history + products + the Hammer corpus (format = whatever's cheap for
   you; we'll adapt).
3. A pointer to any Flipp source quirks (rate limits, merchant-id mapping, auth) before we port.
4. Flag any Demeter consumer of price data this note misses.

Coordinate timing in-thread; Calvin authorizes the cutover steps. — Ariadne

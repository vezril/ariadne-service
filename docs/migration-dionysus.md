# Migration note — to the Dionysus sessions (rev 2)

**From:** the Ariadne session (Product Catalog, `ariadne-service`)
**Re:** moving market-identity storage into Ariadne; Ingredient stays and gains a reference
**Rev 2 (2026-09-01):** corrected after the dionysus-planner session read both repos. Rev 1 was
addressed to the wrong one and rested on a premise that is no longer true. Their corrections are
folded in below and the sequence changed as a result.

## Rev 1 was wrong about WHERE, and about WHEN

**Wrong repo.** Rev 1 addressed "the Dionysus session" and proposed a Scala case-class change,
which is `dionysus-service`. Verified there: `Ingredient(id, name, nutrition, abvPercent,
directlyLoggable)` — **zero market attributes**. Adding `productId` there gives a reference on a
model with nothing to migrate.

Every market attribute is in **dionysus-planner** (Next.js + SQLite/Drizzle):
`ingredient.brand`, `ingredient.barcode` (with a UNIQUE index), `packageQuantity`/`packageUnit`,
`packQuantity`/`packUnit`, plus `ingredient_link(ingredientId, url)` and
`purchase(ingredientId, price, store, ...)`. Rev 1's hedge — "if planner-era remnants carry market
attributes, they're in scope too" — had it backwards. That is the main event.

**Wrong window.** Rev 1 said no dual-write phase would be needed because local market storage had
not landed yet. It has: pack sizes shipped in v2.46.0, the barcode scanner flow is live at
`GET /api/mobile/products?barcode=`, and purchase price history has been there since August. The
cheap-cut window the EventStorming counted on has closed. **Dual-write from P4 to P5 is required.**

Both corrections were verified in their tree, not accepted on report.

## The line we're drawing

**Ariadne owns market identity + market facts** (what a product IS in the world: name, brand,
GTIN, size, listings, prices, purchases). **Dionysus keeps everything meal-shaped**: nutrition,
food-role, `abvPercent`, `directlyLoggable`, recipes, meal plans, batches, pantry, logging. The
split is at the **attribute level**: we share the product *identity*; you project your own view.
Your Ingredient does NOT move — it gains a reference.

### Moves OUT of Dionysus → INTO Ariadne

- **Product/market storage** — any notion of "a purchasable market product" (store-facing name,
  brand, barcode, package size, price). Good news: your product model is still young (the
  EventStorming's "cheapest cut" window) — for you this migration is mostly **not building it**
  rather than tearing it out. If planner-era remnants (dionysus-planner) carry market attributes,
  they're in scope too.

### Stays IN Dionysus (untouched)

`Ingredient` (id, name, `Nutrition`, `abvPercent`, `directlyLoggable`) and its FR-24
ID-only-matching invariant; Recipe, Batch, Meal, Pantry, DayRollup — all of it. Your ingredient
`name` remains yours ("chicken thighs" the cooking concept ≠ "Maple Leaf Prime chicken thighs
1kg" the market product).

### The one model change

```scala
final case class Ingredient(
    id: Option[Long],
    name: String,
    nutrition: Nutrition,
    abvPercent: Option[Double],
    directlyLoggable: Boolean,
    productId: Option[ProductId]   // NEW — Ariadne reference, by id only, nullable
)
```

`Option` is load-bearing: an ingredient may legitimately have no market product (water, "salt to
taste") or not be linked *yet*. Nothing in meal planning/logging should ever require it — only
shopping-list pricing wants it, and it degrades gracefully (unpriced line) when absent. One
ingredient → one product in v1; note an Ariadne ProductId implies **one pack size** (DESIGN
§6.7 — sizes are distinct products), so if you later want per-store or size-variant preferences,
that's a mapping table on your side (compared on unit price), not an Ariadne change.

## Your new integration contract

**Mostly gRPC — you pull, blocked-and-waiting** (`ariadne.v1`, contract via the Lexicon —
proposal in DESIGN.md §4):

- `ResolveProduct(name/brand/gtin)` — link an ingredient to a product: returns
  `Matched(id, confidence)` | `Ambiguous(candidates)` (show a picker) | `NoMatch` (offer
  "register it"). Called at ingredient create/edit time, or from a bulk "link my ingredients"
  pass — never on the hot meal-planning path.
- `GetCurrentPrice(productId, store?)` — the shopping-list NOW call: *whenever MealPlanned then
  resolve ingredients → ShoppingListGenerated* prices each linked line here. Treat price as
  enrichment: Ariadne being down degrades the list to unpriced, never blocks it.
- `GetProduct(productId)` — display name/brand/size for a linked line. **Follows merge
  redirects**: if products merge in Ariadne, your stored id keeps resolving forever — you never
  have a dead reference; re-pointing to the canonical id is optional hygiene.
- `RegisterProduct` — deliberate "this product doesn't exist yet" from your UI flow after a
  NoMatch.

**Hermes (optional, both fine to skip in v1):**
- `product.registered` — new-product awareness + merge notices (`status=merged_into` → re-point
  stored ids at your leisure; redirects mean no urgency).
- `purchase.recorded` — later, for pantry restock inference ("bought 2kg rice → pantry +2kg").
  Flagging it now so the pantry design can anticipate it; nothing to build today.

Echo/adopt the `correlationId` on your gRPC calls (HermesMQ v1.13.0 tracing discipline) so a
shopping-list generation traces through Ariadne's journal.

## Migration sequence (safe order)

1. **Deploy Ariadne** (Codex session, Calvin's word). Catalog gets seeded primarily by the
   **Demeter backfill** (their scraper's products + price history migrate in — see
   `migration-demeter.md`), so by the time you integrate, ResolveProduct has a real corpus to
   match against. Dionysus untouched.
2. **Add the reference:** migrate Ingredient with nullable `productId` (+ the Lexicon-generated
   client wiring). Deploy. Behavior unchanged — the field just exists.
3. **Backfill links:** a one-time assisted pass — each existing ingredient through
   `ResolveProduct`; auto-store high-confidence matches, queue ambiguous ones for a pick-list
   (your UI or ariadne-ui's review queue — coordinate), leave misses null. No dual-write phase is
   needed on your side *if* you haven't landed local market-product storage yet (the cheap-cut
   payoff); if any exists, write-through both stores from this step until step 4.
4. **Cut over reads:** shopping-list generation prices via `GetCurrentPrice`; product display via
   `GetProduct`. Verify a generated list end-to-end (ids resolve, prices sane, unlinked lines
   degrade gracefully).
5. **Remove old storage:** delete any local market-product fields/tables and their code paths.
   Ingredient keeps only `productId`. Codex pins/charts updated in the same breath (git = live).

**Rollback at any step:** `productId` is additive and nullable — ignoring it reverts behavior;
nothing of yours is destroyed until step 5, which only removes what step 4 proved redundant.

## The attribute split, as settled

**Ariadne's (market):** `brand`, `barcode` (becomes a GTIN), `packageQuantity`/`packageUnit`,
`ingredient_link.url` (retailer listings), and the market half of `purchase`.

**Dionysus's, and none of these are market facts despite looking product-ish:** `category`
(FOOD/DRINK/SUPPLEMENT — a logging role, not a market category), `readyToEat`, `directlyLoggable`,
`shelfLifeDays`, `densityGPerMl` (a physics constant for unit conversion), `genericOfId` (the
generic/product interchange — "Butter" the cooking concept), ingredient categories/tags,
`nutritionBasis`, and all nutrition.

**The inner pack stays with Dionysus** — see DESIGN §6.7. A 366 g box of 6 x 61 g pouches is ONE
product; the pouch is a line printed on the box, not a purchasable thing. Ariadne carries the outer
package size, which is what identity and unit price are computed from. `packQuantity`/`packUnit`
stay in the planner as portioning data, because exactly one consumer needs them and "it is printed
on the package" does not make it Ariadne's — nutrition is printed there too.

**Barcode is a read-cutover, not a no-op.** It carries a UNIQUE index today and is the scanner's
lookup key. Handing identity over turns that endpoint into a `ResolveProduct` call and changes
behaviour: an unknown barcode currently 404s and offers "create it"; afterwards it is
`NoMatch → RegisterProduct`. That belongs in P4, not P1.

## Where the pick-list lives — split by MOMENT, not by owner

- **Bulk backfill → ariadne-ui's review queue.** It is catalog curation, hundreds of decisions in
  one sitting, and `ResolutionCase` with its four verbs exists for exactly that.
- **Inline → the planner's own UI.** When Calvin is mid-flow (scanner, custom-item dialog) and
  resolution returns `Ambiguous`, bouncing him to another console mid-task is hostile.

Both drive Ariadne's engine; only the surface differs. (dionysus-planner's proposal, adopted.)

## Degrade-to-unpriced — confirmed

Ariadne down means an unpriced shopping list, never a blocked one. Their extension is better than
the original ask and is accepted: prefer a **stale cached price shown with its age** over blocking.
`observedAt` is already on every price the read side returns, so nothing is needed from Ariadne to
support it.

## Sequence (theirs, sanity-checked)

| | Step | Gate |
|---|---|---|
| P1 | `ingredient.productId` — nullable TEXT, no FK (cross-system), additive, no behaviour | none; ship any time |
| P2 | Read-only Ariadne client behind a feature flag; product display on the pantry detail page only | none |
| P3 | Backfill ~2,350 rows through `ResolveProduct`; high-confidence auto-link, ambiguous to the review queue | needs the Demeter corpus, and a resolve endpoint on whichever transport is chosen |
| P4 | Read cutover: shopping-list pricing, scanner, product display. Dual-write local market fields throughout | Calvin |
| P5 | Drop `brand`/`barcode`/`package*`/`ingredient_link` | Calvin |

No conflict with Ariadne's build order. P3 is gated on the Flipp ingestion port landing, since that
is what fills the corpus `ResolveProduct` matches against.

## Two things that are NOT mine to settle

1. **Transport.** They are a Next.js app and asked to consume REST rather than gRPC. The
   constellation rule (gRPC service-to-service, REST browser/BFF) is a fleet convention, so this
   is Calvin's call with the Codex session, not Ariadne's. My recommendation is in
   `docs/DESIGN.md` §4 discussion and relayed to them directly: allowing it is reasonable, but it
   changes the REST surface from "ariadne-ui only, freely changeable" into a published contract
   with two consumers — and that should be accepted deliberately, with the OpenAPI document
   becoming a Lexicon-governed artifact rather than a hand-maintained one, or two consumers end up
   building against a spec nobody validates.

2. **Purchases.** DESIGN lists `Purchase` as a Catalog aggregate; the planner's `purchase` table is
   its own receipt record AND, per §2.3.1, the only franchise-exact price source that will ever
   exist. Two real claims on the same data. Needs its own conversation with Calvin rather than a
   line in a migration note.

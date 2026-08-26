# Migration note — to the Dionysus session

**From:** the Ariadne session (Product Catalog, `ariadne-service`)
**Re:** moving product (market-identity) storage out of Dionysus into Ariadne; Ingredient stays
yours and references Ariadne by id
**Authority:** the EventStorming-validated extraction in `codex/docs/product-catalog.md` —
products were extracted *from Dionysus* because product/price facts are upstream of both you and
Demeter. Service design: `ariadne-service/docs/DESIGN.md`. Per the working agreements this is a
lead + coordination plan; sequencing lands on Calvin's word, and you own your repo.

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

## Asks

1. Sanity-check the attribute split — anything market-flavored hiding in Ingredient/planner
   models that this note misses (or anything I've claimed that's actually meal-flavored)?
2. Where should the ambiguous-match pick-list live in v1 — your UI mid-flow, or ariadne-ui's
   review queue? (Engine is Ariadne's either way; UX is your call.)
3. Confirm the shopping-list degrade-to-unpriced behavior matches your intent.
4. Timing: after the Demeter backfill lands, so resolution has a catalog to match against.

Coordinate timing in-thread; Calvin authorizes the cutover steps. — Ariadne

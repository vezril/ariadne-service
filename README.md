# ariadne-service

**Ariadne — the constellation's Product Catalog.** The shared, facts-only source of truth for
*what things are and what they cost over time*: product identity, stores, price history, and
purchases. **Demeter** (deals) and **Dionysus** (meals) consume it; neither owns product/price data.

The name is the design: Ariadne gave Theseus the **thread** through the Labyrinth. This domain's
hardest job is the same — a thread through the maze of retailer listings that resolves each one to
the *right known product*. Identity resolution is the soul, not the CRUD.

> **Canonical design (EventStorming-validated) lives in codex: `~/Code/codex/docs/product-catalog.md`.**
> Read it first — this README is the seed; that doc is the source of truth.

## The two rules that shape everything

1. **Facts-only — never a God-object.** Ariadne owns identity + **market facts** (Product, Store,
   PriceObservation, Purchase). It does **NOT** own nutrition (that stays in Dionysus, referencing
   a Product by id) or deal-logic/CPI (that stays in Demeter). The split is at the *attribute*
   level: everyone shares the product **identity**; each consumer projects its own view. If you
   feel tempted to add a consumer-specific field, that's the God-object trap — don't.
2. **Identity resolution is the real work (hot spot #1).** Matching a scraped listing to a known
   product, and a Dionysus ingredient to a Catalog entry — barcode/GTIN as the strong key, fuzzy
   matching as fallback. This, not "store rows," is where Ariadne earns its keep.

## Domain shape (from the EventStorming wall)

- **Aggregates:** Product · Store · PriceObservation · Purchase.
- **Scraping moves IN** (from Demeter): the scheduled `ObservePrice` policy belongs with the price
  facts. Ariadne answers "what does this cost"; Demeter answers "is that a good deal."
- **Event-sourced fit:** `PriceObservation` *is* an append-only event stream; the price-history
  read model is a projection over it. Aggregates as `decide/evolve`; purchases + observations are
  immutable facts.
- **Hermes-first:** publishes `product.registered`, `product.price.observed`, `purchase.recorded`
  (self-provisioned topics — the playbook convention). Consumers subscribe; synchronous reads (a
  shopping list needing current price now) go REST/gRPC. Blocked-waiting → REST; reaction → Hermes.
- **Hot spot #2:** purchase ingestion (manual / receipt / bank) — open; also feeds future budgeting.

## Constellation conventions

Greek name = software (per convention; Ariadne is a legendary figure, Calvin's expand-beyond-
divinities pick). Contract in the Lexicon; self-hosted `/docs`; health+metrics; Postgres via
pg-service; chart + `apps/ariadne` pin in codex; deploys are the Codex session's. Owned/coordinated
per `codex/docs/session-coordination.md`. Mark: none yet (generate via the constellation-logo
pipeline). Migration to coordinate with the **Demeter** (scraper + price-history move out) and
**Dionysus** (products move out) sessions.

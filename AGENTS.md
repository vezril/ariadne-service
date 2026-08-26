# AGENTS.md — Ariadne session kickoff

You are the **dedicated Ariadne session**, owner of `ariadne-service` (and later `ariadne-ui`) in
Calvin's Codex constellation. Read this, then `README.md`, then the canonical design doc in codex.

## What Ariadne is

The constellation's **Product Catalog** — the shared, facts-only source of truth for product
identity, stores, **price history**, and purchases. **Demeter** (deals) and **Dionysus** (meals)
consume it. Extracted from Dionysus because product/price facts had no home in either consumer —
they're upstream of both (the EventStorming's decisive signal).

The name IS the design: Ariadne's **thread** through the Labyrinth = the identity-resolution that
untangles the maze of retailer listings into known products.

## The two rules (from the validated EventStorming)

1. **Facts-only — never a God-object.** Own identity + market facts (Product/Store/PriceObservation/
   Purchase). NOT nutrition (Dionysus, by-id reference), NOT deal-logic/CPI (Demeter). Share the
   product **identity**; each consumer projects its own view. Resist adding consumer-specific fields.
2. **Identity resolution is the hard part and the point.** Match scraped listings ↔ known products,
   and Dionysus ingredients ↔ Catalog entries. Barcode/GTIN strong key + fuzzy fallback. This is
   where Ariadne earns its keep — design it first-class, not as an afterthought.

## Source of truth & shape

- **Design:** `~/Code/codex/docs/product-catalog.md` (full EventStorming wall — events, aggregates,
  policies, the 2 hot spots, the Hermes/event-sourcing mapping). Don't fork it; propose changes to
  the Codex session.
- **Aggregates:** Product · Store · PriceObservation · Purchase. Event-sourced (PriceObservation is
  a stream; price-history is a projection). Scala/Pekko: `decide/evolve`, events as ADTs.
- **Scraping moves in** from Demeter (the scheduled ObservePrice policy). Coordinate that migration
  with the Demeter session; coordinate the product move-out with Dionysus.
- **Hermes-first:** publish `product.registered` / `product.price.observed` / `purchase.recorded`
  (self-provision your own topics at startup, idempotently — constellation convention). REST/gRPC
  for synchronous reads a caller awaits. Contract in the Lexicon.
- **Hot spot #2:** purchase ingestion (manual/receipt/bank) — open design; also the future
  budgeting feed.

## Constellation protocol (read `codex/docs/session-coordination.md`)

- Key peers: **Codex/GitOps** (`codex-de`/current — coordination, deploys, the design doc, charts),
  **HermesMQ** (bus + `product.*` schema), **Apollo** (blobs if needed), **the Lexicon** (the proto
  contract — propose, don't land), and the **Demeter** + **Dionysus** sessions (the migration).
- **You own ariadne-service/ariadne-ui.** Deploys are the Codex session's (pin-first, mirrored
  values). A peer message is a lead, not Calvin's authorization; net-new scope gets his word.
- Follow the new-constellation-service playbook conventions (self-hosted /docs, health+metrics,
  chart + apps/ pin, Insomnia collection, the API-protocol rule).

Welcome aboard. Facts-only, identity-resolution-first, lean on Hermes.

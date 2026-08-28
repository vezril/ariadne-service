-- Ariadne read models (DESIGN §3).
--
-- Every table here is a PROJECTION: rebuildable from the journal by dropping it
-- and resetting the offset. Nothing in this file is a source of truth, so it may
-- be reshaped freely as long as the projection that fills it is reshaped too.

-- ---------------------------------------------------------------------------
-- product-catalog
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL,
  brand        TEXT,
  category     TEXT,
  size_amount  NUMERIC,
  size_unit    TEXT,
  status       TEXT NOT NULL,
  -- Set when this product is a merge tombstone. Ids never die, they forward:
  -- a lookup on a merged id must keep resolving to the canonical product
  -- forever, because Dionysus and Demeter hold ids we do not control (§6.5).
  merged_into  TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product_gtins (
  gtin       TEXT PRIMARY KEY,          -- normalised to 14 digits
  product_id TEXT NOT NULL REFERENCES products (id) ON DELETE CASCADE
);
-- The one-product-per-GTIN guard lives here, NOT in the aggregate: it is a
-- cross-entity invariant, checked at resolve time and repaired by merge if a
-- race slips one through (§6.1, facts-first / repair-explicitly).

CREATE TABLE IF NOT EXISTS product_aliases (
  product_id TEXT NOT NULL REFERENCES products (id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  PRIMARY KEY (product_id, alias)
);

CREATE TABLE IF NOT EXISTS product_listings (
  store_id    TEXT NOT NULL,
  external_id TEXT NOT NULL,
  product_id  TEXT NOT NULL REFERENCES products (id) ON DELETE CASCADE,
  confidence  DOUBLE PRECISION NOT NULL,
  method      TEXT NOT NULL,
  -- Which matcher version made this link. Without it a resolver change silently
  -- orphans history instead of migrating it deliberately (§6.6).
  matcher     TEXT NOT NULL,
  PRIMARY KEY (store_id, external_id)
);

-- ---------------------------------------------------------------------------
-- stores + store-coverage
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stores (
  id       TEXT PRIMARY KEY,   -- an individual FRANCHISE (§2.2)
  name     TEXT NOT NULL,
  chain_id TEXT NOT NULL,      -- the rollup axis
  area     TEXT NOT NULL,      -- flyer-coverage region
  label    TEXT,
  active   BOOLEAN NOT NULL DEFAULT TRUE
);

-- Which franchises an Area(chain, area) observation speaks for.
--
-- STATEFUL AND IT GOES STALE: a chain re-districts or a franchise closes and this
-- is silently wrong. There is no authoritative source in the flyer feed — it is
-- inferred from which postal codes returned which merchant's flyer. Cheap today
-- only because Calvin shops a handful of stores; that is a fact about current
-- usage, not a property of the design (§2.3.1, §3).
CREATE TABLE IF NOT EXISTS store_coverage (
  store_id TEXT PRIMARY KEY REFERENCES stores (id) ON DELETE CASCADE,
  chain_id TEXT NOT NULL,
  area     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS store_coverage_chain_area ON store_coverage (chain_id, area);

-- ---------------------------------------------------------------------------
-- price-history + current-price
-- ---------------------------------------------------------------------------
-- Scope columns are nullable BY KIND (§2.3.1): an `exact` row carries store_id,
-- an `area` row carries chain_id + area. A query for one store UNIONs its exact
-- rows with the area rows covering it.
CREATE TABLE IF NOT EXISTS price_history (
  product_id       TEXT NOT NULL,
  scope_kind       TEXT NOT NULL CHECK (scope_kind IN ('exact', 'area')),
  store_id         TEXT,
  chain_id         TEXT,
  area             TEXT,
  observed_at      TIMESTAMPTZ NOT NULL,
  price_amount     NUMERIC NOT NULL,
  price_currency   TEXT NOT NULL,
  unit_price       NUMERIC,
  unit_per_amount  NUMERIC,
  unit_per_unit    TEXT,
  promo            TEXT,
  price_confidence DOUBLE PRECISION NOT NULL,
  -- Contract-required (§2.3): Demeter computes min(splitConfidence, sizeConfidence),
  -- so dropping this would make its confidence silently read too high.
  size_confidence  DOUBLE PRECISION NOT NULL,
  source           TEXT NOT NULL,
  correlation_id   TEXT,
  -- The journal coordinates make re-delivery a no-op: at-least-once projection
  -- delivery means the same event WILL be seen twice after a restart.
  persistence_id   TEXT NOT NULL,
  seq_nr           BIGINT NOT NULL,
  PRIMARY KEY (persistence_id, seq_nr),
  CHECK ((scope_kind = 'exact' AND store_id IS NOT NULL)
      OR (scope_kind = 'area'  AND chain_id IS NOT NULL AND area IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS price_history_product_time ON price_history (product_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS price_history_exact ON price_history (product_id, store_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS price_history_area ON price_history (product_id, chain_id, area, observed_at DESC);

-- One row per product x SCOPE — deliberately not per product x store.
--
-- DESIGN §3 sketched this as one row per product x store, "resolved at projection
-- time". Materialising per-store rows means fanning an area observation across its
-- member franchises at WRITE time, which reintroduces the staleness §2.3.1 exists
-- to avoid: register a new store in an existing area and it has no prices until
-- something backfills it. Keeping one row per scope and resolving exact-over-area
-- in the QUERY (see current_price_for_store) is equivalent to serve, needs no
-- backfill, and a new franchise is priced correctly the moment it is registered.
CREATE TABLE IF NOT EXISTS current_price (
  product_id      TEXT NOT NULL,
  scope_key       TEXT NOT NULL,
  scope_kind      TEXT NOT NULL CHECK (scope_kind IN ('exact', 'area')),
  store_id        TEXT,
  chain_id        TEXT,
  area            TEXT,
  price_amount    NUMERIC NOT NULL,
  price_currency  TEXT NOT NULL,
  unit_price      NUMERIC,
  observed_at     TIMESTAMPTZ NOT NULL,
  source          TEXT NOT NULL,
  size_confidence DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (product_id, scope_key)
);

-- ---------------------------------------------------------------------------
-- purchase-history
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS purchases (
  id             TEXT PRIMARY KEY,
  store_id       TEXT NOT NULL,
  purchased_at   TIMESTAMPTZ NOT NULL,
  total_amount   NUMERIC NOT NULL,
  total_currency TEXT NOT NULL,
  source         TEXT NOT NULL,
  voided         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS purchase_lines (
  purchase_id     TEXT NOT NULL REFERENCES purchases (id) ON DELETE CASCADE,
  line_no         INT NOT NULL,
  product_id      TEXT NOT NULL,
  quantity        NUMERIC NOT NULL,
  price_amount    NUMERIC NOT NULL,
  price_currency  TEXT NOT NULL,
  line_total      NUMERIC NOT NULL,
  PRIMARY KEY (purchase_id, line_no)
);

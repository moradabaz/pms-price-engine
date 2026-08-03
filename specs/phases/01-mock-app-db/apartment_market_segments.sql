-- Dimension/reference table resolving the gap Phase 3 surfaced and left open
-- (specs/phases/03-market-ingestion/spec.md §8): payment_lines has no city,
-- neighborhood, or property-profile columns, so nothing maps an apartment to
-- the market segment (services/market-ingestor's SEGMENTS, 18 fixed
-- combinations) it should be priced against. Resolved as Decision C.1
-- (docs/phase-4-streaming-design-decisions.md): a seed script populates this
-- table once from mock-pm-app's apartment pool; Flink (Phase 4) consumes it
-- via the Broadcast State Pattern (Decision C), not a per-row query.
--
-- One row per apartment — a dimension table, not an event log like
-- payment_lines. apartment_id is its own primary key (no separate event_id):
-- CDC replica identity only needs the primary key's before-image here, the
-- same reasoning payment_lines.sql already documents for its own PK.
--
-- Reuses the SAME Debezium publication as payment_lines (dbz_publication) —
-- one connector, two captured tables — matching Decision C.2's "viaja por el
-- mismo pipeline CDC ya existente (Fases 1-2)". Wiring
-- infra/debezium/postgres-connector.json's table.include.list to actually
-- include this table is tracked separately (Fase 4 implementation, not this
-- file) — this publication statement alone does not make Debezium capture it.
--
-- Every statement here is idempotent (IF NOT EXISTS / OR REPLACE / a guarded
-- DO block for the publication) on purpose: this file runs twice in practice
-- — once via docker-entrypoint-initdb.d on a brand-new volume, and again via
-- mock_pm_app.migrations at every startup, which is what actually creates
-- this table on an existing volume from before this table existed (Postgres
-- only runs initdb.d scripts against an empty data directory — an existing
-- volume from an earlier phase would otherwise never get this table at all).
-- The two invocations must stay byte-identical; mock_pm_app/migrations.py
-- embeds this same SQL as a Python string rather than reading this file at
-- runtime, so keep them in sync by hand if this file changes.

CREATE TABLE IF NOT EXISTS public.apartment_market_segments (
    apartment_id         TEXT PRIMARY KEY,
    apartment_reference  TEXT NOT NULL,

    -- Market segment identity — must match one of the 18 combinations
    -- services/market-ingestor/src/market_ingestor/segments.py actually prices.
    -- Not FK-constrained against that list (it lives in a different service,
    -- not a table this database can reference) — kept in sync by convention
    -- and, ideally, a contract test (specs/contracts/) rather than a DB FK.
    city                 TEXT NOT NULL CHECK (city IN ('Barcelona', 'Madrid', 'Valencia')),
    neighborhood         TEXT NOT NULL,
    property_type        TEXT NOT NULL CHECK (property_type IN ('studio', 'apartment')),
    bedrooms             SMALLINT NOT NULL CHECK (bedrooms >= 0),

    -- Decision C.2: cost + this margin is a non-negotiable floor the pricing
    -- engine itself never overrides (ADR-0007) — lowering it, or delisting the
    -- apartment, is always the client's decision, never automatic. 0.05 (5%)
    -- is the confirmed default for this first version; per-apartment override
    -- is already supported by this being a real column, not a global constant.
    target_margin            NUMERIC(5,4) NOT NULL DEFAULT 0.05
                                  CHECK (target_margin >= 0),
    -- Fraction below avg_nightly_rate_eur to stay competitive (price_decision.v1's
    -- calculation.competitiveness_discount). Same default rationale as target_margin.
    competitiveness_discount NUMERIC(5,4) NOT NULL DEFAULT 0.05
                                  CHECK (competitiveness_discount >= 0 AND competitiveness_discount <= 1),

    -- ADR-0009 (D2): blended OTA + payment-processing commission, in the
    -- profitability floor's denominator (it scales with the final price, so
    -- it can't be pre-computed as a fixed euro cost the way Cf/Cv/Cr are).
    -- 0.15 (15%) is the confirmed default; per-apartment override is already
    -- supported by this being a real column, same pattern as target_margin.
    commission_pct       NUMERIC(5,4) NOT NULL DEFAULT 0.15
                                  CHECK (commission_pct >= 0 AND commission_pct <= 1),

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ
);

-- ADR-0009: this table predates commission_pct — ADD COLUMN IF NOT EXISTS so a
-- volume from before this column existed still picks it up (CREATE TABLE IF
-- NOT EXISTS above is a no-op once the table already exists, same concern this
-- file's header already documents for re-running against an existing volume).
ALTER TABLE public.apartment_market_segments
    ADD COLUMN IF NOT EXISTS commission_pct NUMERIC(5,4) NOT NULL DEFAULT 0.15;
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_commission_pct_check;
ALTER TABLE public.apartment_market_segments
    ADD CONSTRAINT apartment_market_segments_commission_pct_check
        CHECK (commission_pct >= 0 AND commission_pct <= 1);

CREATE INDEX IF NOT EXISTS idx_apartment_market_segments_segment
    ON public.apartment_market_segments (city, neighborhood, property_type, bedrooms);

-- Same freshness-trigger pattern as payment_lines.sql — updated_at must
-- reflect every UPDATE regardless of what the writing application sets.
CREATE OR REPLACE FUNCTION public.set_apartment_market_segment_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- CREATE OR REPLACE TRIGGER requires Postgres 14+ (we run postgres:16).
CREATE OR REPLACE TRIGGER trg_apartment_market_segments_updated_at
    BEFORE UPDATE ON public.apartment_market_segments
    FOR EACH ROW
    EXECUTE FUNCTION public.set_apartment_market_segment_updated_at();

-- ALTER PUBLICATION ... ADD TABLE has no IF NOT EXISTS form — it errors if
-- the table is already a publication member, so re-running this file (the
-- whole point of this being idempotent) needs an explicit guard.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'apartment_market_segments'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.apartment_market_segments;
    END IF;
END $$;

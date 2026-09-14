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

    -- Phase 25 (docs/adr/ADR-0018, backlog #15): target_margin/
    -- competitiveness_discount (Decision C.2) lived here through Phase 24 —
    -- moved to their own insert-only pricing_strategies table
    -- (pricing_strategies.sql) so a strategy edit can be versioned without
    -- mutating this dimension row. See this file's migration block below.

    -- Phase 8 (docs/adr/ADR-0011, backlog #6): raw Property Bonus/Malus
    -- attributes. Flink resolves these into a single property_attribute_factor
    -- on CostAggregate (flink_jobs/property_attributes.py) — this table stores
    -- the inputs, not the derived factor.
    quality_tier         TEXT NOT NULL DEFAULT 'standard'
                                  CHECK (quality_tier IN ('basic', 'standard', 'premium', 'luxury')),
    rating                NUMERIC(2,1) NOT NULL DEFAULT 4.0
                                  CHECK (rating >= 1.0 AND rating <= 5.0),
    has_view              BOOLEAN NOT NULL DEFAULT false,
    has_parking           BOOLEAN NOT NULL DEFAULT false,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ
);

-- Phase 11 (docs/adr/ADR-0011, backlog #5): commission_pct moves to
-- owner_contracts.commission_pct (owner_contracts.sql) — a real removal, not
-- another additive column, since keeping the same number in two tables would
-- be the kind of dual-source-of-truth drift this project's SOLID/DRY
-- guidance rules out. Drops the constraint first (DROP COLUMN cascades it
-- anyway, but explicit is safer against a constraint left orphaned by a
-- partial prior run).
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_commission_pct_check;
ALTER TABLE public.apartment_market_segments
    DROP COLUMN IF EXISTS commission_pct;

-- Phase 8: ADD COLUMN IF NOT EXISTS so a volume from before this phase still
-- picks up sane defaults (same idempotent-migration convention this file's
-- header already documents).
ALTER TABLE public.apartment_market_segments
    ADD COLUMN IF NOT EXISTS quality_tier TEXT NOT NULL DEFAULT 'standard';
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_quality_tier_check;
ALTER TABLE public.apartment_market_segments
    ADD CONSTRAINT apartment_market_segments_quality_tier_check
        CHECK (quality_tier IN ('basic', 'standard', 'premium', 'luxury'));

ALTER TABLE public.apartment_market_segments
    ADD COLUMN IF NOT EXISTS rating NUMERIC(2,1) NOT NULL DEFAULT 4.0;
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_rating_check;
ALTER TABLE public.apartment_market_segments
    ADD CONSTRAINT apartment_market_segments_rating_check
        CHECK (rating >= 1.0 AND rating <= 5.0);

ALTER TABLE public.apartment_market_segments
    ADD COLUMN IF NOT EXISTS has_view BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE public.apartment_market_segments
    ADD COLUMN IF NOT EXISTS has_parking BOOLEAN NOT NULL DEFAULT false;

-- Phase 25 (ADR-0018 §1): backfills any existing target_margin/
-- competitiveness_discount into a version=1 pricing_strategies row each,
-- then drops both columns — a real substitution, not a redundant copy, same
-- precedent this file's own commission_pct removal above already
-- established. Guarded by information_schema, not a flag column; harmless
-- no-op once already migrated or on a fresh install (the columns never
-- existed, since they are no longer in the CREATE TABLE above).
DO $$
DECLARE
    legacy_columns_exist boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'apartment_market_segments'
          AND column_name = 'target_margin'
    ) INTO legacy_columns_exist;

    IF legacy_columns_exist THEN
        INSERT INTO public.pricing_strategies
            (apartment_id, version, target_margin, competitiveness_discount)
        SELECT ams.apartment_id, 1, ams.target_margin, ams.competitiveness_discount
        FROM public.apartment_market_segments ams
        WHERE NOT EXISTS (
            SELECT 1 FROM public.pricing_strategies ps
            WHERE ps.apartment_id = ams.apartment_id AND ps.version = 1
        );

        ALTER TABLE public.apartment_market_segments
            DROP CONSTRAINT IF EXISTS apartment_market_segments_target_margin_check;
        ALTER TABLE public.apartment_market_segments
            DROP COLUMN IF EXISTS target_margin;
        ALTER TABLE public.apartment_market_segments
            DROP CONSTRAINT IF EXISTS apartment_market_segments_competitiveness_discount_check;
        ALTER TABLE public.apartment_market_segments
            DROP COLUMN IF EXISTS competitiveness_discount;
    END IF;
END $$;

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

-- Phase 25 (docs/adr/ADR-0018, ADR-0011 backlog #15): target_margin/
-- competitiveness_discount, extracted out of apartment_market_segments
-- (apartment_market_segments.sql) into their own insert-only table — the
-- same real-substitution pattern ADR-0012/ADR-0013 already established for
-- payment_lines/owner_contracts.
--
-- Insert-only, never UPDATEd: editing a strategy means inserting a new row
-- with version = previous_max_version + 1 and a fresh effective_from; the
-- previous version's row is never modified. This is the mechanism that
-- makes "future-only, no replay" concrete — Debezium can only ever emit an
-- INSERT CDC event for a strategy change, mirroring the append-only shape
-- manual_overrides.sql already uses for its own audit trail (Phase 14).
--
-- apartment_id is TEXT (not a surrogate UUID) — every other table in this
-- schema references apartment_market_segments.apartment_id, which is TEXT
-- (e.g. "BCN-001"); this table follows the same convention.
--
-- Same "kept in sync by hand" convention as owner_contracts.sql/
-- cost_definitions.sql: mock_pm_app/migrations.py embeds this same SQL as a
-- Python string.

CREATE TABLE IF NOT EXISTS public.pricing_strategies (
    pricing_strategy_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    apartment_id             TEXT NOT NULL
                                  REFERENCES public.apartment_market_segments(apartment_id),
    version                  INT NOT NULL CHECK (version >= 1),

    target_margin            NUMERIC(5,4) NOT NULL
                                  CHECK (target_margin >= 0 AND target_margin < 1),
    competitiveness_discount NUMERIC(5,4) NOT NULL
                                  CHECK (competitiveness_discount >= 0
                                         AND competitiveness_discount < 1),

    -- Descriptive only — decide_price() still derives the *actual*
    -- per-decision floor_policy from target_margin == 0 (ADR-0013 §5),
    -- unchanged by this phase.
    floor_policy_default     TEXT NOT NULL DEFAULT 'soft'
                                  CHECK (floor_policy_default IN ('hard', 'soft')),

    -- Set at insertion time (now()), not a user-chosen future effective
    -- date — CostDefinition's validity_start/validity_end (Phase 19)
    -- already support scheduling a change ahead of time; this table does
    -- not gain that same capability in this phase, a documented scope cut.
    effective_from            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (apartment_id, version)
);

-- ALTER PUBLICATION ... ADD TABLE has no IF NOT EXISTS form — it errors if
-- the table is already a publication member, so re-running this file (the
-- whole point of this being idempotent) needs an explicit guard.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'pricing_strategies'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.pricing_strategies;
    END IF;
END $$;

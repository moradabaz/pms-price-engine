-- Phase 11 (docs/adr/ADR-0011, backlog #5): replaces the implicit "commission
-- is charged on Total Revenue" assumption apartment_market_segments.commission_pct
-- carried since ADR-0009 (D2) with an explicit, configurable commission base.
-- One row per apartment (its PK) — a real contract, not just a relabeled
-- copy of the old column — with owner_id letting several apartments share
-- the same owner, resolving ADR-0011's own stance that this "needs a
-- net-new owner/contract entity, not a schema tweak."
--
-- Reuses the SAME Debezium publication/connector as payment_lines and
-- apartment_market_segments (dbz_publication) — no new connector, slot, or
-- publication (spec 11 pre-spec §B). Wiring
-- infra/debezium/postgres-connector.json's table.include.list to actually
-- include this table is tracked separately (Phase 11 implementation, not
-- this file) — this publication statement alone does not make Debezium
-- capture it.
--
-- Same idempotent-migration convention every other table here follows.
-- mock_pm_app/migrations.py embeds this same SQL as a Python string rather
-- than reading this file at runtime — keep them in sync by hand.

CREATE TABLE IF NOT EXISTS public.owner_contracts (
    apartment_id     TEXT PRIMARY KEY
                          REFERENCES public.apartment_market_segments(apartment_id),
    owner_id         TEXT NOT NULL REFERENCES public.owners(owner_id),

    -- ADR-0011 backlog #5: which revenue base commission_pct is charged
    -- against. 'total_revenue' reproduces this project's pre-Phase-11
    -- behavior exactly (decide_price()'s default, spec 11 §4).
    commission_base  TEXT NOT NULL DEFAULT 'total_revenue'
                          CHECK (commission_base IN
                              ('total_revenue', 'revenue_minus_ota',
                               'revenue_minus_ota_minus_cleaning')),

    -- Same field ADR-0009 (D2) introduced on apartment_market_segments,
    -- relocated here as this phase's single source of truth (spec 11 §3) —
    -- not duplicated in both tables.
    commission_pct   NUMERIC(5,4) NOT NULL DEFAULT 0.15
                          CHECK (commission_pct >= 0 AND commission_pct <= 1),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ
);

-- Same freshness-trigger pattern as apartment_market_segments.sql.
CREATE OR REPLACE FUNCTION public.set_owner_contract_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_owner_contracts_updated_at
    BEFORE UPDATE ON public.owner_contracts
    FOR EACH ROW
    EXECUTE FUNCTION public.set_owner_contract_updated_at();

-- ALTER PUBLICATION ... ADD TABLE has no IF NOT EXISTS form — needs an
-- explicit guard for this file to stay idempotent, same as
-- apartment_market_segments.sql's own publication guard.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'owner_contracts'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.owner_contracts;
    END IF;
END $$;

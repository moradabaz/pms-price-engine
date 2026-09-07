-- Phase 14 (docs/adr/ADR-0011, backlog #9): the first human write path into
-- this pipeline. One row per (apartment_id, target_date) — the natural key,
-- and already Debezium's default message key (no message.key.columns
-- override needed, unlike payment_lines' surrogate event_id).
--
-- Unlike every other table in specs/phases/01-mock-app-db/, this table has
-- NO seed function and NO mock_pm_app generator writing to it. It starts
-- empty and stays empty unless a human inserts a row directly (see
-- docs/manual/MANUAL.md's "Authoring a Manual Override" walkthrough) — that
-- manual act IS the feature (spec 14 §1).
--
-- Reuses the SAME Debezium publication/connector as payment_lines,
-- apartment_market_segments, and owner_contracts (dbz_publication) — no new
-- connector, slot, or publication (spec 14 §2). Wiring
-- infra/debezium/postgres-connector.json's table.include.list to actually
-- include this table is tracked separately (Phase 14 implementation, not
-- this file) — this publication statement alone does not make Debezium
-- capture it.
--
-- Cancelling a live override is an UPDATE (SET valid_until = now()), never a
-- DELETE — the existing global transforms.unwrap.delete.handling.mode=drop
-- setting silently drops delete events for every captured table, project-wide
-- (spec 14 §1/§7). No history table: the audit trail for "what price was
-- actually published while this override was active" lives in the
-- price_decision history this override produces (Iceberg), not in a
-- Postgres row history (spec 14 §1, same "current-state-only" convention
-- owner_contracts already established).

CREATE TABLE IF NOT EXISTS public.manual_overrides (
    apartment_id         TEXT NOT NULL
                              REFERENCES public.apartment_market_segments(apartment_id),
    target_date          DATE NOT NULL,

    override_price_eur   NUMERIC(10,2) NOT NULL
                              CHECK (override_price_eur >= 0),
    reason               TEXT NOT NULL,
    authorized_by        TEXT NOT NULL,
    valid_until          TIMESTAMPTZ NOT NULL,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ,

    PRIMARY KEY (apartment_id, target_date)
);

-- Same freshness-trigger pattern as owner_contracts.sql/
-- apartment_market_segments.sql — makes an UPDATE-based cancellation
-- (rather than a DELETE, see header) visible on read via updated_at.
CREATE OR REPLACE FUNCTION public.set_manual_override_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_manual_overrides_updated_at
    BEFORE UPDATE ON public.manual_overrides
    FOR EACH ROW
    EXECUTE FUNCTION public.set_manual_override_updated_at();

-- ALTER PUBLICATION ... ADD TABLE has no IF NOT EXISTS form — needs an
-- explicit guard for this file to stay idempotent, same as
-- owner_contracts.sql's own publication guard.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'manual_overrides'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.manual_overrides;
    END IF;
END $$;

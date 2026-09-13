-- Contract for the CDC source table behind the payment-events.v1 Kafka topic.
--
-- Every column here is part of the payment_line.v1 event contract
-- (specs/events/payment_line.v1.json). Changing a column here without updating
-- that schema (and vice versa) breaks the CDC contract — see ADR-0003.
--
-- This file is the authoritative source-table definition for Phase 1 (mock app
-- + payment_lines DB). It is not yet wired into infra/docker-compose.yml —
-- doing so (as a docker-entrypoint-initdb.d script) is Phase 1 implementation
-- work, tracked in specs/phases/01-mock-app-db/spec.md. Phase 2 (CDC pipeline,
-- specs/phases/02-cdc-pipeline/spec.md) depends on this table existing and
-- being actively written to by the Phase 1 mock app.
--
-- Phase 19 (ADR-0012): schema_version 2.0. concept/cost_type/is_shared/
-- allocation_ratio are REMOVED — cost_definition_id (references
-- cost_definitions.sql) replaces all four. This is the fresh-install shape;
-- an already-populated table is migrated in place (with a backfill, never a
-- data loss) by mock_pm_app/migrations.py, not by this file.
--
-- No FK constraint on cost_definition_id (or on apartment_id) here: this
-- script runs first in docker-entrypoint-initdb.d, before
-- apartment_market_segments.sql, and cost_definitions.sql isn't part of the
-- initdb sequence at all (only mock_pm_app/migrations.py creates it, at
-- application startup) — a forward FK reference to either table would fail
-- immediately on a fresh install.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- required for gen_random_uuid()

CREATE TABLE public.payment_lines (
    -- Idempotency key AND row identity. Stable for the lifetime of the row —
    -- never regenerated on UPDATE. Debezium/Flink rely on this being constant
    -- across a row's INSERT and all subsequent UPDATEs (ADR-0003, point 2):
    -- downstream consumers upsert keyed state by event_id, they do not sum
    -- messages.
    event_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Phase 19 (ADR-0012): bumped from '1.0'. Every prior phase only added
    -- fields; this one removes required ones (concept/cost_type/is_shared/
    -- allocation_ratio below), which is not backward compatible.
    schema_version       TEXT NOT NULL DEFAULT '2.0' CHECK (schema_version = '2.0'),

    apartment_id         TEXT NOT NULL,
    apartment_reference  TEXT NOT NULL,

    -- Phase 19 (ADR-0012): replaces concept/cost_type/is_shared/
    -- allocation_ratio entirely — CostDefinition is now the single source
    -- of truth for what this cost is (specs/phases/01-mock-app-db/
    -- cost_definitions.sql). No FK here — see header note above.
    cost_definition_id   UUID NOT NULL,

    description          TEXT NOT NULL,

    -- Flattened from the schema's original nested `supplier` object — see
    -- ADR-0003, point 3. Independently nullable at the DB level; the
    -- constraint "tax_id implies name" (if any) is an application-layer rule,
    -- not enforced here.
    supplier_name        TEXT,
    supplier_tax_id      TEXT,

    invoice_number       TEXT,

    -- Flattened from the schema's original nested `billing_period` object —
    -- see ADR-0003, point 3. This is the service period the cost covers, NOT
    -- the payment date.
    billing_period_start DATE NOT NULL,
    billing_period_end   DATE NOT NULL CHECK (billing_period_end >= billing_period_start),

    amount_gross         NUMERIC(10,2) NOT NULL CHECK (amount_gross >= 0),
    vat_rate             NUMERIC(4,3)  NOT NULL CHECK (vat_rate IN (0.0, 0.04, 0.10, 0.21)),

    -- Derived, not entered — removes any risk of amount_net drifting from
    -- amount_gross/vat_rate. Stored (not virtual) so it is emitted by Debezium
    -- like any other column instead of requiring computation downstream.
    amount_net            NUMERIC(10,2) GENERATED ALWAYS AS (round(amount_gross / (1 + vat_rate), 2)) STORED,

    currency              TEXT NOT NULL DEFAULT 'EUR' CHECK (currency = 'EUR'),

    due_date              DATE,
    payment_date          DATE,
    payment_method        TEXT CHECK (
                              payment_method IS NULL
                              OR payment_method IN ('bank_transfer', 'direct_debit', 'card', 'cash')
                          ),
    payment_status        TEXT NOT NULL CHECK (payment_status IN ('pending', 'paid', 'overdue', 'disputed')),

    source                TEXT NOT NULL CHECK (source IN ('bank_statement', 'manual_entry', 'synthetic')),

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ
);

-- event_id (the primary key) never changes on UPDATE, so the default replica
-- identity (primary key only) already gives Debezium everything it needs —
-- REPLICA IDENTITY FULL is not required here.

CREATE INDEX idx_payment_lines_apartment_id ON public.payment_lines (apartment_id);

-- updated_at must change on every UPDATE regardless of what the writing
-- application sets, so freshness can be trusted by downstream consumers.
CREATE OR REPLACE FUNCTION public.set_payment_line_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payment_lines_updated_at
    BEFORE UPDATE ON public.payment_lines
    FOR EACH ROW
    EXECUTE FUNCTION public.set_payment_line_updated_at();

-- Debezium's publication.autocreate.mode=filtered (infra/debezium/postgres-connector.json)
-- would create this automatically if missing, but creating it explicitly here
-- keeps the contract self-contained and fails fast if it ever drifts from
-- table.include.list.
CREATE PUBLICATION dbz_publication FOR TABLE public.payment_lines;

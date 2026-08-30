-- Phase 11 (docs/adr/ADR-0011, backlog #5): backs a real owner entity behind
-- the commission-base configurability owner_contracts.sql adds — several
-- apartments can share the same owner_id, the actual point of modeling a
-- real owner instead of a flat per-apartment column (as commission_pct was
-- before this phase). Not CDC-captured: nothing downstream (Flink, the
-- pricing formula, the dashboard) needs owner_name or any owner-level
-- rollup yet — only owner_contracts' already-resolved per-apartment
-- (commission_base, commission_pct) pair matters to decide_price().
--
-- Same idempotent-migration convention every other table here follows: runs
-- once via docker-entrypoint-initdb.d on a brand-new volume, and again via
-- mock_pm_app.migrations at every startup for an existing volume from before
-- this table existed. mock_pm_app/migrations.py embeds this same SQL as a
-- Python string rather than reading this file at runtime — keep them in
-- sync by hand if this file changes.

CREATE TABLE IF NOT EXISTS public.owners (
    owner_id    TEXT PRIMARY KEY,
    owner_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

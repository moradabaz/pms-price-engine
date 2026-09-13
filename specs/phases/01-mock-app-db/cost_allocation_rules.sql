-- Phase 19 (ADR-0011 backlog #13, ADR-0012): how a CostDefinition's already-
-- known amount converts into a per-night imputed figure. One rule per
-- definition for this phase (UNIQUE) — the external spec allows reuse, not
-- exploited here since nothing in this PoC needs the same rule shared
-- across definitions yet (spec 19 §2).

CREATE TABLE IF NOT EXISTS public.cost_allocation_rules (
    cost_allocation_rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cost_definition_id      UUID NOT NULL UNIQUE
                                REFERENCES public.cost_definitions(cost_definition_id),

    method                  TEXT NOT NULL CHECK (method IN
                                ('direct', 'calendar_day', 'available_night', 'occupied_night',
                                 'booking', 'revenue', 'weighted')),

    -- Only meaningful when method = 'weighted' (always paired with
    -- scope=company): a JSON map of {apartment_id: share}. Null under
    -- 'weighted' means equal split across every known apartment.
    weight_config           JSONB,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION public.set_cost_allocation_rule_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_cost_allocation_rules_updated_at
    BEFORE UPDATE ON public.cost_allocation_rules
    FOR EACH ROW
    EXECUTE FUNCTION public.set_cost_allocation_rule_updated_at();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'cost_allocation_rules'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.cost_allocation_rules;
    END IF;
END $$;

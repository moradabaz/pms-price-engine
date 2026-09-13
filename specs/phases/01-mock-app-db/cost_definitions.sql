-- Phase 19 (ADR-0011 backlog #13, ADR-0012): classifies a cost along the
-- external target spec's dimensions (section 8) instead of the old flat
-- concept/cost_type pair. Never carries an amount/rate/formula — a
-- CostDefinition classifies and spreads a real, already-known amount
-- (payment_lines.amount_gross / company_cost_occurrences.amount_gross), it
-- never computes one (see spec 19 §2).
--
-- Same "kept in sync by hand" convention as owner_contracts.sql:
-- mock_pm_app/migrations.py embeds this same SQL as a Python string.

CREATE TABLE IF NOT EXISTS public.cost_definitions (
    cost_definition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Phase 20 (ADR-0013): 'owner_commission' added — the owner-contract
    -- commission migrated here from owner_contracts.commission_pct/
    -- commission_base (spec 20 §2/§4), unified under CostDefinition like
    -- every other cost concept.
    concept            TEXT NOT NULL CHECK (concept IN (
                           'electricity', 'water', 'gas', 'internet', 'pms_subscription',
                           'ota_fee', 'channel_manager', 'office_rent', 'cleaning',
                           'maintenance', 'insurance', 'community_fee', 'other',
                           'owner_commission'
                       )),

    -- Which level this cost belongs to. company-scoped costs never appear
    -- in payment_lines (every row there belongs to one apartment) — they
    -- live in company_cost_occurrences instead (spec 19 §2).
    scope              TEXT NOT NULL CHECK (scope IN ('booking', 'property', 'company')),

    -- Reporting/classification only — never branched on for arithmetic
    -- (semi_variable folds into the variable bucket, spec 19 §2). fixed and
    -- variable are consumed exactly like the old cost_type values were.
    behavior           TEXT NOT NULL CHECK (behavior IN ('fixed', 'variable', 'semi_variable')),

    trigger            TEXT NOT NULL CHECK (trigger IN
                           ('reservation', 'night', 'guest', 'time', 'revenue', 'event')),

    calculation_base   TEXT NOT NULL CHECK (calculation_base IN
                           ('fixed_amount', 'pct_revenue', 'pct_adjusted_revenue',
                            'per_night', 'per_guest')),

    recurrence         TEXT NOT NULL CHECK (recurrence IN
                           ('per_booking', 'daily', 'monthly', 'quarterly', 'annual', 'one_off')),

    -- Only meaningful when calculation_base = 'pct_adjusted_revenue'. Reuses
    -- owner_contracts' own commission_base enum rather than a parallel one.
    revenue_base       TEXT CHECK (revenue_base IN
                           ('total_revenue', 'revenue_minus_ota', 'revenue_minus_ota_minus_cleaning')),

    -- Phase 20 (ADR-0013 §3): the actual configured rate for a percentage
    -- cost (e.g. 0.15 for a 15% OTA fee/commission) — required exactly when
    -- calculation_base is percentage-based, since a CostOccurrence's
    -- amount_gross is always a real invoiced EUR figure, never a rate the
    -- Break-Even formula could reuse prospectively (spec 20 §2/§4).
    rate               NUMERIC(6,4) CHECK (rate IS NULL OR (rate >= 0 AND rate <= 1)),
    CONSTRAINT cost_definitions_rate_matches_calculation_base CHECK (
        (calculation_base IN ('pct_revenue', 'pct_adjusted_revenue')) = (rate IS NOT NULL)
    ),

    validity_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    validity_end       DATE CHECK (validity_end IS NULL OR validity_end >= validity_start),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION public.set_cost_definition_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_cost_definitions_updated_at
    BEFORE UPDATE ON public.cost_definitions
    FOR EACH ROW
    EXECUTE FUNCTION public.set_cost_definition_updated_at();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'cost_definitions'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.cost_definitions;
    END IF;
END $$;

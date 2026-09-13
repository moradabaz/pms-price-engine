-- Phase 19 (ADR-0011 backlog #13, ADR-0012): a company-scoped cost's
-- actual, already-known amount for one billing period. Lives in its own
-- table rather than payment_lines because payment_lines is a keyed Kafka
-- stream (key_by(apartment_id)) — a company cost belongs to no single
-- apartment and cannot key a stream that way. Consumed as broadcast state
-- in Flink instead (spec 19 §2/§5), never keyed.

CREATE TABLE IF NOT EXISTS public.company_cost_occurrences (
    company_cost_occurrence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_version              TEXT NOT NULL DEFAULT '1.0' CHECK (schema_version = '1.0'),
    cost_definition_id         UUID NOT NULL
                                   REFERENCES public.cost_definitions(cost_definition_id),

    billing_period_start       DATE NOT NULL,
    billing_period_end         DATE NOT NULL CHECK (billing_period_end >= billing_period_start),
    amount_gross               NUMERIC(10,2) NOT NULL CHECK (amount_gross >= 0),
    description                TEXT NOT NULL,

    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION public.set_company_cost_occurrence_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_company_cost_occurrences_updated_at
    BEFORE UPDATE ON public.company_cost_occurrences
    FOR EACH ROW
    EXECUTE FUNCTION public.set_company_cost_occurrence_updated_at();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'company_cost_occurrences'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.company_cost_occurrences;
    END IF;
END $$;

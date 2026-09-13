from typing import Any

# Byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/apartment_market_segments.sql — kept as a
# Python string rather than read from that file at runtime so this
# doesn't depend on the repo layout being preserved inside the container.
# Must be kept in sync by hand if that file changes; see its own header
# comment for why this exists twice.
#
# This runs unconditionally at every mock-pm-app startup, not just on a fresh
# volume. Postgres only executes docker-entrypoint-initdb.d/ scripts against
# an empty data directory — an existing volume from a session before this
# table existed would otherwise never get it, and mock-pm-app would crash on
# its first query against apartment_market_segments. Every statement here is
# idempotent (IF NOT EXISTS / OR REPLACE / a guarded DO block for the
# publication), so running it again against a volume that already has the
# table (created by the initdb script) is a harmless no-op.
_ENSURE_APARTMENT_MARKET_SEGMENTS_SQL = """
CREATE TABLE IF NOT EXISTS public.apartment_market_segments (
    apartment_id         TEXT PRIMARY KEY,
    apartment_reference  TEXT NOT NULL,
    city                 TEXT NOT NULL
                             CHECK (city IN ('Barcelona', 'Madrid', 'Valencia')),
    neighborhood         TEXT NOT NULL,
    property_type        TEXT NOT NULL CHECK (property_type IN ('studio', 'apartment')),
    bedrooms             SMALLINT NOT NULL CHECK (bedrooms >= 0),
    target_margin            NUMERIC(5,4) NOT NULL DEFAULT 0.05
                                  CHECK (target_margin >= 0),
    competitiveness_discount NUMERIC(5,4) NOT NULL DEFAULT 0.05
                                  CHECK (competitiveness_discount >= 0
                                         AND competitiveness_discount <= 1),
    quality_tier         TEXT NOT NULL DEFAULT 'standard'
                                  CHECK (quality_tier IN
                                      ('basic', 'standard', 'premium', 'luxury')),
    rating                NUMERIC(2,1) NOT NULL DEFAULT 4.0
                                  CHECK (rating >= 1.0 AND rating <= 5.0),
    has_view              BOOLEAN NOT NULL DEFAULT false,
    has_parking           BOOLEAN NOT NULL DEFAULT false,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ
);

-- Phase 11 (docs/adr/ADR-0011, backlog #5): commission_pct moves to
-- owner_contracts.commission_pct — a real removal, not another additive
-- column, since this project's SOLID/DRY guidance rules out two sources of
-- truth for the same number. Drops the constraint first (DROP COLUMN would
-- cascade-drop it anyway, but explicit is safer against a constraint left
-- orphaned by a partial prior run).
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_commission_pct_check;
ALTER TABLE public.apartment_market_segments
    DROP COLUMN IF EXISTS commission_pct;

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

CREATE INDEX IF NOT EXISTS idx_apartment_market_segments_segment
    ON public.apartment_market_segments (city, neighborhood, property_type, bedrooms);

CREATE OR REPLACE FUNCTION public.set_apartment_market_segment_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_apartment_market_segments_updated_at
    BEFORE UPDATE ON public.apartment_market_segments
    FOR EACH ROW
    EXECUTE FUNCTION public.set_apartment_market_segment_updated_at();

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
"""


def ensure_apartment_market_segments_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_APARTMENT_MARKET_SEGMENTS_SQL)
    conn.commit()


# Phase 11 (ADR-0011 backlog #5): byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/owners.sql — same "kept in sync by hand,
# runs unconditionally at every startup" convention as
# _ENSURE_APARTMENT_MARKET_SEGMENTS_SQL above. Not CDC-captured (nothing
# downstream needs owner_name) — plain Postgres dimension only.
_ENSURE_OWNERS_SQL = """
CREATE TABLE IF NOT EXISTS public.owners (
    owner_id    TEXT PRIMARY KEY,
    owner_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_owners_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_OWNERS_SQL)
    conn.commit()


# Phase 11: byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/owner_contracts.sql. Reuses the existing
# dbz_publication/connector (spec 11 pre-spec §B) — no new connector, slot,
# or publication, one more captured table on the one already there.
#
# Phase 20 (ADR-0013 §4): commission_base/commission_pct are no longer part
# of a fresh table's shape — cost_definition_id replaces them (a real
# CostDefinition, resolved elsewhere). An already-populated table (still
# carrying the old columns) is migrated by
# migrate_owner_contracts_to_cost_definitions() below, not by this
# CREATE-TABLE-IF-NOT-EXISTS statement.
_ENSURE_OWNER_CONTRACTS_SQL = """
CREATE TABLE IF NOT EXISTS public.owner_contracts (
    apartment_id       TEXT PRIMARY KEY
                            REFERENCES public.apartment_market_segments(apartment_id),
    owner_id           TEXT NOT NULL REFERENCES public.owners(owner_id),
    cost_definition_id UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ
);

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
"""


def ensure_owner_contracts_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_OWNER_CONTRACTS_SQL)
    conn.commit()


# Phase 14 (ADR-0011 backlog #9): byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/manual_overrides.sql. Reuses the existing
# dbz_publication/connector (spec 14 §2) — no new connector, slot, or
# publication. Unlike every other _ENSURE_*_SQL here, there is no matching
# seed_*/already_seeded_* pair for this table (spec 14 §1) — it starts empty
# and stays empty unless a human inserts a row directly.
_ENSURE_MANUAL_OVERRIDES_SQL = """
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
"""


def ensure_manual_overrides_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_MANUAL_OVERRIDES_SQL)
    conn.commit()


# Phase 18 (ADR-0011 backlog #13 prerequisite): byte-identical (schema-wise)
# to specs/phases/01-mock-app-db/bookings.sql. Reuses the existing
# dbz_publication/connector — no new connector, slot, or publication.
_ENSURE_BOOKINGS_SQL = """
CREATE TABLE IF NOT EXISTS public.bookings (
    booking_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_version TEXT NOT NULL DEFAULT '1.0' CHECK (schema_version = '1.0'),
    apartment_id   TEXT NOT NULL
                       REFERENCES public.apartment_market_segments(apartment_id),
    check_in       DATE NOT NULL,
    check_out      DATE NOT NULL CHECK (check_out > check_in),
    channel        TEXT NOT NULL
                       CHECK (channel IN ('airbnb', 'booking', 'vrbo', 'direct')),
    guests         SMALLINT NOT NULL CHECK (guests >= 1),
    revenue_eur    NUMERIC(10,2) NOT NULL CHECK (revenue_eur >= 0),
    status         TEXT NOT NULL DEFAULT 'confirmed'
                       CHECK (status IN ('confirmed', 'cancelled')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ
);

-- Self-healing for a bookings table created before schema_version existed
-- (same pattern apartment_market_segments.sql's own column additions use).
ALTER TABLE public.bookings
    ADD COLUMN IF NOT EXISTS schema_version TEXT NOT NULL DEFAULT '1.0';
ALTER TABLE public.bookings
    DROP CONSTRAINT IF EXISTS bookings_schema_version_check;
ALTER TABLE public.bookings
    ADD CONSTRAINT bookings_schema_version_check CHECK (schema_version = '1.0');

CREATE INDEX IF NOT EXISTS idx_bookings_apartment_id ON public.bookings (apartment_id);

CREATE OR REPLACE FUNCTION public.set_booking_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_bookings_updated_at
    BEFORE UPDATE ON public.bookings
    FOR EACH ROW
    EXECUTE FUNCTION public.set_booking_updated_at();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_publication'
          AND schemaname = 'public'
          AND tablename = 'bookings'
    ) THEN
        ALTER PUBLICATION dbz_publication ADD TABLE public.bookings;
    END IF;
END $$;
"""


def ensure_bookings_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_BOOKINGS_SQL)
    conn.commit()


# Phase 19 (ADR-0011 backlog #13, ADR-0012): byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/cost_definitions.sql. Never carries an amount —
# classifies an already-known amount, never computes one. Phase 20 (ADR-0013
# §3) adds `rate`, the one exception: a percentage cost's rate is real
# configuration (like owner_contracts.commission_pct always was), not an
# amount, and the Break-Even formula needs it prospectively.
_ENSURE_COST_DEFINITIONS_SQL = """
CREATE TABLE IF NOT EXISTS public.cost_definitions (
    cost_definition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept            TEXT NOT NULL CHECK (concept IN (
                           'electricity', 'water', 'gas', 'internet',
                           'pms_subscription', 'ota_fee', 'channel_manager',
                           'office_rent', 'cleaning', 'maintenance', 'insurance',
                           'community_fee', 'other', 'owner_commission'
                       )),
    scope              TEXT NOT NULL
                           CHECK (scope IN ('booking', 'property', 'company')),
    behavior           TEXT NOT NULL
                           CHECK (behavior IN ('fixed', 'variable', 'semi_variable')),
    trigger            TEXT NOT NULL CHECK (trigger IN
                           ('reservation', 'night', 'guest', 'time',
                            'revenue', 'event')),
    calculation_base   TEXT NOT NULL CHECK (calculation_base IN
                           ('fixed_amount', 'pct_revenue', 'pct_adjusted_revenue',
                            'per_night', 'per_guest')),
    recurrence         TEXT NOT NULL CHECK (recurrence IN
                           ('per_booking', 'daily', 'monthly', 'quarterly',
                            'annual', 'one_off')),
    revenue_base       TEXT CHECK (revenue_base IN
                           ('total_revenue', 'revenue_minus_ota',
                            'revenue_minus_ota_minus_cleaning')),
    validity_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    validity_end       DATE
                           CHECK (validity_end IS NULL
                                  OR validity_end >= validity_start),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ
);

-- Self-healing for a cost_definitions table created before Phase 20 (same
-- pattern apartment_market_segments.sql's own column additions use):
-- widen the concept enum to include owner_commission, and add rate.
ALTER TABLE public.cost_definitions
    DROP CONSTRAINT IF EXISTS cost_definitions_concept_check;
ALTER TABLE public.cost_definitions
    ADD CONSTRAINT cost_definitions_concept_check CHECK (concept IN (
        'electricity', 'water', 'gas', 'internet', 'pms_subscription',
        'ota_fee', 'channel_manager', 'office_rent', 'cleaning',
        'maintenance', 'insurance', 'community_fee', 'other', 'owner_commission'
    ));

ALTER TABLE public.cost_definitions
    ADD COLUMN IF NOT EXISTS rate NUMERIC(6,4);
ALTER TABLE public.cost_definitions
    DROP CONSTRAINT IF EXISTS cost_definitions_rate_check;
ALTER TABLE public.cost_definitions
    ADD CONSTRAINT cost_definitions_rate_check
        CHECK (rate IS NULL OR (rate >= 0 AND rate <= 1));

-- Backfill BEFORE the pairing constraint below, not after (same ordering
-- bug fixed once already in the payment_lines schema_version migration) —
-- a pre-Phase-20 percentage CostDefinition (e.g. ota_fee) has rate=NULL
-- from the ADD COLUMN above and would otherwise immediately violate it.
UPDATE public.cost_definitions
    SET rate = 0.15
    WHERE calculation_base IN ('pct_revenue', 'pct_adjusted_revenue')
      AND rate IS NULL;

ALTER TABLE public.cost_definitions
    DROP CONSTRAINT IF EXISTS cost_definitions_rate_matches_calculation_base;
ALTER TABLE public.cost_definitions
    ADD CONSTRAINT cost_definitions_rate_matches_calculation_base CHECK (
        (calculation_base IN ('pct_revenue', 'pct_adjusted_revenue'))
        = (rate IS NOT NULL)
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
"""


def ensure_cost_definitions_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_COST_DEFINITIONS_SQL)
    conn.commit()


# Phase 19: byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/cost_allocation_rules.sql.
_ENSURE_COST_ALLOCATION_RULES_SQL = """
CREATE TABLE IF NOT EXISTS public.cost_allocation_rules (
    cost_allocation_rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cost_definition_id      UUID NOT NULL UNIQUE
                                REFERENCES public.cost_definitions(cost_definition_id),
    method                  TEXT NOT NULL CHECK (method IN
                                ('direct', 'calendar_day', 'available_night',
                                 'occupied_night', 'booking', 'revenue', 'weighted')),
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
"""


def ensure_cost_allocation_rules_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_COST_ALLOCATION_RULES_SQL)
    conn.commit()


# Phase 19: byte-identical (schema-wise) to
# specs/phases/01-mock-app-db/company_cost_occurrences.sql.
_ENSURE_COMPANY_COST_OCCURRENCES_SQL = """
CREATE TABLE IF NOT EXISTS public.company_cost_occurrences (
    company_cost_occurrence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_version              TEXT NOT NULL DEFAULT '1.0'
                                     CHECK (schema_version = '1.0'),
    cost_definition_id         UUID NOT NULL
                                   REFERENCES
                                       public.cost_definitions(cost_definition_id),
    billing_period_start       DATE NOT NULL,
    billing_period_end         DATE NOT NULL
                                   CHECK (billing_period_end >= billing_period_start),
    amount_gross               NUMERIC(10,2) NOT NULL CHECK (amount_gross >= 0),
    description                TEXT NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ
);

-- Self-healing for a table created before schema_version existed (same
-- pattern bookings.sql's own column addition uses, Phase 18).
ALTER TABLE public.company_cost_occurrences
    ADD COLUMN IF NOT EXISTS schema_version TEXT NOT NULL DEFAULT '1.0';
ALTER TABLE public.company_cost_occurrences
    DROP CONSTRAINT IF EXISTS company_cost_occurrences_schema_version_check;
ALTER TABLE public.company_cost_occurrences
    ADD CONSTRAINT company_cost_occurrences_schema_version_check
        CHECK (schema_version = '1.0');

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
"""


def ensure_company_cost_occurrences_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENSURE_COMPANY_COST_OCCURRENCES_SQL)
    conn.commit()


# Phase 19 (ADR-0012): the breaking payment_lines migration. Adds
# cost_definition_id (nullable at first), backfills it from every distinct
# (concept, cost_type) pair still present, then drops the legacy columns and
# enforces NOT NULL — all guarded so this is a harmless no-op once the
# legacy columns are already gone (checked via information_schema, not a
# flag column, since payment_lines itself carries no version marker beyond
# schema_version, which Debezium messages carry, not this migration).
#
# The mapping is not a straight 1:1 rename: cost_type='one_time' has no
# matching `behavior` value (behavior is fixed/variable/semi_variable, there
# is no "one_time") — semantically, "one time" was always about *recurrence*,
# not behavior. It maps to behavior='variable' + recurrence='one_off' (and
# allocation method 'direct', matching one_time_cost_eur's existing
# "average across lines in the period" semantics) instead of a value that
# doesn't exist in the new enum.
_MIGRATE_PAYMENT_LINES_TO_COST_DEFINITIONS_SQL = """
ALTER TABLE public.payment_lines
    ADD COLUMN IF NOT EXISTS cost_definition_id UUID
        REFERENCES public.cost_definitions(cost_definition_id);

DO $$
DECLARE
    legacy_columns_exist boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'payment_lines'
          AND column_name = 'concept'
    ) INTO legacy_columns_exist;

    IF legacy_columns_exist THEN
        INSERT INTO public.cost_definitions
            (concept, scope, behavior, trigger, calculation_base, recurrence)
        SELECT DISTINCT
            pl.concept,
            CASE WHEN pl.concept IN ('ota_fee', 'channel_manager')
                 THEN 'booking' ELSE 'property' END,
            CASE WHEN pl.cost_type = 'one_time' THEN 'variable' ELSE pl.cost_type END,
            'time',
            'fixed_amount',
            CASE WHEN pl.cost_type = 'one_time' THEN 'one_off' ELSE 'monthly' END
        FROM public.payment_lines pl
        WHERE NOT EXISTS (
            SELECT 1 FROM public.cost_definitions cd
            WHERE cd.concept = pl.concept
              AND cd.behavior = CASE WHEN pl.cost_type = 'one_time'
                                     THEN 'variable' ELSE pl.cost_type END
              AND cd.recurrence = CASE WHEN pl.cost_type = 'one_time'
                                       THEN 'one_off' ELSE 'monthly' END
        );

        INSERT INTO public.cost_allocation_rules (cost_definition_id, method)
        SELECT cd.cost_definition_id,
               CASE WHEN cd.recurrence = 'one_off' THEN 'direct' ELSE 'calendar_day' END
        FROM public.cost_definitions cd
        WHERE NOT EXISTS (
            SELECT 1 FROM public.cost_allocation_rules car
            WHERE car.cost_definition_id = cd.cost_definition_id
        );

        UPDATE public.payment_lines pl
        SET cost_definition_id = cd.cost_definition_id
        FROM public.cost_definitions cd
        WHERE pl.cost_definition_id IS NULL
          AND pl.concept = cd.concept
          AND cd.behavior = CASE WHEN pl.cost_type = 'one_time'
                                 THEN 'variable' ELSE pl.cost_type END
          AND cd.recurrence = CASE WHEN pl.cost_type = 'one_time'
                                   THEN 'one_off' ELSE 'monthly' END;

        ALTER TABLE public.payment_lines
            DROP CONSTRAINT IF EXISTS payment_lines_concept_check;
        ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS concept;
        ALTER TABLE public.payment_lines
            DROP CONSTRAINT IF EXISTS payment_lines_cost_type_check;
        ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS cost_type;
        ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS is_shared;
        ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS allocation_ratio;

        ALTER TABLE public.payment_lines ALTER COLUMN cost_definition_id SET NOT NULL;

        ALTER TABLE public.payment_lines
            DROP CONSTRAINT IF EXISTS payment_lines_schema_version_check;
        ALTER TABLE public.payment_lines ALTER COLUMN schema_version SET DEFAULT '2.0';
        UPDATE public.payment_lines SET schema_version = '2.0'
            WHERE schema_version = '1.0';
        ALTER TABLE public.payment_lines
            ADD CONSTRAINT payment_lines_schema_version_check
                CHECK (schema_version = '2.0');
    END IF;
END $$;
"""


def migrate_payment_lines_to_cost_definitions(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_MIGRATE_PAYMENT_LINES_TO_COST_DEFINITIONS_SQL)
    conn.commit()


# Phase 20 (ADR-0013 §4): the breaking owner_contracts migration — unifies
# commission_base/commission_pct under CostDefinition, the same real-
# substitution pattern _MIGRATE_PAYMENT_LINES_TO_COST_DEFINITIONS_SQL already
# established for payment_lines (ADR-0012). Must run after
# ensure_cost_definitions_schema/ensure_cost_allocation_rules_schema (this
# migration inserts into both). Guarded by information_schema, not a flag
# column, same convention as the payment_lines migration — harmless no-op
# once commission_pct is already gone.
_MIGRATE_OWNER_CONTRACTS_TO_COST_DEFINITIONS_SQL = """
ALTER TABLE public.owner_contracts
    ADD COLUMN IF NOT EXISTS cost_definition_id UUID;

DO $$
DECLARE
    legacy_columns_exist boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'owner_contracts'
          AND column_name = 'commission_pct'
    ) INTO legacy_columns_exist;

    IF legacy_columns_exist THEN
        INSERT INTO public.cost_definitions
            (concept, scope, behavior, trigger, calculation_base, recurrence,
             revenue_base, rate)
        SELECT DISTINCT
            'owner_commission', 'booking', 'variable', 'revenue',
            'pct_adjusted_revenue', 'per_booking',
            oc.commission_base, oc.commission_pct
        FROM public.owner_contracts oc
        WHERE NOT EXISTS (
            SELECT 1 FROM public.cost_definitions cd
            WHERE cd.concept = 'owner_commission'
              AND cd.revenue_base = oc.commission_base
              AND cd.rate = oc.commission_pct
        );

        INSERT INTO public.cost_allocation_rules (cost_definition_id, method)
        SELECT cd.cost_definition_id, 'direct'
        FROM public.cost_definitions cd
        WHERE cd.concept = 'owner_commission'
          AND NOT EXISTS (
              SELECT 1 FROM public.cost_allocation_rules car
              WHERE car.cost_definition_id = cd.cost_definition_id
          );

        UPDATE public.owner_contracts oc
        SET cost_definition_id = cd.cost_definition_id
        FROM public.cost_definitions cd
        WHERE oc.cost_definition_id IS NULL
          AND cd.concept = 'owner_commission'
          AND cd.revenue_base = oc.commission_base
          AND cd.rate = oc.commission_pct;

        ALTER TABLE public.owner_contracts
            DROP CONSTRAINT IF EXISTS owner_contracts_commission_base_check;
        ALTER TABLE public.owner_contracts DROP COLUMN IF EXISTS commission_base;
        ALTER TABLE public.owner_contracts
            DROP CONSTRAINT IF EXISTS owner_contracts_commission_pct_check;
        ALTER TABLE public.owner_contracts DROP COLUMN IF EXISTS commission_pct;

        ALTER TABLE public.owner_contracts
            ALTER COLUMN cost_definition_id SET NOT NULL;
    END IF;
END $$;
"""


def migrate_owner_contracts_to_cost_definitions(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(_MIGRATE_OWNER_CONTRACTS_TO_COST_DEFINITIONS_SQL)
    conn.commit()

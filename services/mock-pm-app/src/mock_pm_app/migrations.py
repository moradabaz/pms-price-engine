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
_ENSURE_OWNER_CONTRACTS_SQL = """
CREATE TABLE IF NOT EXISTS public.owner_contracts (
    apartment_id     TEXT PRIMARY KEY
                          REFERENCES public.apartment_market_segments(apartment_id),
    owner_id         TEXT NOT NULL REFERENCES public.owners(owner_id),
    commission_base  TEXT NOT NULL DEFAULT 'total_revenue'
                          CHECK (commission_base IN
                              ('total_revenue', 'revenue_minus_ota',
                               'revenue_minus_ota_minus_cleaning')),
    commission_pct   NUMERIC(5,4) NOT NULL DEFAULT 0.15
                          CHECK (commission_pct >= 0 AND commission_pct <= 1),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ
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

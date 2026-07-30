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
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ
);

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

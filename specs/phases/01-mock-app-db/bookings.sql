-- Phase 18 (docs/adr/ADR-0011, backlog #13 prerequisite): tracks which
-- nights an apartment is actually booked, so a CostAllocationRule can later
-- divide a shared cost by occupied/available nights instead of a flat
-- calendar split. Contract for the CDC source table behind the
-- booking-events.v1 Kafka topic (specs/events/booking.v1.json).
--
-- Same "kept in sync by hand" convention as owner_contracts.sql/
-- manual_overrides.sql: mock_pm_app/migrations.py embeds this same SQL as a
-- Python string rather than reading this file at runtime.

CREATE TABLE IF NOT EXISTS public.bookings (
    -- Identity/idempotency key — Debezium/Flink upsert keyed state by this,
    -- same convention payment_lines.event_id already established.
    booking_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Matches booking.v1's own required field — same convention
    -- payment_lines.schema_version already established.
    schema_version TEXT NOT NULL DEFAULT '1.0' CHECK (schema_version = '1.0'),

    apartment_id   TEXT NOT NULL REFERENCES public.apartment_market_segments(apartment_id),

    -- Standard hospitality convention: nights occupied are
    -- [check_in, check_out) — the night of check_out itself is not occupied.
    check_in       DATE NOT NULL,
    check_out      DATE NOT NULL CHECK (check_out > check_in),

    -- Includes 'direct', unlike price_decision.v1's ChannelPriceCandidate
    -- (airbnb/booking/vrbo only) — bookings is an independent entity, and
    -- the external target spec's own examples repeatedly compare Direct
    -- vs. OTA (section 12, T03).
    channel        TEXT NOT NULL CHECK (channel IN ('airbnb', 'booking', 'vrbo', 'direct')),

    guests         SMALLINT NOT NULL CHECK (guests >= 1),
    revenue_eur    NUMERIC(10,2) NOT NULL CHECK (revenue_eur >= 0),

    status         TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'cancelled')),

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ
);

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

-- Reuses the existing dbz_publication (same connector, slot as every other
-- table here) — no new connector, slot, or publication.
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

# Phase 18 — Bookings / Occupancy model

**Status:** Draft
**Depends on:** Phase 1 (`apartment_market_segments`), Phase 2 (CDC pipeline / Debezium connector), Phase 4 (Stage A cost aggregation, whose output this phase enriches)
**Blocks:** Phase 19 (`CostDefinition`/`CostOccurrence`/`CostAllocationRule`) — the `available_night`/`occupied_night` allocation rules need this phase's `occupied_nights` figure to exist first.
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) backlog #13, `/Users/morad/Downloads/dynamic_price_engine.md` section 8 (Allocation Rule column)

---

## 1. Executive summary

No table in this repo tracks which nights an apartment is actually booked. This blocks the
external target spec's `available_night`/`occupied_night` `CostAllocationRule` values, which need
a real occupied-vs-available night count per apartment per billing period to divide a cost by.
This phase adds exactly that: a `bookings` table, CDC'd the same way as every other Postgres table
here, and a new Flink stage that joins bookings onto `CostAggregate`, adding one new figure:
`occupied_nights` for the apartment's current billing period. `available_nights` is not a new
field — it is the existing `available_days` (calendar days in the billing period), since this PoC
has no apartment-blocked/maintenance concept yet (stated default, `docs/metrics-dictionary.md`-style
note left in `models.py`).

**Done when:** a real seeded booking, confirmed via Postgres → CDC → Kafka, produces a nonzero
`occupied_nights` on the next `CostAggregate` for that apartment, verified live against the running
stack (not just unit-tested) — cross-checked by hand against the seeded booking's date range.

**Not in this phase:** `CostDefinition`/`CostOccurrence`/`CostAllocationRule` themselves (Phase 19);
any apartment-blocked/maintenance-hold concept (would make `available_nights` a real new field
distinct from `available_days` — no anchor needs it yet); using `occupied_nights` in any pricing
formula (display/reporting-ready figure only, same "additive, not wired into `decide_price()` yet"
convention Phase 17's `cost_breakdown` established); cancellation replay/adjustment beyond the
`status` column itself (a booking that flips to `cancelled` is excluded from the *next*
recomputation, not retroactively removed from a `PriceDecision` already emitted — same "no
backfill" convention every broadcast-config change in this project already follows).

---

## 2. Scope

### In scope

**Postgres / mock-pm-app:**
- `specs/phases/01-mock-app-db/bookings.sql` (new) — see §3.
- `services/mock-pm-app/src/mock_pm_app/migrations.py` — `ensure_bookings_schema` (new, same
  self-healing `CREATE TABLE IF NOT EXISTS` pattern as every other table here).
- `services/mock-pm-app/src/mock_pm_app/data.py` — new `Booking` dataclass +
  `build_bookings(apartments, rng, today)`, generating overlapping/adjacent/gapped bookings per
  apartment so occupied vs. available nights are actually exercised (not every night booked, not
  every night empty).
- `services/mock-pm-app/src/mock_pm_app/bookings_rows.py` (new, mirrors `rows.py`'s shape) —
  `insert_booking`.
- `services/mock-pm-app/src/mock_pm_app/seed.py` — `seed_bookings`/`already_seeded_bookings`.
- `services/mock-pm-app/src/mock_pm_app/generator.py` — occasionally inserts a new live booking
  (same interval-loop shape as `insert_one`), so the stack keeps producing fresh occupancy changes.
- `services/mock-pm-app/src/mock_pm_app/main.py` — wires the new schema-ensure and seed calls.

**Debezium / infra:**
- `infra/debezium/postgres-connector.json` — `bookings` added to `table.include.list`; a new
  `routeBookings` `RegexRouter` transform to `booking-events.v1`; `message.key.columns` gains
  `public.bookings:apartment_id` (same override `payment_lines` already needs, since `bookings`'
  PK is `booking_id`, not `apartment_id`). No new connector, slot, or publication.
- `docs/manual/MANUAL.md` — a new manually-created Kafka topic, `booking-events.v1`.

**Shared schema / event contract:**
- `specs/events/booking.v1.json` (new) + `libs/shared-schemas/src/shared_schemas/booking.py` (new)
  — see §3.

**Flink (`streaming/flink-jobs`):**
- `streaming/flink-jobs/src/flink_jobs/occupancy.py` (new, pure function, mirrors
  `cost_aggregation.py`'s shape) — `occupied_nights(bookings, period_start, period_end) -> int`.
- `streaming/flink-jobs/src/flink_jobs/models.py` — new `BookingRow` (mirrors `ApartmentSegmentRow`
  — one CDC row, as received from Kafka); `CostAggregate` gains `occupied_nights: int = 0`.
- `streaming/flink-jobs/src/flink_jobs/stage_booking_enrichment.py` (new) —
  `BookingEnrichmentFunction(KeyedCoProcessFunction)`, chained right after `CostEnrichmentFunction`
  (Stage A), before `OwnerContractEnrichmentFunction` (Stage A2) — see §4.
- `streaming/flink-jobs/src/flink_jobs/job.py` — new `bookings` `KafkaSource`, a new
  `_parse_booking_row` parser, and the new stage wired between Stage A and Stage A2.
- `streaming/flink-jobs/src/flink_jobs/settings.py` — `booking_events_topic: str = "booking-events.v1"`.

**Contracts / tests:**
- `specs/contracts/fixtures/booking/` (new) — valid insert/update fixtures + at least one invalid
  fixture (missing required field), same shape as `specs/contracts/fixtures/payment_line/`.
- `specs/contracts/test_booking_contract.py` (new).
- Unit tests: `occupancy.py` (overlap arithmetic across period boundaries, cancelled bookings
  excluded, overlapping bookings not double-counted, zero bookings), `stage_booking_enrichment.py`
  (new test file, mirrors `test_stage_cost_enrichment.py`'s shape — cost-side-first, booking-side-
  first, both orders).
- **Live LocalStack verification is a required acceptance criterion** — new Postgres table, new
  Debezium-captured table, new Kafka topic, new Flink stage: exactly the class of change this
  project's own precedent (Phase 11 AC-11, `error-handling/*.md` incidents) says unit tests alone
  can't fully vouch for.

### Out of scope (explicitly deferred)

- Apartment-blocked/maintenance-hold concept (`available_nights` as a field distinct from
  `available_days`) — no anchor needs it yet.
- Any pricing-formula use of `occupied_nights` — that is Phase 19/20's job, once
  `CostAllocationRule` exists to consume it.
- Historical replay when a booking is cancelled or modified after a decision already used the old
  occupancy figure — future-only, matching every other broadcast-config change in this project.
- `guests` beyond storing it on `bookings` — Phase 19's `per_guest` trigger/calculation-base is
  what actually reads it; this phase only makes the data exist.

---

## 3. Data model

### `bookings` (new)

```sql
CREATE TABLE IF NOT EXISTS public.bookings (
    booking_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    apartment_id   TEXT NOT NULL REFERENCES public.apartment_market_segments(apartment_id),
    check_in       DATE NOT NULL,
    check_out      DATE NOT NULL CHECK (check_out > check_in),
    channel        TEXT NOT NULL CHECK (channel IN ('airbnb', 'booking', 'vrbo', 'direct')),
    guests         SMALLINT NOT NULL CHECK (guests >= 1),
    revenue_eur    NUMERIC(10,2) NOT NULL CHECK (revenue_eur >= 0),
    status         TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'cancelled')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_bookings_apartment_id ON public.bookings (apartment_id);
```

`channel` includes `direct` (unlike `ChannelPriceCandidate.platform`, which only knows
`airbnb`/`booking`/`vrbo` today) — matching the source spec's own repeated Direct-vs-OTA
comparisons (section 12, T03). `bookings` does not need to agree with `ChannelPriceCandidate`'s
enum; they are independent entities.

`check_in`/`check_out` follow the standard hospitality convention: nights occupied are
`[check_in, check_out)` — the night of `check_out` itself is not occupied (the guest leaves that
morning).

Reuses the existing `dbz_publication`/connector — no new connector, slot, or publication. Routed
to `booking-events.v1`.

### `booking.v1` event contract

Mirrors the table 1:1 (same "flatten, no nesting" convention `payment_line.v1` established):
`booking_id`, `apartment_id`, `check_in`, `check_out`, `channel`, `guests`, `revenue_eur`,
`status`, `created_at`, `updated_at`.

### `CostAggregate` — new field

| Field | Type | Source |
|---|---|---|
| `occupied_nights` | `int` | Resolved by the new Stage A-bookings hop from `occupancy.py`, for the apartment's current `billing_period_start`/`billing_period_end`. Default `0` until the new stage has seen at least one booking event for this apartment. |

---

## 4. Flink stage design

`BookingEnrichmentFunction(KeyedCoProcessFunction)`, keyed by `apartment_id` on both inputs (same
key `CostEnrichmentFunction`'s output and the new booking stream already share):

- **State:** `latest_cost_aggregate: ValueState[CostAggregate]` (the last `CostAggregate` seen for
  this apartment — needed so a booking arriving on its own, with no new cost event, can still
  trigger a re-emission), `bookings: MapState[str, Booking]` (upsert by `booking_id`, same
  "MapState keyed by natural id" convention `cost_lines_state` already uses).
- **`process_element1` (a `CostAggregate` arrives, Stage A's output):** store it in
  `latest_cost_aggregate`; compute `occupied_nights` from the current `bookings` state against
  this aggregate's billing period (`occupancy.py`); emit `dataclasses.replace(value,
  occupied_nights=...)`.
- **`process_element2` (a `BookingRow` arrives):** upsert into `bookings` state; if
  `latest_cost_aggregate` has a cached value, recompute `occupied_nights` against its billing
  period and re-emit an enriched copy of it (a booking change can shift occupancy for a period
  Stage A already emitted, without needing a new cost event to notice). If no cost aggregate has
  been seen yet for this apartment, do not emit — nothing to enrich (same "skip, don't buffer"
  rule every other broadcast/join stage here already follows for a not-yet-known counterpart).
- **Retention:** simpler than payment lines' 2-distinct-period-ends rule (which assumes discrete,
  non-overlapping periods — bookings don't cluster that way). On every booking upsert, once a
  `CostAggregate` has been cached at least once, prune any booking whose `check_out` is more than
  90 days before the cached aggregate's `billing_period_start` — a generous buffer past the
  current billing period, bounding state growth without risking evicting anything the current
  occupancy calculation could still need. Before a `CostAggregate` has ever arrived for this
  apartment, no pruning happens yet (nothing to compare against).

Wired in `job.py` immediately after `CostEnrichmentFunction` (Stage A) and before
`OwnerContractEnrichmentFunction` (Stage A2) — a regular two-keyed-stream join
(`KeyedCoProcessFunction`, no broadcast), the same shape as Stage B's cost/market join, not a third
broadcast hop.

---

## 5. Occupancy calculation

```python
def occupied_nights(bookings: Iterable[Booking], period_start: date, period_end: date) -> int:
```

Builds the set of individual night-dates covered by every `status == "confirmed"` booking
(`{check_in + timedelta(days=i) for i in range((check_out - check_in).days)}`), unions across all
bookings (so two overlapping bookings — a data-entry error in this PoC's synthetic generator, or a
same-day turnover — never double-count a night), intersects with the inclusive
`[period_start, period_end]` range, and returns the count. `cancelled` bookings are excluded
entirely. Zero bookings (or none confirmed) returns `0`.

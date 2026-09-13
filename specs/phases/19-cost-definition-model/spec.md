# Phase 19 — CostDefinition / CostOccurrence / CostAllocationRule

**Status:** Draft
**Depends on:** Phase 18 (bookings/occupancy — `occupied_nights`, and this phase adds
`booking_count` alongside it), Phase 1/2 (mock-pm-app, CDC pipeline)
**Blocks:** Phase 20 (rewiring `libs/pricing-formulas`, Iceberg, dbt, dashboard onto the richer
cost representation this phase produces)
**Related:** ADR-0012 (the breaking-change decision this phase implements), ADR-0011 backlog #13,
`/Users/morad/Downloads/dynamic_price_engine.md` sections 8/21, `docs/post-poc-roadmap.md`

---

## 1. Executive summary

`payment_line.v1`'s `concept`/`cost_type` pair is replaced by a `cost_definition_id` reference into
a new `cost_definitions` table, which classifies a cost along the external spec's 6 dimensions
(scope, behavior, trigger, calculation_base, recurrence — plus a validity window) instead of one.
A second new table, `cost_allocation_rules`, says how a cost's already-known real amount
(`PaymentLine.amount_gross` — this phase does not compute amounts, only spread and classify ones
that are already invoiced) converts into a per-night imputed figure. `payment_lines` itself loses
`concept`/`cost_type`/`is_shared`/`allocation_ratio` entirely (ADR-0012) — a real substitution, not
an additive field.

Company-scoped costs (one cost applying to every apartment, e.g. shared office rent) cannot live in
`payment_lines` at all, since every row there belongs to exactly one apartment
(`apartment_id NOT NULL`). They get their own new table, `company_cost_occurrences`, broadcast to
every apartment the same way owner-contract config already is — the corrected fan-out design from
ADR-0012 (broadcast state read from the keyed side, not a push, since PyFlink has no
`applyToKeyedState`).

**Done when:** a real seeded `office_rent` `CostDefinition` (`scope=company`) contributes its
correctly-weighted per-night share to every seeded apartment's next `CostAggregate`, and a real
`property`-scoped cost using `allocation_rule=occupied_night` produces a different per-night figure
than one using `calendar_day` for the same apartment — both verified live against the running
stack, with existing decisions still flowing end to end (this phase does not touch
`libs/pricing-formulas`, so `decide_price()` keeps consuming `fixed_cost_eur`/`variable_cost_eur`/
`one_time_cost_eur`, now computed from `CostDefinition.behavior` instead of the old
`PaymentLine.cost_type`).

**Not in this phase:** any change to `libs/pricing-formulas`, `price_decision.v1`, Iceberg, dbt, or
the dashboard (Phase 20); computing an amount from a rate/formula (`CostDefinition` classifies an
already-known `amount_gross`, it never derives one — see §3, this resolves the "semi-variable
formula" ambiguity raised earlier: there isn't one, because the amount is always the real invoiced
figure); a generic multi-apartment weighting UI or per-owner cost splitting beyond an explicit
`weight_config` map; historical replay/backfill of `PriceDecision`s already emitted when a
`CostDefinition` changes (ADR-0012).

---

## 2. Data model

### `cost_definitions` (new)

```sql
CREATE TABLE IF NOT EXISTS public.cost_definitions (
    cost_definition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept            TEXT NOT NULL CHECK (concept IN (
                           'electricity', 'water', 'gas', 'internet', 'pms_subscription',
                           'ota_fee', 'channel_manager', 'office_rent', 'cleaning',
                           'maintenance', 'insurance', 'community_fee', 'other'
                       )),
    scope              TEXT NOT NULL CHECK (scope IN ('booking', 'property', 'company')),
    behavior           TEXT NOT NULL CHECK (behavior IN ('fixed', 'variable', 'semi_variable')),
    trigger            TEXT NOT NULL CHECK (trigger IN
                           ('reservation', 'night', 'guest', 'time', 'revenue', 'event')),
    calculation_base   TEXT NOT NULL CHECK (calculation_base IN
                           ('fixed_amount', 'pct_revenue', 'pct_adjusted_revenue',
                            'per_night', 'per_guest')),
    recurrence         TEXT NOT NULL CHECK (recurrence IN
                           ('per_booking', 'daily', 'monthly', 'quarterly', 'annual', 'one_off')),
    -- Only meaningful when calculation_base = 'pct_adjusted_revenue'. Reuses owner_contracts'
    -- own commission_base enum rather than inventing a parallel one (client question #4,
    -- resolved: same concept, same values).
    revenue_base       TEXT CHECK (revenue_base IN
                           ('total_revenue', 'revenue_minus_ota', 'revenue_minus_ota_minus_cleaning')),
    validity_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    validity_end       DATE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ
);
```

**Why no rate/amount/formula field:** `CostOccurrence` (§2's `payment_lines`/
`company_cost_occurrences`) always carries a real, already-known `amount_gross` — an actual
invoice or bill, exactly like today. `CostDefinition` only classifies and spreads that number; it
never computes one from a formula. This is why `behavior=semi_variable` needs no special-cased
math here: it is a reporting/classification label (this cost's nature, for later Revenue
Management use), not something Stage A's arithmetic branches on. `fixed`/`variable` behavior
values are consumed exactly like today's `cost_type` was, for backward-compatible bucketing into
`fixed_cost_eur`/`variable_cost_eur` (see §4); `semi_variable` folds into `variable_cost_eur` for
now (a documented simplification, revisited only if Phase 20 needs otherwise).

### `cost_allocation_rules` (new)

```sql
CREATE TABLE IF NOT EXISTS public.cost_allocation_rules (
    cost_allocation_rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cost_definition_id      UUID NOT NULL UNIQUE
                                REFERENCES public.cost_definitions(cost_definition_id),
    method                  TEXT NOT NULL CHECK (method IN
                                ('direct', 'calendar_day', 'available_night', 'occupied_night',
                                 'booking', 'revenue', 'weighted')),
    -- Only meaningful when method = 'weighted' (always paired with scope=company): a JSON map of
    -- {apartment_id: share}. Null under 'weighted' means equal split across every known apartment.
    weight_config           JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ
);
```

One rule per definition (`UNIQUE`) for this phase — the external spec allows reuse, not exploited
here since nothing in this PoC needs the same rule shared across definitions yet.

**Recurrence divisor (Stage A0, a plain constant table, not a DB column):**
`per_booking`/`daily`/`monthly`/`one_off` → divide `amount_gross` by 1 (already a monthly-or-less
figure); `quarterly` → divide by 3; `annual` → divide by 12. This produces a "monthly-equivalent
amount" before any allocation method runs — the direct fix for the "annual insurance lump-sum"
problem named in the roadmap's own anchor for this backlog item.

**Allocation method → per-night formula**, given the monthly-equivalent amount `M`:

| Method | Formula | Needs |
|---|---|---|
| `direct` | Averaged across however many such lines exist in the period (same "one turnover" semantics `one_time_cost_eur` already uses) | Nothing extra |
| `calendar_day` | `M / available_days` | Nothing extra (today's existing formula) |
| `available_night` | Same as `calendar_day` in this PoC (no apartment-blocked concept yet, Phase 18 §note) | Nothing extra |
| `occupied_night` | `M / occupied_nights` (falls back to `calendar_day` if `occupied_nights == 0`) | Phase 18's `occupied_nights` |
| `booking` | `M / max(1, booking_count)` | A new `booking_count` figure (§4 — Phase 18's booking stage gains this alongside `occupied_nights`) |
| `revenue` | Reported separately as `revenue_weighted_cost_ratio = M / total_booking_revenue`, **not** folded into the per-night blended total (different unit — EUR per EUR of revenue, not EUR per night). Deferred: applying this ratio live per booking is Phase 20+ work. | Booking revenue totals |
| `weighted` | Only for `scope=company`: this apartment's share = `M × weight_for(apartment_id)`, then that share `/ available_days` (composes with `calendar_day`) | `weight_config` + the company fan-out (§5) |

### `payment_lines` — breaking change (ADR-0012)

```sql
ALTER TABLE public.payment_lines
    DROP CONSTRAINT IF EXISTS payment_lines_concept_check;
ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS concept;
ALTER TABLE public.payment_lines
    DROP CONSTRAINT IF EXISTS payment_lines_cost_type_check;
ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS cost_type;
ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS is_shared;
ALTER TABLE public.payment_lines DROP COLUMN IF EXISTS allocation_ratio;

ALTER TABLE public.payment_lines
    ADD COLUMN IF NOT EXISTS cost_definition_id UUID
        REFERENCES public.cost_definitions(cost_definition_id);
-- NOT NULL added only after the backfill (below) completes, so an in-place migration on a
-- populated table never fails on rows that don't have it yet.
```

**Backfill migration (confirmed with the user), run once:** for every distinct `(concept,
cost_type)` pair already observed in existing rows, insert one `CostDefinition` (`scope=booking`
if the concept is `ota_fee`/`channel_manager`, else `property` — matching how those two concepts
already netted specially in Phase 11; `behavior` = the old `cost_type` value 1:1;
`trigger=time`/`calculation_base=fixed_amount`/`recurrence=monthly` as a reasonable default for
backfilled data, since the original rows carry no information beyond `concept`/`cost_type`) plus a
matching `calendar_day` `cost_allocation_rule`, then `UPDATE payment_lines SET cost_definition_id
= ... WHERE concept = ... AND cost_type = ...`. Guarded to run once (skipped once every row has a
non-null `cost_definition_id`).

### `company_cost_occurrences` (new)

```sql
CREATE TABLE IF NOT EXISTS public.company_cost_occurrences (
    company_cost_occurrence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cost_definition_id         UUID NOT NULL
                                   REFERENCES public.cost_definitions(cost_definition_id),
    billing_period_start       DATE NOT NULL,
    billing_period_end         DATE NOT NULL CHECK (billing_period_end >= billing_period_start),
    amount_gross               NUMERIC(10,2) NOT NULL CHECK (amount_gross >= 0),
    description                TEXT NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ
);
```

**Why a separate table, not a nullable `payment_lines.apartment_id`:** `payment_lines` is a keyed
Kafka stream (`key_by(apartment_id)`) — a null key cannot be keyed. A company-wide cost needs to
reach *every* apartment, which is exactly what broadcast state is for, not a keyed stream. Reuses
`dbz_publication`; routed to a new `company-cost-occurrences.v1` topic, consumed as a **broadcast**
stream in Flink, never keyed.

---

## 3. Event contracts

- `specs/events/payment_line.v1.json` — `schema_version` const becomes `"2.0"` (ADR-0012);
  `concept`/`cost_type`/`is_shared`/`allocation_ratio` removed from `required` and `properties`;
  `cost_definition_id` (`format: uuid`) added, required.
- `specs/events/company_cost_occurrence.v1.json` (new) + `libs/shared-schemas/.../
  company_cost_occurrence.py` — mirrors the table 1:1, same flat-CDC-event shape as every other
  contract here.
- `specs/contracts/fixtures/payment_line/*.json` — all fixtures updated to the new shape (5
  existing files); a new `invalid_legacy_v1_shape.json` regression fixture (the OLD concept/
  cost_type shape) asserting it now fails validation — same "old shape becomes a rejected
  regression fixture" pattern ADR-0003's own fixtures already established for a prior breaking
  change.
- `specs/contracts/fixtures/company_cost_occurrence/` (new) — valid insert + invalid missing
  required, same shape as `booking`'s own fixture set (Phase 18).

---

## 4. Flink pipeline changes

**New Stage A0 — `stage_cost_definition_resolution.py` (`CostDefinitionResolutionFunction`,
`KeyedBroadcastProcessFunction`, keyed by `apartment_id`):** chained *before* the existing Stage A
(`CostEnrichmentFunction`). Broadcasts `cost_definitions` joined with `cost_allocation_rules`
(one `CostDefinitionAssignment` per `cost_definition_id`, keyed by it in broadcast state).
`process_element` (a `PaymentLine`) looks up `value.cost_definition_id`; if not yet known, skip (no
emission — same "skip, don't buffer" rule every broadcast join here already follows); otherwise
emits an `EnrichedPaymentLine` (a new dataclass: everything `PaymentLine` still has, plus
`concept`/`scope`/`behavior`/`trigger`/`calculation_base`/`recurrence`/`allocation_method`/
`weight_config` resolved from the definition).

**`cost_aggregation.py` — rewritten to consume `EnrichedPaymentLine` instead of `PaymentLine`:**
`aggregate_cost()`'s `fixed`/`variable`/`one_time` grouping now reads `line.behavior`
(`fixed`/`variable`/`semi_variable`, with `semi_variable` folded into the `variable` bucket) instead
of `line.cost_type` — numerically a drop-in replacement once the backfill migration (§2) has run,
since every existing row's `behavior` was seeded 1:1 from its old `cost_type`. `OTA_RELATED_CONCEPTS`/
`CLEANING_CONCEPTS` now filter on `line.concept` as resolved from `CostDefinition`, not from the
line itself. New: `cost_occurrence_breakdown` — one entry per `cost_definition_id` present in the
period, carrying the resolved `concept`/`calculation_base`/`allocation_method` and either its
already-final per-night amount (for `direct`/`calendar_day`/`available_night`/`weighted`, computable
here) or a placeholder computed with `calendar_day` pending correction (for `occupied_night`/
`booking`, which need Phase 18 data not yet available at this point in the pipeline — see below).

**Phase 18's `occupancy.py`/`stage_booking_enrichment.py` gain one more figure:** `booking_count(
bookings, period_start, period_end) -> int` (confirmed bookings whose stay overlaps the period at
all), alongside the already-verified `occupied_nights`. Additive to already-shipped, live-verified
code — not a change to `occupied_nights`' own logic.

**New Stage A-correction — `stage_allocation_correction.py` (`AllocationCorrectionFunction`,
a plain `MapFunction`, no state):** chained *after* `BookingEnrichmentFunction` (Phase 18, which now
carries both `occupied_nights` and `booking_count` on `CostAggregate`). For every
`cost_occurrence_breakdown` entry whose `allocation_method` is `occupied_night` or `booking`,
re-derives the correct per-night figure from the entry's own stored monthly-equivalent amount
(never re-reads Postgres or Kafka — pure arithmetic over data already on `CostAggregate`). Entries
using any other method pass through unchanged. This keeps Stage A's existing logic and Phase 18's
already-verified stage completely untouched; the correction is a small, separately-testable,
additive stage.

**New Stage A4 — `stage_company_cost_enrichment.py` (`CompanyCostEnrichmentFunction`,
`KeyedBroadcastProcessFunction`, keyed by `apartment_id`):** chained after the allocation-correction
stage. Broadcasts `company_cost_occurrences` (each keyed by its own id in broadcast state).
`process_element` (a `CostAggregate`): for every broadcast company cost occurrence, resolves its
`CostDefinition`'s `weight_config` (via the same `cost_definitions` broadcast Stage A0 already
consumes — this stage additionally connects to it) to get this apartment's share, applies the
`weighted` formula (§2), and adds the result into `fixed_cost_eur` (or `variable_cost_eur`,
per `behavior`) and a `cost_occurrence_breakdown` entry. `process_broadcast_element` just stores
the occurrence. No occurrence yet known → apartment's own figures are simply unaffected (nothing to
add), never blocked.

**`job.py` wiring, in order:** `payment_stream` → Stage A0 (cost-definition resolution) → Stage A
(cost aggregation, now reading `EnrichedPaymentLine`) → Stage A-bookings (Phase 18, unchanged) →
Stage A-correction (occupied_night/booking fixups) → Stage A4 (company cost fan-out) → Stage A2
(owner contract) → Stage B (unchanged).

**`CostAggregate` new fields:** `cost_occurrence_breakdown: tuple[CostOccurrenceAmount, ...]`,
`revenue_weighted_cost_ratio: float | None = None`, `booking_count: int = 0` (alongside Phase 18's
`occupied_nights`).

---

## 5. Company cost fan-out — corrected design (ADR-0012)

No `applyToKeyedState` in PyFlink. `company_cost_occurrences` broadcasts to every apartment;
each apartment applies its computed share the next time **its own** event reaches Stage A4 (a new
`PaymentLine`, a new booking, or the owner-contract/segment broadcasts' own periodic re-triggers —
whatever next causes this apartment's keyed state to be touched). A quiet apartment does not see a
company cost update instantly; it sees it on its own next event, identical to how an owner-contract
commission change already behaves today.

---

## 6. Scope

### In scope
- `specs/phases/01-mock-app-db/`: `cost_definitions.sql`, `cost_allocation_rules.sql`,
  `company_cost_occurrences.sql` (new); `payment_lines.sql` updated (breaking change).
- `services/mock-pm-app/`: migrations (new tables + `payment_lines` column changes + the one-time
  backfill), `data.py` (`CostDefinition`/`CostAllocationRule`/`CompanyCostOccurrence` dataclasses
  and builders — including finally generating `office_rent` as `scope=company`, closing the gap
  Phase 18's exploration found), seed/generator/main wiring.
- `infra/debezium/postgres-connector.json`: `cost_definitions`, `cost_allocation_rules`,
  `company_cost_occurrences` added to `table.include.list`; new `RegexRouter` transforms; explicit
  `message.key.columns` entries wherever the table's PK isn't `apartment_id`.
- `specs/events/payment_line.v1.json` (breaking), `specs/events/company_cost_occurrence.v1.json`
  (new), matching `libs/shared-schemas` models.
- `streaming/flink-jobs/`: `models.py`, `cost_aggregation.py`, `occupancy.py` (additive), new
  `stage_cost_definition_resolution.py`, `stage_allocation_correction.py`,
  `stage_company_cost_enrichment.py`, `job.py`, `settings.py`.
- Contract fixtures + unit tests for every new/changed module; live verification (§ Done when).

### Out of scope (explicitly deferred)
- `libs/pricing-formulas`, `price_decision.v1`, Iceberg, dbt, dashboard (Phase 20).
- The `revenue` allocation method's ratio actually being applied per booking (reported, not
  consumed, this phase).
- Any UI/panel for authoring `CostDefinition`/`CostAllocationRule`/`weight_config` — Postgres rows,
  same as every other config table in this project.
- Historical replay of already-emitted decisions on a `CostDefinition` change (ADR-0012).

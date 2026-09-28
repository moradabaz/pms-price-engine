# Phase 24 — Additional owner-contract revenue bases

**Status:** Implemented (2026-09-14) — see §7 for corrections made during implementation
**Depends on:** Phase 20 (BER/MPR rewire; `RevenueBase`/netting mechanism this phase extends,
ADR-0013 §3)
**Blocks:** nothing further planned
**Related:** ADR-0017 (the decision this phase implements), ADR-0011 backlog #5, `/Users/morad/Downloads/dynamic_price_engine.md`
§11.1 (owner contract models), test scenarios T04/T05, `docs/post-poc-roadmap.md`

---

## 1. Executive summary

The external spec (§11.1) lists five owner-contract revenue bases: Total Reservation; Reservation −
OTA; Reservation − OTA − Cleaning; Reservation − OTA − Cleaning − Laundry; Reservation − all Booking
Costs. `libs/pricing-formulas/src/pricing_formulas/layers/commercial.py`'s `RevenueBase` Literal
supports exactly the first three. This phase adds the remaining two, extending the existing netting
mechanism (`netted_revenue_base_amount()`) rather than replacing it — the same additive pattern
every `RevenueBase` value already follows.

**Done when:** a real seeded owner contract configured with `revenue_minus_ota_minus_cleaning_minus_laundry`
or `revenue_minus_all_booking_costs` produces a `netting_eur` in a live decision matching a hand
computation from that apartment's own seeded `ota_fee`/`cleaning`/`laundry`/total-booking-scope cost
lines for the period — verified live against LocalStack, alongside the three existing bases'
behavior remaining unchanged (regression, not just addition).

**Not in this phase:**
- A generic, arbitrary-formula "solver" for owner-contract bases (client spec §11.1's closing
  suggestion — "the engine must build the contractual base symbolically or evaluate the equation
  with a deterministic solver"). This project's existing five (after this phase) bases are all
  linear, enumerable subtractions from Total Revenue — a closed Literal with one netting function
  per value remains simpler and fully testable; a general solver is explicitly deferred, matching
  the client document's own framing of it as a "decisions to validate" open question (§29), not a
  committed requirement.
- Per-channel owner-contract bases (a contract that nets differently on Airbnb vs. Direct) — every
  base still applies uniformly across `decide_price_by_channel()`'s candidates, same as today.

---

## 2. Current state (read before writing code)

`layers/commercial.py`:

```python
RevenueBase = Literal[
    "total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"
]

def netted_revenue_base_amount(
    revenue_base: RevenueBase,
    ota_related_cost_eur: float,
    cleaning_cost_eur: float,
) -> float:
    if revenue_base == "total_revenue":
        return 0.0
    if revenue_base == "revenue_minus_ota":
        return ota_related_cost_eur
    return ota_related_cost_eur + cleaning_cost_eur
```

`ota_related_cost_eur`/`cleaning_cost_eur` are resolved upstream in
`streaming/flink-jobs/src/flink_jobs/cost_aggregation.py` via `OTA_RELATED_CONCEPTS`/
`CLEANING_CONCEPTS` concept-filters over the apartment's `EnrichedPaymentLine`s for the period
(Phase 19/20's `CostDefinition.concept` classification). `mock-pm-app`'s `_COMMISSION_BASES`
(`services/mock-pm-app/src/mock_pm_app/data.py`) currently seeds only the first three values.

---

## 3. Extension

### New concept-filter, `layers/commercial.py`

```python
RevenueBase = Literal[
    "total_revenue",
    "revenue_minus_ota",
    "revenue_minus_ota_minus_cleaning",
    "revenue_minus_ota_minus_cleaning_minus_laundry",   # new
    "revenue_minus_all_booking_costs",                   # new
]

def netted_revenue_base_amount(
    revenue_base: RevenueBase,
    ota_related_cost_eur: float,
    cleaning_cost_eur: float,
    laundry_cost_eur: float,           # new parameter
    total_booking_scope_cost_eur: float,  # new parameter
) -> float:
    if revenue_base == "total_revenue":
        return 0.0
    if revenue_base == "revenue_minus_ota":
        return ota_related_cost_eur
    if revenue_base == "revenue_minus_ota_minus_cleaning":
        return ota_related_cost_eur + cleaning_cost_eur
    if revenue_base == "revenue_minus_ota_minus_cleaning_minus_laundry":
        return ota_related_cost_eur + cleaning_cost_eur + laundry_cost_eur
    # revenue_minus_all_booking_costs: every scope='booking' CostDefinition's
    # contribution for the period, already the widest netting base possible —
    # by construction >= the other four for the same apartment/period.
    return total_booking_scope_cost_eur
```

Two new resolved figures needed upstream, following the exact pattern `ota_related_cost_eur`/
`cleaning_cost_eur` already establish in `cost_aggregation.py`:

- `laundry_cost_eur` — sum of `EnrichedPaymentLine`s whose `concept == "laundry"`. **Note:**
  `laundry` is not currently a value in `cost_definitions.concept`'s CHECK constraint (Phase 19's
  enum: `electricity, water, gas, internet, pms_subscription, ota_fee, channel_manager,
  office_rent, cleaning, maintenance, insurance, community_fee, other, owner_commission`) — this
  phase adds `'laundry'` to that CHECK, a small additive migration (`ALTER TABLE cost_definitions
  DROP CONSTRAINT ... ADD CONSTRAINT ... CHECK (concept IN (...))`), plus the matching addition to
  `CostConcept` in `price_decision.py`/`payment_line.v1`'s own concept enum wherever it's mirrored.
- `total_booking_scope_cost_eur` — sum of every `EnrichedPaymentLine` (or resolved
  `CostOccurrence`) whose `CostDefinition.scope == "booking"` for the period, regardless of
  concept — a coarser aggregate `cost_aggregation.py` does not currently compute (today it only
  totals by `behavior` for `fixed_cost_eur`/`variable_cost_eur`, not by `scope`). New field on
  `CostAggregate`: `booking_scope_cost_eur: float`.

`decide_price()`'s own `netting_eur`/`p` computation (`stage_price_decision.py`'s
`_build_price_decision()`) already loops over `all_percentage_costs` and calls
`netted_revenue_base_amount()` once per percentage `CostDefinition` — this phase only widens that
one call's arguments, no change to the loop structure itself.

---

## 4. Scope

### In scope
- `services/mock-pm-app/src/mock_pm_app/`: `cost_definitions.sql` — CHECK constraint gains
  `'laundry'`; a new migration adds it to any already-deployed table
  (`ALTER TABLE ... DROP CONSTRAINT ...; ALTER TABLE ... ADD CONSTRAINT ...`, guarded idempotent, same
  pattern every prior CHECK-widening migration in this repo follows); `owner_contracts.sql`/
  `data.py`'s `_COMMISSION_BASES` gains the two new values, plus at least one seeded
  `laundry`-concept `CostDefinition`/`CostAllocationRule` per apartment (mirroring how `cleaning` is
  already seeded) so the new bases have real data to net against.
- `streaming/flink-jobs/src/flink_jobs/cost_aggregation.py`: `LAUNDRY_CONCEPTS` filter (mirroring
  `CLEANING_CONCEPTS`); `booking_scope_cost_eur` aggregate added.
- `streaming/flink-jobs/src/flink_jobs/models.py`: `CostAggregate` gains `laundry_cost_eur: float`,
  `booking_scope_cost_eur: float`.
- `libs/pricing-formulas/src/pricing_formulas/layers/commercial.py`: `RevenueBase` extended;
  `netted_revenue_base_amount()` signature extended (§3).
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py`: `_build_price_decision()`'s
  `netted_revenue_base_amount()` call site passes the two new arguments.
- `libs/shared-schemas/src/shared_schemas/price_decision.py`: `CommissionBase` Literal gains the two
  new values (additive enum widening, no `schema_version` bump — same reasoning Phase 22's
  `ReasonCode` extension documents); `CostConcept` gains `"laundry"`.
- `specs/events/price_decision.v1.json`, `specs/events/payment_line.v1.json`: matching enum
  extensions.
- Full test suite update for `commercial.py` (new base cases) + `cost_aggregation.py` (new
  aggregate fields).

### Out of scope
- A symbolic/solver-based contract engine (§1).
- Per-channel contract bases (§1).

---

## 5. Acceptance criteria

- **AC-01:** `netted_revenue_base_amount("revenue_minus_ota_minus_cleaning_minus_laundry", ...)`
  returns exactly `ota + cleaning + laundry`; `netted_revenue_base_amount("revenue_minus_all_booking_costs",
  ...)` returns `total_booking_scope_cost_eur`, and this value is `>=` every other base's netted
  amount for the same inputs in a property-based test (monotonic widening).
- **AC-02:** a real seeded apartment with a `revenue_minus_all_booking_costs` owner contract shows a
  `netting_eur` in its live decision equal to the hand-summed total of every `scope='booking'`
  `cost_breakdown` entry for that apartment/period.
- **AC-03:** existing seeded apartments on the three original bases show unchanged `netting_eur`
  after this phase (regression).
- **AC-04:** contract fixture tests: `payment_line.v1`/`price_decision.v1` fixtures using
  `concept: laundry` validate; a pre-existing fixture using the old 3-value `CommissionBase` enum
  still validates unchanged (additive widening confirmed non-breaking).

---

## 6. Known limitations

- `laundry` seed volume mirrors `cleaning`'s existing seeding heuristic (a plausible per-turnover
  fixed cost) — not sourced from a real BiLemon cost figure, same caveat every synthetic cost concept
  in this project already carries.
- `revenue_minus_all_booking_costs` nets the *entire* booking-scope cost total, including the
  percentage cost itself if it happens to also be `scope='booking'` (e.g. owner commission). This
  circularity (the commission base includes the commission's own cost) is a documented, accepted
  simplification — no iterative/fixed-point solve is attempted, consistent with §1's decision to
  defer a general solver.

---

## 7. Corrections made during implementation (2026-09-14)

- **`netted_revenue_base_amount()`'s two new parameters got defaults (`0.0`), unlike §3's literal
  signature.** Making them required (as originally written) would have broken every existing call
  site/test that only passes `(revenue_base, ota_related_cost_eur, cleaning_cost_eur)` — the same
  class of mistake Phase 22 caught for `days_to_arrival`'s original `= 0` default (see Phase 22 spec
  §7). Defaulting to `0.0` is safe here specifically because the three original bases never read
  either new parameter at all, so a caller that omits them gets byte-identical behavior to before
  this phase.
- **`decide_price_by_channel()` also needed the two new parameters**, not just the
  `stage_price_decision.py` call site §4 named — it calls `netted_revenue_base_amount()` internally
  for the channel-price matrix's own netting, and without threading `laundry_cost_eur`/
  `booking_scope_cost_eur` through its own signature, every channel candidate would silently net
  against stale (zero) laundry/booking-scope figures regardless of the apartment's real owner
  contract. Both `libs/pricing-formulas/engine.py`'s `decide_price_by_channel()` and its
  `stage_price_decision.py` call site were updated.
- **`specs/events/payment_line.v1.json` needed no changes.** §4 listed a "matching enum extension"
  there, but Phase 19 (ADR-0012) already removed `concept` from `payment_line.v1` entirely — it
  carries `cost_definition_id` instead, resolved via broadcast state. The only event schema with a
  `concept` enum is `price_decision.v1.json`'s `cost_breakdown[].concept`, already updated.
- **No self-healing `ALTER` previously existed for `cost_definitions.revenue_base`'s CHECK
  constraint** — unlike `concept`, which already had one (added when `owner_commission` was
  introduced, ADR-0013). This phase's migration is the *first* self-healing `ALTER` for that
  constraint, not a widening of an existing one; an already-running deployment predating this phase
  needed it to pick up the two new values at all.
- Live-verified against a clean LocalStack stack on 2026-09-14, hand-checked against real seeded
  data (AC-02): `MAD-010`/`BCN-002` on `revenue_minus_all_booking_costs` net exactly their own
  `laundry` cost-breakdown line (the only booking-scope line present for that apartment/period);
  `BCN-001` on the pre-existing `revenue_minus_ota` correctly nets only `ota_fee` (9.27 EUR/night),
  excluding its own `cleaning`/`laundry` lines despite both being present — confirming AC-03
  (regression: the three original bases are unaffected by this phase). Zero Flink exceptions. No
  seeded apartment happened to land on `revenue_minus_ota_minus_cleaning_minus_laundry` in this
  particular random seed run (5 possible bases, 10 apartments) — covered instead by
  `test_netted_revenue_base_amount_laundry_base` and the monotonic-widening property test
  (`test_all_booking_costs_base_is_the_widest_netting_amount`).

# Phase 20 — Rewire pricing formula, Iceberg, dbt, dashboard onto BER/MPR

**Status:** Draft
**Depends on:** Phase 19 (`CostDefinition`/`CostOccurrence`/`CostAllocationRule`, ADR-0012),
Phase 18 (bookings/occupancy)
**Blocks:** nothing further planned
**Related:** ADR-0013 (the decision this phase implements), ADR-0009 (the floor formula this phase
retires), ADR-0011 backlog #13, `/Users/morad/Downloads/dynamic_price_engine.md` §11 (Break-Even y
Profitable Floor), `docs/post-poc-roadmap.md`

---

## 1. Executive summary

Phase 19 built the full `CostDefinition` model but left it disconnected from the actual pricing
formula — `decide_price()` still ran ADR-0009's 3-tier antelación floor against
`fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur`. This phase finishes the job: the floor
formula is replaced by the external spec's own flat, always-the-same Break-Even Revenue (BER) /
Minimum Profitable Revenue (MPR) formulas (§11), every non-percentage cost feeds one consolidated
`fixed_and_allocated_costs_eur` term (no antelación-based exclusion — see ADR-0013), and every
percentage cost (including, newly, owner commission) feeds a single `p` rate. Iceberg, dbt, and the
dashboard are rewired to the new terms.

**Done when:** a real seeded apartment's live decision shows `break_even_revenue_eur`,
`profitable_floor_eur`, and `suggested_price_eur` in the correct order
(`BER <= MPR <= suggested_price` whenever `rule_applied != minimum_profitable_price`, and
`MPR == suggested_price` when it is), computed from real `CostDefinition.rate`-backed percentage
costs (including the apartment's own migrated owner-commission `CostDefinition`) and real
allocated non-percentage costs — verified by hand against the seeded data, live against the running
stack, exactly like every prior phase.

**Not in this phase:** any change to Revenue Management (Phase 13), Market Validation, Channel
gross-up math itself (Phase 16) — only the cost/floor terms those layers receive change, not their
own logic; historical replay of `PriceDecision`s already emitted (ADR-0011/ADR-0013 — future-only,
same as every other broadcast-config change in this repo); a UI for configuring `CostDefinition.rate`
directly (mock-pm-app's seed data covers it, same as every other phase's seed-only approach).

---

## 2. Scope

### In scope

- **`services/mock-pm-app/src/mock_pm_app/`**: `cost_definitions.sql` migration — add `rate NUMERIC(6,4)`,
  `CHECK ((calculation_base IN ('pct_revenue','pct_adjusted_revenue')) = (rate IS NOT NULL))`;
  `owner_contracts.sql` — drop `commission_pct`/`commission_base`, add
  `cost_definition_id UUID NOT NULL` (no FK, same convention as `payment_lines.cost_definition_id`);
  `migrations.py` — backfill: one `CostDefinition`+`CostAllocationRule(method='direct')` per distinct
  `(commission_base, commission_pct)` pair observed across existing `owner_contracts` rows
  (`concept='owner_commission'`, `scope='booking'`, `behavior='variable'`, `trigger='revenue'`,
  `calculation_base='pct_adjusted_revenue'`, `recurrence='per_booking'`,
  `revenue_base=<old commission_base>`, `rate=<old commission_pct>`), reusing the exact
  upsert-by-shape pattern `resolve_cost_definition_ids` already established; every
  `owner_contracts` row's `cost_definition_id` backfilled to match, then the two old columns
  dropped. `data.py`'s `COST_DEFINITION_SPECS`/`ota_fee` entry gains a `rate` (e.g. `0.15`).
- **`streaming/flink-jobs/src/flink_jobs/`**:
  - `models.py` — `CostDefinitionRow` gains `rate: float | None`; `CostAggregate` gains
    `p: float`, `break_even_revenue_eur: float`, `profitable_floor_eur: float`; drops
    `commission_pct`/`commission_base` (now resolved via the owner's commission `CostDefinition`
    like any other broadcast cost, not a dedicated pair of fields).
  - `cost_aggregation.py` — `aggregate_cost()`: percentage lines (`calculation_base IN
    ('pct_revenue','pct_adjusted_revenue')`) are excluded from `fixed_total`/`variable_total` and
    instead summed into a new `p` (sum of `rate`), with the existing OTA/cleaning netting mechanism
    (`ota_related_cost_eur`/`cleaning_cost_eur`) generalized to any percentage
    `CostDefinition.revenue_base`, not hardcoded concepts.
  - `stage_owner_contract_enrichment.py` — resolves the apartment's commission `CostDefinition` via
    `owner_contracts.cost_definition_id` through the same broadcast state `stage_cost_enrichment.py`
    already reads, instead of carrying `commission_pct`/`commission_base` directly.
  - `stage_price_decision.py` — computes `fixed_and_allocated_costs_eur` (= `fixed_cost_eur +
    variable_cost_eur` + the `booking`-allocated lump, already resolved per-night by
    `stage_allocation_correction.py`) and calls the new `decide_price()` signature; no longer passes
    `days_to_arrival` into it (still records it on the event directly, informational only).
- **`libs/pricing-formulas/src/pricing_formulas/`**:
  - `layers/booking_window.py` — **deleted**. `FloorType`/`floor_policy_for()` move into
    `engine.py` directly as the new flat BER/MPR calc (see §3).
  - `layers/commercial.py` — `commission_base_netting_component` generalized to
    `revenue_base_netting_component` (same formula, name reflects it's no longer commission-only);
    still called once per percentage `CostDefinition` with a `revenue_base` (commission included,
    now indistinguishable from any other such cost).
  - `engine.py` — `decide_price()`/`decide_price_los_matrix()`/channel-pricing entry point:
    `fixed_cost_eur`/`variable_cost_eur` collapse into `fixed_and_allocated_costs_per_night_eur`;
    `one_time_cost_eur` renamed `per_booking_cost_eur` (unchanged mechanically — still divided by
    `stay_length`); `commission_pct`/`commission_netting_eur` replaced by `p`/`netting_eur`;
    `days_to_arrival` parameter removed; `PriceCalculation` gains `break_even_revenue_eur`,
    `profitable_floor_eur`; `floor_type` removed, `floor_policy` now derived from
    `target_margin == 0` (ADR-0013 §5); `recommend_minimum_stay`'s `_reservation_cost_eur` updated
    to the same two terms.
  - Full test suite rewrite (signature change), not an extension.
- **`libs/shared-schemas/src/shared_schemas/price_decision.py`** and
  **`specs/events/price_decision.v1.json`** — `schema_version` bump `1.0` → `2.0` (breaking, ADR-0013
  §8): `cost_inputs` loses `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur`'s old
  descriptions (kept as informational `behavior`-grouped figures, see §4), gains
  `fixed_and_allocated_costs_eur`, `per_booking_cost_eur`, `p`; `cost_breakdown` entries gain
  `scope`/`behavior`/`trigger`/`calculation_base`/`recurrence`/`allocation_method` (Phase 19's
  dimensions) alongside `concept`/`amount_eur`; `calculation` loses `floor_type`, gains
  `break_even_revenue_eur`, `profitable_floor_eur`; `commission_pct`/`commission_base` stay (still
  meaningful — the resolved rate/base for this specific decision — just sourced from a
  `CostDefinition` now instead of `owner_contracts` directly).
- **`specs/contracts/fixtures/price_decision/*.json`** — all rewritten to the `2.0` shape; new
  `invalid_legacy_v1_shape.json` regression fixture (same pattern ADR-0012 established for
  `payment_line.v1`).
- **`services/lakehouse-consumer/src/lakehouse_consumer/schema.py`** — new `NestedField`s appended
  from **94** (last used: 93), never reusing/removing `floor_type`(26)/`fixed_cost_eur`(12)/
  `variable_cost_eur`(13)/`one_time_cost_eur`(14)'s existing IDs. New fields: `p` (94),
  `fixed_and_allocated_costs_eur` (95), `per_booking_cost_eur` (96), `break_even_revenue_eur` (97),
  `profitable_floor_eur` (98), plus the 6 new `cost_breakdown` dimension fields (99-104).
- **`services/lakehouse-consumer/src/lakehouse_consumer/transform.py`** — passthrough for every new
  field; a pre-Phase-20 record (no `p`/`fixed_and_allocated_costs_eur` key) falls back to
  `fixed_cost_eur + variable_cost_eur` / `p=0.0` respectively (same "re-derive from what the old
  record actually had" convention Phase 12 established for `floor_policy`), not a fabricated
  constant.
- **`transform/models/staging/stg_price_decision.sql`**, **`transform/models/marts/
  fct_price_decision.sql`** — new passthrough columns.
- **`dashboard/src/dashboard/`**: `hot_path.py`/`marts.py`/`app.py` — Apartment Detail's cost table
  and the Cost breakdown tab rewritten (not extended): cost table shows
  `fixed_and_allocated_costs_eur`/`p`/`break_even_revenue_eur`/`profitable_floor_eur`/
  `suggested_price_eur` as an explicit cascade (external spec §14, Explainability), still keeping
  the existing Fixed/Variable informational split (behavior-grouped) alongside it; Cost breakdown
  tab's table gains scope/behavior/allocation_method columns and groups rows by `scope`
  (booking/property/company) so a company-wide cost's contribution is visually distinct.

### Out of scope

- Revenue Management engine internals (Phase 13), Market Validation (structural/performance/
  inventory layers), Channel gross-up math (Phase 16) — they keep consuming
  `property_reference_price_eur`/`market_reference_price_eur` unchanged; only the floor terms
  feeding into them change.
- A generic UI for authoring `CostDefinition.rate`/`weight_config` — still seed-data only.
- Any change to Manual Override (Phase 14) or Minimum Stay Recommendation's own selection logic
  (Phase 15) beyond updating the two cost terms `_reservation_cost_eur` needs.

---

## 3. Formula (ADR-0013)

Per apartment/decision, given already-allocated non-percentage costs and the summed percentage
rate:

```
fixed_and_allocated_costs_eur = fixed_and_allocated_costs_per_night_eur
                                 + per_booking_cost_eur / stay_length

BER  (Break-Even Revenue)        = fixed_and_allocated_costs_eur / (1 - p)
MPR  (Minimum Profitable Revenue) = fixed_and_allocated_costs_eur / (1 - p - target_margin)
```

Both are per-night ADR figures here (this project always works in per-night terms — see ADR-0009's
existing convention, preserved). `minimum_price_eur` (the floor guardrails actually enforce) is
always `MPR`. `BER` is carried alongside purely for explainability (external spec §14) — it never
substitutes for `MPR` as the enforced floor.

`p` = sum of every applicable `CostDefinition.rate` (owner commission's now included, having been
migrated under `CostDefinition` — ADR-0013 §4) whose `revenue_base` is `total_revenue` (or absent) —
adjusted for any `revenue_base` that excludes concepts (`revenue_minus_ota`,
`revenue_minus_ota_minus_cleaning`) via the generalized `revenue_base_netting_component`, exactly as
today's commission netting already works, just per-`CostDefinition` instead of commission-only.

`floor_policy` = `"hard"` iff `target_margin == 0` (in which case `BER == MPR`), else `"soft"`
(ADR-0013 §5) — `floor_type`, and the whole antelación-tiered selection, is gone.

`days_to_arrival` is no longer read by `decide_price()`'s floor math (there is nothing left that
varies by it) — Flink still records it directly on the event for audit/explainability, just
without threading it through the formula.

---

## 4. Data model

### `CostAggregate` (Flink) — key fields after this phase

```
fixed_cost_eur: float          # informational only now — behavior='fixed' lines, per-night
variable_cost_eur: float       # informational only now — behavior='variable'/'semi_variable', per-night
per_booking_cost_eur: float    # renamed from one_time_cost_eur — allocation_method='booking' lump, per-turnover average
p: float                       # sum of applicable CostDefinition.rate (incl. migrated commission)
# fixed_and_allocated_costs_per_night_eur is NOT stored — computed at decide_price() call time as
# fixed_cost_eur + variable_cost_eur, to avoid a second source of truth for the same two numbers.
```

### `PriceCalculation` (pricing-formulas) — key fields after this phase

```
break_even_revenue_eur: float
profitable_floor_eur: float     # == minimum_price_eur going forward; kept as its own name for clarity in callers
minimum_price_eur: float        # unchanged name/role — always equals profitable_floor_eur now
floor_policy: FloorPolicy       # "hard" | "soft", derived from target_margin == 0
# floor_type: REMOVED
```

### `cost_definitions` (mock-pm-app/migrations.py) — new column

```sql
ALTER TABLE public.cost_definitions
    ADD COLUMN IF NOT EXISTS rate NUMERIC(6,4)
        CHECK (rate IS NULL OR (rate >= 0 AND rate <= 1));
-- Guarded, not a CHECK co-constraint with calculation_base directly (Postgres CHECKs referencing
-- two columns are fine, but this keeps the migration idempotent-safe if run against partially
-- migrated rows — the app-level resolve step enforces the pairing before insert).
```

### `owner_contracts` — breaking change

```sql
ALTER TABLE public.owner_contracts DROP COLUMN commission_pct;
ALTER TABLE public.owner_contracts DROP COLUMN commission_base;
ALTER TABLE public.owner_contracts ADD COLUMN cost_definition_id UUID NOT NULL;
```

Backfill (one-time, guarded by `information_schema.columns`, same convention as Phase 19's own
`payment_lines` backfill): for each distinct `(commission_base, commission_pct)` pair observed
before the drop, `resolve_cost_definition_ids`-style upsert one `CostDefinition`
(`concept='owner_commission'`) + one `CostAllocationRule(method='direct')`; every `owner_contracts`
row's `cost_definition_id` set to match its own prior `(commission_base, commission_pct)`.

---

## 5. Acceptance criteria

- **AC-01**: `decide_price()` (and the LOS matrix / channel-pricing entry points) no longer accept
  `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur`/`commission_pct`/`commission_netting_eur`/
  `days_to_arrival`; they accept `fixed_and_allocated_costs_per_night_eur`/`per_booking_cost_eur`/`p`/
  `netting_eur`, and return `break_even_revenue_eur`/`profitable_floor_eur` alongside the existing
  fields (minus `floor_type`).
- **AC-02**: for `target_margin=0`, `break_even_revenue_eur == profitable_floor_eur == minimum_price_eur`
  and `floor_policy == "hard"`; for `target_margin>0` they differ and `floor_policy == "soft"`, in
  both the unit tests and a live decision.
- **AC-03**: a real seeded owner-commission `CostDefinition` (migrated from a real
  `owner_contracts` row) contributes its `rate` to `p` and its netting (if `revenue_base` excludes
  OTA/cleaning) reduces the numerator identically to how `commission_netting_eur` did before this
  phase — same numeric floor for an unchanged apartment, verified live.
- **AC-04**: a real seeded `ota_fee` `CostDefinition` (`calculation_base=pct_adjusted_revenue`,
  `rate` seeded) contributes to `p` and is excluded from `fixed_and_allocated_costs_eur` — confirmed
  by comparing a decision's `fixed_and_allocated_costs_eur` against a hand-summed total of only the
  non-percentage `cost_breakdown` entries for that apartment/period.
- **AC-05**: Iceberg schema migration is additive-only (new field IDs 94-104, nothing renumbered or
  removed); a pre-Phase-20 row still reads back via `transform.py`'s fallback derivation without
  raising.
- **AC-06**: dashboard's Apartment Detail and Cost breakdown tabs render without error against a
  real running stack, showing the BER → MPR → Suggested cascade and scope-grouped cost rows.

---

## 6. Test strategy

- `libs/pricing-formulas`: full suite rewrite (signature change touches every existing test) —
  `engine.py`/`layers/commercial.py`/`layers/guardrails.py`; explicit tests for AC-02's hard/soft
  boundary and for `recommend_minimum_stay`'s updated `_reservation_cost_eur`.
- `streaming/flink-jobs`: `cost_aggregation.py` (percentage-line exclusion from fixed/variable,
  `p` summation, generalized netting), `stage_owner_contract_enrichment.py` (resolves commission via
  `CostDefinition` broadcast), `stage_price_decision.py` (new `decide_price()` call shape).
- `specs/contracts`: `price_decision.v1` `2.0` fixtures + `invalid_legacy_v1_shape.json` regression
  test (rejects a `1.0`-shaped message, same as `payment_line.v1`'s own regression test).
- `services/mock-pm-app`: migration test for the `owner_contracts` backfill (dedup-by-shape,
  no orphaned rows, old columns actually dropped after backfill).
- `services/lakehouse-consumer`: schema migration + transform fallback-derivation tests (a record
  missing the new keys still parses).
- Live LocalStack verification (required, matching every phase so far): seed a real
  `owner_contracts` row and a real `ota_fee` line, confirm `p`/`fixed_and_allocated_costs_eur`/
  `break_even_revenue_eur`/`profitable_floor_eur` in a real DynamoDB `price_decision` item match a
  hand computation from the seeded data; confirm the dashboard renders both rewritten tabs against
  the live stack.

---

## 7. Known limitations

- No historical replay: existing Iceberg rows keep their old `floor_type`/`fixed_cost_eur`/
  `variable_cost_eur`/`one_time_cost_eur` values as they were computed at the time; nothing is
  recalculated retroactively (ADR-0011/ADR-0013).
- `p + target_margin >= 1` is not specially guarded against (same pre-existing risk
  `commission_pct` already carried before this phase — a configuration error, not a new one this
  phase introduces); the floor formula will simply blow up (division by a non-positive number),
  same as today.
- `calculation_base='per_guest'` costs rely on Phase 20's own new `avg_guests` aggregation
  (extending `occupancy.py`/`stage_booking_enrichment.py`, analogous to `occupied_nights`) — first
  real use of that `calculation_base` value; no seeded per-guest cost exists yet beyond what this
  phase adds for verification.
- `event`-triggered costs (manually tagged `CostOccurrence`s, Phase 19 §2) still have no automatic
  detection system — unchanged limitation, not addressed by this phase.

---

## 8. Follow-ups

- A UI for authoring/editing `CostDefinition.rate` and `weight_config` directly (today: seed-data
  only, same as every other phase).
- Revisit whether `p + target_margin` should be capped/guarded now that `p` can aggregate multiple
  independent percentage costs (previously only `commission_pct` alone).

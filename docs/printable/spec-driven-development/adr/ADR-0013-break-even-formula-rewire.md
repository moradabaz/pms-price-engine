# ADR-0013 — Rewire the pricing formula onto Break-Even/Profitable Floor (BER/MPR)

**Status:** Accepted
**Related:** ADR-0009 (the antelación-tiered floor this ADR retires), ADR-0011 (target architecture),
ADR-0012 (`CostDefinition`/`CostOccurrence`/`CostAllocationRule`, the data model this ADR finally
wires into the pricing formula), `specs/phases/20-cost-model-rewire/spec.md`,
`/Users/morad/Downloads/dynamic_price_engine.md` §11 (Break-Even y Profitable Floor)

## Context

Phase 19 (ADR-0012) built the full `CostDefinition`/`CostOccurrence`/`CostAllocationRule` model and
wired it into Flink's cost aggregation, but `libs/pricing-formulas` was explicitly left untouched —
`decide_price()` still consumes the pre-existing `fixed_cost_eur`/`variable_cost_eur`/
`one_time_cost_eur` split, and the floor formula is still ADR-0009's own invention: three tiers
selected by `days_to_arrival` (`structural_full_margin` > 30d, `structural_reduced_margin` 15-30d,
`contribution` < 15d), the last of which excludes `fixed_cost_eur` entirely on the theory that it's
already sunk.

The external target spec (§11) does not describe antelación-tiered floors at all. It defines two
flat formulas, evaluated identically regardless of days to arrival:

```
Break-Even Revenue:        BER = Fixed-and-allocated Costs / (1 - p)
Minimum Profitable Revenue: MPR = Fixed-and-allocated Costs / (1 - p - m)
Minimum Profitable ADR:     MPA = MPR / LOS
```

`p` is the summed rate of every percentage-based cost sharing the same revenue base as the
candidate price; `m` is the target operating margin.

The user has directed that, wherever this project's own prior inventions (ADR-0009's antelación
tiers, the fixed/variable exclusion at short notice) diverge from the client's own document, the
client's document wins. This ADR records that decision and its concrete consequences.

## Decision

1. **The 3-tier antelación floor (ADR-0009) is retired.** `booking_window_floor()`,
   `FloorType` (`structural_full_margin`/`structural_reduced_margin`/`contribution`), and the
   `days_to_arrival`-driven branch are removed from `libs/pricing-formulas`. There is one floor
   formula, always evaluated the same way regardless of days to arrival.
2. **No cost is ever excluded from the floor.** Every non-percentage `CostDefinition` (any
   `calculation_base` other than `pct_revenue`/`pct_adjusted_revenue`), once allocated to a
   per-night or per-booking figure by its `CostAllocationRule`, contributes to
   `fixed_and_allocated_costs_eur` unconditionally — there is no "sunk, exclude at short notice"
   mechanism of any kind. This project's ADR-0009 `contribution` floor behavior does not survive
   this rewrite.
3. **`p` is a real rate, not an inferred one.** `cost_definitions` gains a `rate NUMERIC` column,
   required (and validated) exactly when `calculation_base IN ('pct_revenue',
   'pct_adjusted_revenue')`, null otherwise — configuration, the same way `owner_contracts.
   commission_pct` always was, not something computed by dividing a historical invoiced amount by
   a historical revenue figure. `p` = the sum of every applicable `CostDefinition.rate` for the
   apartment/period in question.
4. **Owner commission is unified under `CostDefinition`.** `owner_contracts.commission_pct`/
   `commission_base` are physically removed (same "real substitution, not a redundant copy"
   precedent ADR-0012 established for `payment_lines`). `owner_contracts` instead carries a
   `cost_definition_id` referencing a `scope='booking'`, `calculation_base='pct_adjusted_revenue'`
   `CostDefinition` (`rate` = the old `commission_pct`, `revenue_base` = the old `commission_base`),
   resolved via the exact upsert-by-shape pattern Phase 19's `resolve_cost_definition_ids`
   established (dedupes identical `(revenue_base, rate)` pairs across apartments into one shared
   row, same as every other concept). The existing per-`revenue_base` netting mechanism
   (`commission_base_netting_component`) generalizes to any percentage `CostDefinition` with a
   `revenue_base`, not just commission.
5. **Hard vs Soft floor is redefined.** `floor_policy` no longer derives from which antelación tier
   fired (there are none). `minimum_price_eur` (the floor actually enforced by guardrails) is
   always **MPR** — BER is exposed purely as an informational, explainability value (§14 of the
   external spec), never itself substituted in as the enforced floor. `floor_policy` becomes
   `"hard"` iff `target_margin == 0` (BER and MPR coincide) and `"soft"` otherwise — still a
   pure, one-line derivation, just derived from margin instead of `floor_type`.
6. **Per-booking costs keep their stay-length amortization.** `CostAllocationRule.method='booking'`
   costs (Phase 19's `one_time_cost_eur`-equivalent slot) are still divided by the *candidate*
   `stay_length` inside `decide_price()`, not inside Flink — Flink only knows a period average, not
   the specific LOS a given price candidate is being evaluated for. This is unchanged from today.
7. **Iceberg field IDs are never reused or removed.** `fixed_cost_eur`/`variable_cost_eur`/
   `one_time_cost_eur`/`floor_type`'s NestedFields stay in the table schema forever (old rows keep
   them); new code stops writing meaningful values into `floor_type` (removed at the event-contract
   level) and adds new fields for the new terms.
8. **`price_decision.v1` takes a second breaking bump, to `2.0`.** Same class of change as
   `payment_line.v1`'s `1.0` → `2.0` in ADR-0012: `floor_type` is removed (no tiers exist to name);
   `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur` are kept (informational grouping by
   `CostDefinition.behavior` only, see spec 20 §4) but no longer feed the formula directly; new
   required fields carry `fixed_and_allocated_costs_eur`, `p`, `break_even_revenue_eur`,
   `profitable_floor_eur`.

## Consequences

- This is a genuine behavior change for real decisions close to arrival: today's `contribution`
  floor (fixed cost excluded, no margin) no longer exists — every decision now targets a margin-
  inclusive MPR floor regardless of `days_to_arrival`. Antelación-based demand/margin tactics move
  entirely to the Revenue Management layer (Phase 13), not the floor.
- `days_to_arrival` is no longer an argument `decide_price()`'s floor math needs; Flink keeps
  recording it on the event directly (informational), but stops threading it through
  `decide_price()`.
- Every call site of `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur` across
  `libs/pricing-formulas` (`engine.py`, `layers/booking_window.py` — removed —,
  `recommend_minimum_stay`'s `_reservation_cost_eur`) is rewritten, not extended; the whole test
  suite for the package is rewritten to match, per this project's established convention for
  signature-changing phases.
- `owner_contracts` is no longer self-contained — commission is a real join through
  `cost_definitions`, exactly like every other cost concept. This is more model-consistent, but
  means "what does this apartment pay in commission" now requires one extra join, same trade-off
  ADR-0012 already accepted for `payment_lines`.

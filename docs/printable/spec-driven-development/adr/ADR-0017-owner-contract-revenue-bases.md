# ADR-0017 — Extend `RevenueBase` to all five owner-contract models from the external spec, reject a general solver

**Status:** Proposed
**Related:** ADR-0011 backlog #5, ADR-0013 §3 (the netting mechanism this ADR extends),
`specs/phases/24-owner-contract-revenue-bases/spec.md`, `/Users/morad/Downloads/dynamic_price_engine.md`
§11.1, test scenarios T04/T05

## Context

The external spec (§11.1) lists five owner-contract revenue bases (Total Reservation; −OTA;
−OTA−Cleaning; −OTA−Cleaning−Laundry; −all Booking Costs) and explicitly raises, as an open
implementation question (§29), whether resolving them needs "algebra predefined per model or a
generic linear solver." This project's `RevenueBase`/`netted_revenue_base_amount()`
(`layers/commercial.py`) currently implements exactly three of the five, each as one `if`/`elif`
branch — the same pattern that would need to either grow by two more branches, or be replaced by
something more general, to close the gap.

## Decision

1. **Extend the closed `Literal`/branch pattern to all five bases**, rather than building a symbolic
   or solver-based engine. All five bases in the client's own document are linear subtractions from
   Total Revenue with no interdependent unknowns — a closed enum with one netting formula per value
   remains fully enumerable, fully testable, and consistent with how every other closed
   classification in this project (`CostConcept`, `CostScope`, `CostBehavior`, etc.) is modeled. A
   general solver is deferred as an open question, exactly as the client's own document frames it
   (§29 — "decisions to validate," not a committed requirement) — revisit only if a genuinely
   non-linear or user-defined base is ever needed.
2. **`"revenue_minus_all_booking_costs"` nets the whole `scope='booking'` cost total**, including the
   percentage cost's own contribution if it happens to also be booking-scoped (e.g. owner
   commission). This is a documented circularity, not resolved by an iterative/fixed-point
   computation — accepted as a known simplification (same class of accepted approximation this
   project's `p + target_margin >= 1` un-guarded edge case already carries, per ADR-0013's own
   Known Limitations).
3. **`laundry` becomes a new `CostDefinition.concept` value.** It did not exist in Phase 19's
   original 13-value concept enum — a small, additive CHECK-constraint widening plus matching
   `CostConcept` enum additions in `shared_schemas`/event contracts, following the exact same pattern
   Phase 20 used to add `owner_commission` to that same enum.
4. **The `CommissionBase` enum widening in `price_decision.v1` is additive, not a breaking bump** —
   consistent with how ADR-0015 (`ReasonCode`) reasons about enum growth: existing consumers reading
   older decisions are unaffected; only new decisions can carry the two new values.

## Consequences

- `cost_aggregation.py` gains a new aggregate, `booking_scope_cost_eur` (sum by `scope`, not just by
  `behavior` as today) — the first place in this project that aggregates costs along the `scope`
  dimension rather than `behavior`, opening a natural path for `scope='property'`/`scope='company'`
  equivalents later if ever needed (not built now — YAGNI, no current consumer).
- Every apartment needs at least one seeded `laundry`-concept cost line for the two new bases to have
  real data to net against in live verification — `mock-pm-app`'s seed data grows by one more
  concept, mirroring how `cleaning` is already seeded.
- The five-base enum is now believed complete against the client's own document; any sixth base
  would need this ADR's decision (§1) revisited, not silently extended by analogy.

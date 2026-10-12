# ADR-0015 — Add a Booking Window Revenue Management layer, reintroducing `days_to_arrival` as an RM-only signal

**Status:** Proposed
**Related:** ADR-0013 (removed `days_to_arrival` from the floor formula — this ADR does not reverse
that decision), ADR-0011 (target architecture, RM layers A-G), `specs/phases/22-booking-window-layer/spec.md`,
`/Users/morad/Downloads/dynamic_price_engine.md` §14.1 row D

## Context

ADR-0013 retired the antelación-tiered floor formula and, as a direct consequence, removed
`days_to_arrival` as a parameter `decide_price()` reads — correctly, since nothing in the *floor*
varies by it anymore. But ADR-0013's own Consequences section named where that logic should
actually go: *"Antelación-based demand/margin tactics move entirely to the Revenue Management layer
(Phase 13), not the floor."* No RM layer for booking window was ever built. The external spec's
§14.1 lists it as layer D (Booking Window: early bird, standard window, last minute, same day) —
this project has the other six layers (Structural, Market, Performance, Inventory, Commercial,
Guardrails) but not this one.

## Decision

1. **A new `layers/booking_window.py` module**, structurally identical to every other layer file:
   pure functions, configurable constants (`BOOKING_WINDOW_TIERS`), a `DecisionComponent`-emitting
   explainability function that returns `None` when the tier's own adjustment is zero (the same
   "don't emit a component that explains nothing" convention `commercial.py`'s netting component
   already follows).
2. **`days_to_arrival` re-enters `decide_price()`'s signature — as a new, default-`0` keyword
   argument feeding only the new layer, not the floor formula.** This is deliberately *not* a
   reversal of ADR-0013: the floor math (`break_even_revenue_eur`/`profitable_floor_eur`) does not
   read this argument at all; only `booking_window_factor()`, applied to
   `property_reference_price_eur` alongside the existing `performance_layer()`/`inventory_layer()`
   multiplication, does.
3. **Placement in the pipeline:** immediately after Performance/Inventory, before Market. This
   project's existing layer order (Structural → Performance → Inventory → Market → Floor →
   Guardrails) is already a specific sequencing choice that doesn't literally follow the external
   spec's own A-G lettering; Booking Window is inserted where it adjusts the property's own
   reference price, consistent with how Performance/Inventory already behave, rather than forcing a
   full pipeline reorder to match the external document's letter order exactly. This is a judgment
   call, recorded here rather than left implicit.
4. **Concrete tier thresholds are placeholder, configurable values** (§3 of the phase spec) — not
   sourced from a real BiLemon policy, same caveat every other constant table in this codebase
   (`CHANNEL_COMMISSION_PCT`, `LOS_CANDIDATES`, `QUALITY_TIER_ADJUSTMENTS`) already carries openly.

## Consequences

- `decide_price_los_matrix()`/`decide_price_by_channel()` also gain the new parameter (threaded
  through to their shared `decide_price()` call) — a small, additive signature widening with a
  default value, not a breaking change to either function's existing callers.
- `ReasonCode` (both `pricing_formulas.decision_components` and `shared_schemas.price_decision`)
  gains three new values — an additive, non-breaking enum widening (no `schema_version` bump; a
  reader validating against the old, narrower enum would reject a new decision carrying a new code,
  but nothing in this project re-validates already-emitted historical data against a newer schema,
  so this is accepted as consistent with how every other phase has treated enum growth).
- A real decision's `suggested_price_eur` can now differ purely based on `days_to_arrival` even when
  every cost/market input is unchanged — a new, intentional source of variation the dashboard should
  surface via the new reason codes, not hide.

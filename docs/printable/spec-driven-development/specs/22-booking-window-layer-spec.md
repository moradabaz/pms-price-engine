# Phase 22 — Booking Window Revenue Management layer

**Status:** Implemented (2026-09-14) — see §7 for corrections made during implementation
**Depends on:** Phase 20 (BER/MPR rewire, ADR-0013) — the layer this phase adds sits in the RM
pipeline Phase 13 built and Phase 20 last touched; no dependency on Phase 21 (no entity from that
phase is needed here — `days_to_arrival` is already computed inline in `stage_price_decision.py`)
**Blocks:** nothing further planned
**Related:** ADR-0015 (the decision this phase implements), ADR-0013 (explicitly named this gap in
its own Consequences section — see below), `/Users/morad/Downloads/dynamic_price_engine.md`
§14.1 row D (Booking Window), `docs/post-poc-roadmap.md`

---

## 1. Executive summary

The external spec's Revenue Management engine has 7 layers (§14.1, A–G). This project implements 6:
Structural (`layers/structural.py`), Market (`layers/market.py`), Performance
(`layers/performance.py`, stub), Inventory (`layers/inventory.py`, stub), Commercial
(`layers/commercial.py`), Guardrails (`layers/guardrails.py`). Layer D, **Booking Window** (early
bird / standard / last minute / same day), has no implementation at all —
`grep -r booking_window libs/pricing-formulas` finds only the file ADR-0013 deleted
(`layers/booking_window.py`, which held the *old*, unrelated antelación-tiered floor logic, not this
layer). ADR-0013's own Consequences section names this exact gap: *"Antelación-based demand/margin
tactics move entirely to the Revenue Management layer (Phase 13), not the floor"* — this phase is
that move.

**Done when:** a new `layers/booking_window.py` module produces a configurable multiplicative
factor from `days_to_arrival`, wired into `decide_price()`'s existing layer pipeline between Market
and Inventory (matching the external spec's A→B→C→D→E→F→G lettering as closely as this project's
existing layer order allows — see §3); a live decision for the same apartment/night computed at two
different `days_to_arrival` values (e.g. 60 days out vs. 2 days out) shows a different
`property_reference_price_eur`/`suggested_price_eur` purely from the booking-window factor, with a
`RULE_EARLY_BIRD`/`RULE_LAST_MINUTE`/`RULE_SAME_DAY` `DecisionComponent` explaining it — verified
live against LocalStack.

**Not in this phase:**
- Any change to the floor formula (`BER`/`MPR`) — ADR-0013 §1/§2 stand: the floor never varies by
  `days_to_arrival` again. This phase reintroduces `days_to_arrival` only as an RM-layer signal, not
  as a floor input (§2 explains the exact mechanics).
- Priority/compatibility/cap orchestration across multiple RM rules firing at once (external spec
  §14.2's full rule-conflict framework) — this project's RM pipeline remains a fixed, ordered
  composition of layers (§C of `docs/phase-13-layered-rm-engine-design-decisions.md`), not a
  general rule engine. One layer, one factor, same discipline every existing layer already follows.
- Lead-time-aware inventory optimization (orphan gaps, min-stay auto-adjustment triggered by booking
  window) — Phase 15's `recommend_minimum_stay()` stays a separate, LOS-driven mechanism; Phase 23
  is where booking-window and floor-conflict logic are allowed to interact (§13 of the external
  spec), not this phase.

---

## 2. Design fork: does `days_to_arrival` re-enter `decide_price()`'s signature?

ADR-0013 removed `days_to_arrival` as a `decide_price()` parameter specifically because nothing in
the floor formula varied by it anymore (§5 of that ADR). This phase needs it again, but only for an
RM-layer factor, not the floor. **Decision:** `decide_price()`/`decide_price_los_matrix()`/
`decide_price_by_channel()` each gain a `days_to_arrival: int = 0` keyword parameter, threaded only
into the new `booking_window_layer()` call — the floor math (`break_even_revenue_eur`/
`profitable_floor_eur`) does not read it, preserving ADR-0013's decision unchanged. `stage_price_decision.py`
already computes `days_to_arrival` inline (kept for audit purposes even after ADR-0013) — this phase
just also passes that existing local variable into the three call sites, one line each.

---

## 3. `layers/booking_window.py` (new)

Follows the exact structure of `layers/performance.py`/`layers/structural.py` — configurable
constants, not hardcoded branches (client spec §3, "Configurable, no hard-coded"):

```python
# Tiers as (min_days_inclusive, adjustment) pairs, ordered furthest-out first.
# Positive = markup, negative = markdown. Configurable, not embedded
# irreversibly — same discipline QUALITY_TIER_ADJUSTMENTS (structural.py)
# already established.
BOOKING_WINDOW_TIERS: tuple[tuple[int, float, str], ...] = (
    (45, 0.00, "standard_window"),   # >= 45 days: no adjustment (baseline)
    (15, -0.03, "early_bird"),       # 15-44 days: small markdown, reward early commitment
    (3, 0.00, "standard_window"),    # 3-14 days: no adjustment
    (1, -0.05, "last_minute"),       # 1-2 days: markdown to move unsold inventory
    (0, -0.08, "same_day"),          # 0 days: steepest markdown
)


def booking_window_factor(days_to_arrival: int) -> float:
    """Returns the multiplicative factor for this booking window tier.
    Clamped at 0 days minimum (a negative days_to_arrival — a decision built
    for a past date — is not expected to reach here; Stage B already skips
    market updates for past target_dates)."""
    days = max(days_to_arrival, 0)
    for threshold, adjustment, _label in BOOKING_WINDOW_TIERS:
        if days >= threshold:
            return round(1 + adjustment, 4)
    return 1.0  # unreachable given the 0-days final tier, kept for safety


def booking_window_component(days_to_arrival: int) -> DecisionComponent | None:
    """Explains the booking-window adjustment. None when the tier's own
    adjustment is 0.0 (standard_window) — same 'don't emit a component that
    explains nothing' rule commercial.py's netting component already
    follows."""
    days = max(days_to_arrival, 0)
    for threshold, adjustment, label in BOOKING_WINDOW_TIERS:
        if days >= threshold:
            if adjustment == 0.0:
                return None
            return DecisionComponent(
                code=f"rule_{label}",
                label=f"Booking window: {days} days to arrival ({label}, {adjustment:+.0%})",
                impact=adjustment,
            )
    return None
```

New `ReasonCode` entries (`decision_components.py` and `libs/shared-schemas/src/shared_schemas/price_decision.py`'s
own `ReasonCode` Literal, kept in sync as every prior phase requires): `"rule_early_bird"`,
`"rule_last_minute"`, `"rule_same_day"` (`"rule_standard_window"` is never emitted — see
`booking_window_component`'s `None` case — so it is *not* added to the closed vocabulary, avoiding a
reason code that can never appear in real data).

### Wiring into `engine.py`

```python
property_reference_price_eur *= (
    performance_layer() * inventory_layer() * booking_window_factor(days_to_arrival)
)
```

placed on the same line `performance_layer()`/`inventory_layer()` already multiply — this project's
existing layer order is Structural → Performance → Inventory → Market → Floor → Guardrails (not a
strict A–G reading of the external spec's own lettering, a pre-existing deviation from Phase 13 this
phase does not attempt to fix). Booking Window (external spec's layer D) is inserted immediately
after Performance/Inventory and before Market, on the grounds that it — like Performance/Inventory —
adjusts the *property's own* reference price ahead of the market-competitiveness discount, not
after it; this ordering choice is a judgment call, documented here rather than silently implied.
`booking_window_component()`'s result (if not `None`) is appended to `decision_components` in
`decide_price()`, at the same point `property_decision_components`/`commission_decision_components`
are spliced in.

---

## 4. Scope

### In scope
- `libs/pricing-formulas/src/pricing_formulas/layers/booking_window.py` (new).
- `libs/pricing-formulas/src/pricing_formulas/decision_components.py`: `ReasonCode` gains the three
  new values.
- `libs/pricing-formulas/src/pricing_formulas/engine.py`: `decide_price()`/
  `decide_price_los_matrix()`/`decide_price_by_channel()` gain `days_to_arrival: int = 0`; wiring per
  §3.
- `libs/shared-schemas/src/shared_schemas/price_decision.py`: `ReasonCode` Literal extended
  (additive — a JSON Schema enum widening, no `schema_version` bump; existing consumers reading
  older values are unaffected, only new decisions can carry the new codes).
- `specs/events/price_decision.v1.json`: matching `enum` extension for `reason_code`/equivalent.
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py`: `_build_price_decision()`'s
  already-computed `days_to_arrival` local variable is also passed into the three `decide_price*`
  calls.
- New unit tests: `booking_window_factor()`/`booking_window_component()` boundary tests at each
  tier edge (44/45, 14/15, 2/3, 0/1 days); an `engine.py` test confirming the same apartment/cost
  inputs produce a lower `suggested_price_eur` at `days_to_arrival=1` than at `days_to_arrival=60`
  when `rule_applied == "market_competitive"` for both (the case where the factor is actually
  visible in the output, not masked by the floor).

### Out of scope
- Floor formula changes (§1).
- Rule priority/compatibility/cap framework (§1).
- Any UI for editing `BOOKING_WINDOW_TIERS` — seed/code-config only, same as every other layer's
  constants in this project.

---

## 5. Acceptance criteria

- **AC-01:** `booking_window_factor(0)`, `(2)`, `(10)`, `(30)`, `(60)` return `0.92`, `0.92`,
  `0.95`, `1.0`, `1.0` respectively (matching §3's tier table exactly).
- **AC-02:** for a candidate where `rule_applied == "market_competitive"` at both extremes,
  `suggested_price_eur` at `days_to_arrival=1` is strictly lower than at `days_to_arrival=60` for
  identical cost/market inputs otherwise.
- **AC-03:** `decision_components` includes a `rule_last_minute`/`rule_same_day`/`rule_early_bird`
  entry exactly when the corresponding tier's adjustment is non-zero, and no entry at all for the
  two `standard_window` tiers.
- **AC-04:** the floor terms (`break_even_revenue_eur`, `profitable_floor_eur`) are numerically
  identical regardless of `days_to_arrival` for otherwise-identical inputs — confirms ADR-0013's
  floor-formula decision is untouched by this phase.
- **AC-05:** live LocalStack verification: reprice a real seeded apartment/night, observe the
  booking-window component appear/disappear as `target_date` moves across a tier boundary over
  successive days.

---

## 6. Known limitations

- Tier thresholds/adjustments (§3) are placeholder values calibrated for plausibility, not sourced
  from a real BiLemon pricing policy — same caveat `CHANNEL_COMMISSION_PCT`/`LOS_CANDIDATES` already
  carry elsewhere in this codebase. Revisit once real strategy input exists (Phase 25 or later).
  the client's own document does not specify concrete tier day-boundaries either — §3's own
  categories (early bird / standard / last minute / same day) are qualitative there.
- No interaction with the Inventory layer's "orphan gap" concept (still a stub, §1) — a same-day gap
  next to an already-booked night is not specially treated; that remains Inventory's job once it
  stops being a stub.

---

## 7. Corrections made during implementation (2026-09-14)

- **§5/AC-01 arithmetic error, corrected at code time.** The spec's own AC-01 claimed
  `booking_window_factor(0/2/10/30/60)` returns `0.92/0.92/0.95/1.0/1.0` — inconsistent with §3's own
  `BOOKING_WINDOW_TIERS` table (which the code follows verbatim). Recomputing from that table:
  `factor(0)=0.92` (same_day), `factor(2)=0.95` (last_minute, 1-2 days), `factor(10)=1.0`
  (standard_window, 3-14 days), `factor(30)=0.97` (early_bird, 15-44 days), `factor(60)=1.0`
  (standard_window, >=45 days). Implemented and tested against these corrected values, not the
  original AC-01 prose.
- **`days_to_arrival`'s default could not stay `0`.** §2 originally specified
  `days_to_arrival: int = 0` as the new keyword default on all three `decide_price*` functions. Since
  0 days lands in the steepest `same_day` tier (not a neutral one), this silently discounted every
  pre-existing caller that omits the argument — it broke 11 of the package's existing tests on first
  run. Fixed by introducing `NO_BOOKING_WINDOW_SIGNAL = 999` (lands in the neutral `standard_window`
  tier, factor `1.0`, no component) as the actual default; the one real production caller
  (`stage_price_decision.py`) always passes its own computed value explicitly regardless.
- Live-verified against a clean LocalStack stack on 2026-09-14: a real seeded apartment
  (`BCN-004`/2026-09-16, `days_to_arrival=2`) showed `rule_last_minute` in `decision_components`
  (top-level and every `los_floor_matrix` candidate), absent at `days_to_arrival=3`/`4` for the same
  apartment on adjacent nights — confirming AC-03 live. `break_even_revenue_eur`/`profitable_floor_eur`
  were numerically identical across all three nights despite the differing `days_to_arrival`,
  confirming AC-04 live. No Flink job exceptions. Full `pytest` suite (65/65 pricing-formulas incl.
  new `test_booking_window.py` and 3 new `test_engine.py` cases) and `mypy`/`ruff` clean.

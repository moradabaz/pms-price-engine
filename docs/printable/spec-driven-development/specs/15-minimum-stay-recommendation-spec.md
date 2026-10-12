# Phase 15 — Minimum Stay Recommendation (profitability lever)

**Status:** Implemented — all acceptance criteria (§5) verified, including AC-09 live against a running LocalStack stack on 2026-09-07: zero Flink exceptions with the new logic active, real `cost_protected` apartments producing correct `minimum_stay_recommendation` values in DynamoDB (both the "already fine at 1 night" and "recommendation found at n>1" branches, cross-checked against `recommend_minimum_stay()` directly), the field reaching `fct_price_decision` via `dbt run`, and the dashboard's own `to_display_row()` rendering it correctly against live DynamoDB items. **Extended same day** with `cost_per_reservation_eur`/`suggested_price_per_reservation_eur` (§3) on client request — unit-tested end to end, same shape as the original rollout.
**Depends on:** Phase 9 (`los_floor_matrix`, `LOS_CANDIDATES`, the amortization of `one_time_cost_eur` by `stay_length`), Phase 10 (`decision_components`, the reason-code mechanism this phase's recommendation plugs into), Phase 13 (`property_reference_price_eur`, the reference this phase compares the diluted floor against)
**Blocks:** Nothing downstream depends on this
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #12), [`docs/profitable-pricing-glossary.md`](../../../docs/profitable-pricing-glossary.md) §5/§9 (Break-Even/Floor, LOS), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #12, surfaced 2026-09-07), [Phase 9 spec](../09-los-floor-matrix/spec.md) (the matrix this phase reads, and its own explicit "not yet acted on" deferral in §2/§7)

---

## 1. Executive summary

Phase 9 computes, for every decision, what the cost floor would be at five candidate stay lengths (`{1, 2, 3, 7, 14}` nights) — but nothing reads that matrix. It sits in `price_decision.v1` as audit data. The external spec's own framing (§10.2, test scenario T11) is that this is not just explainability: a one-time booking cost (cleaning+laundry, `Cr`) charged in full to a single night can force a price above what the property could ever competitively charge, while the exact same cost diluted across 3+ nights clears easily. **The lever the PM has today (setting `minimum_stay` on the listing) is exactly the tool that fixes this — but the system never tells them when to use it, or to what value.**

This phase adds no new inputs and no new fan-out: `decide_price_los_matrix()` already produces a `rule_applied` per candidate, and — because `one_time_cost_eur / stay_length` is the only term that varies with `n`, `floor_type` is constant across every candidate in one decision (Phase 9 §3), and `market_reference_price_eur`/`property_reference_price_eur` don't depend on `stay_length` either — the floor is **provably monotonically non-increasing** as `stay_length` grows. That means `rule_applied` can only move `cost_protected → minimum_floor → market_competitive` as `n` increases within one decision, never backwards. A single well-defined threshold therefore always exists: the shortest candidate stay length that is no longer `cost_protected`. This phase computes exactly that, as pure logic over data Phase 9 already produces.

**Done when:** every `price_decision.v1` event carries a `calculation.minimum_stay_recommendation` whose `recommended_min_stay` is the shortest LOS candidate that clears `cost_protected` (or `null` if none of the five candidates do — a genuinely different, structural signal, see §4), together with the EUR/night floor relief that recommendation buys versus a 1-night stay, and — when the recommendation says something worth acting on — a decision component explaining it in the same place a PM already reads every other pricing reason.

**Not in this phase:**
- **Actually setting `minimum_stay` on a listing, or publishing LOS-specific rates.** This phase recommends; it does not act. Phase 9 already drew this line (§2) for the matrix itself, and this phase's recommendation inherits the same boundary — there is still no channel/PMS write-back path in this project (that would be new scope, closer to Phase 14's write-path precedent, not requested here).
- **Recomputing the recommendation against real availability.** Same structural gap Phase 9 already has (§7) — no occupancy/availability calendar exists anywhere in this project; a 7-night recommendation is not checked against whether 7 consecutive nights are actually free.
- **A new Flink stage, Kafka topic, or Postgres table.** Unlike Phase 11/14, this is pure logic added to the computation Stage B (`stage_price_decision.py`) already performs on data it already has (`los_floor_matrix`, `property_reference_price_eur`) — no new source, no new join, no new broadcast state.
- **Runtime-configurable "viability" threshold.** The threshold is "no longer `cost_protected`" — a fixed definition tied to the existing `rule_applied` enum, not a tunable knob. Same stance Phase 8/9 took on their own constants.
- **A dashboard panel dedicated to the recommendation** (e.g. a full min-stay planning view). A single new read-only column on the existing "Current price" table is in scope (§2) — a dedicated panel is a natural, low-effort follow-up (§8), not required here.

---

## 2. Scope

### In scope

**`libs/pricing-formulas`:**
- `src/pricing_formulas/decision_components.py` — new `MinimumStayReasonCode = Literal["minimum_stay_recommended", "minimum_stay_not_viable"]`, added to the `ReasonCode` union (alongside `PropertyReasonCode`/`RuleReasonCode`/`CommissionReasonCode`).
- `src/pricing_formulas/engine.py`:
  - New `MinimumStayRecommendation` frozen dataclass: `recommended_min_stay: int | None`, `floor_relief_eur: float | None`, `decision_component: DecisionComponent | None`.
  - New `recommend_minimum_stay(candidates: list[LosFloorCandidate], property_reference_price_eur: float) -> MinimumStayRecommendation` function — pure post-processing over an already-computed `los_floor_matrix`, no formula duplicated (same "thin composition" discipline `decide_price_los_matrix()` itself follows). Logic in §4.

**Flink (`streaming/flink-jobs`):**
- `src/flink_jobs/stage_price_decision.py` — `_build_price_decision()` calls `recommend_minimum_stay(los_matrix, calc.property_reference_price_eur)` once, alongside the existing `decide_price_los_matrix()` call; assembles `Calculation.minimum_stay_recommendation` from the result; appends `.decision_component` (when not `None`) to the same flat `decision_components` list Phase 10/11/14 already share.

**Shared schema / event contract:**
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — new `MinimumStayRecommendation` Pydantic model (`recommended_min_stay: int | None`, `floor_relief_eur: float | None`, both `ge=0`/`ge=1` where non-null, `extra="forbid"`); `Calculation` gains `minimum_stay_recommendation: MinimumStayRecommendation` (**required**, not nullable — always computable from data Stage B already has, same "always present" convention `los_floor_matrix` uses, not `manual_override`'s "sometimes-null" one); `ReasonCode` gains `"minimum_stay_recommended"` and `"minimum_stay_not_viable"` (11 values total).
- `specs/events/price_decision.v1.json` — `calculation.minimum_stay_recommendation`: required object with `recommended_min_stay` (`["integer","null"]`, `minimum: 1`) and `floor_relief_eur` (`["number","null"]`, `minimum: 0`); the shared `decision_component` `$defs` entry's `code` enum gains the two new values.

**Lakehouse / dbt:**
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — one new **required** `NestedField` group (continuing the `los_floor_matrix` convention, not `manual_override`'s optional one — this struct is always present) for `minimum_stay_recommendation`, fresh IDs continuing from 67 (Phase 14's last-used ID): `68` (the struct), `69` (`recommended_min_stay`, `IntegerType`), `70` (`floor_relief_eur`, `DoubleType`) — both leaf fields declared the same way `market_inputs.occupancy_rate`/`sample_size` already are (a Python `None` flows through the existing generic writer path unchanged; no new nullability handling needed).
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — new `_minimum_stay_recommendation(calculation: dict) -> dict` helper (always returns a dict, never `None` — mirrors `los_floor_matrix`'s always-present handling, not `manual_override`'s `None`-coercion style).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `calculation.minimum_stay_recommendation` (nested struct column, same mechanics as every prior nested addition).

**Dashboard (`dashboard`):**
- `src/dashboard/hot_path.py` — `to_display_row()` reads `item["calculation"]["minimum_stay_recommendation"]` and adds `min_stay_reco`, `cost_per_reservation_eur`, `suggested_price_per_reservation_eur` fields (each `None` displayed as `"—"`, same missing-value convention already used elsewhere in this table).
- `src/dashboard/app.py` — `render_current_prices()`'s `column_config` gains `"Min. stay reco."`, `"Cost per reservation"`, `"Suggested price (reservation)"` columns; the existing per-night cost column is relabeled `"Cost (1-night)"` (client-requested 2026-09-07, to disambiguate it from the new per-reservation figures — `total_cost_eur` itself is unchanged, still `fixed_cost_eur + variable_cost_eur + one_time_cost_eur` undiluted, i.e. what a 1-night stay alone would cost). No new color logic — these are plain informational columns, not a status.

**Contracts / tests:**
- 4 existing `specs/contracts/fixtures/price_decision/*.json` gain `calculation.minimum_stay_recommendation` (all four already-`cost_protected`-at-LOS-1 fixtures get a real, non-trivial recommendation; the rest get `{"recommended_min_stay": 1, "floor_relief_eur": 0.0}` — minimally changed, same convention Phase 11/14 followed).
- 1 new fixture, `specs/contracts/fixtures/price_decision/minimum_stay_not_viable.json`, exercising the `recommended_min_stay: null` shape (a `cost_protected`-at-every-candidate apartment, e.g. the kind already observed live for several seeded apartments where `profit_floor > market_avg` structurally).
- Unit tests: `recommend_minimum_stay()` (pure logic, no Flink) — the three cases in §4 (already fine at LOS 1, recommendation found at some `n > 1`, not viable at any candidate) plus the monotonicity invariant itself (§1) as a property-style test across a range of `Cr`/`Cf`/`Cv` inputs; `stage_price_decision.py`'s emitted `PriceDecision.calculation.minimum_stay_recommendation` has the right shape and the right decision component appended (or not) in each case; `shared_schemas` contract tests (both fixture shapes); `lakehouse-consumer` schema/transform tests; `dashboard.hot_path.to_display_row()`'s new field, including the `None` → `"—"` case.

### Out of scope (explicitly deferred)

- Acting on the recommendation (writing `minimum_stay` anywhere), availability-aware recomputation, a configurable viability threshold, and a dedicated dashboard panel — all listed in §1's "Not in this phase," not repeated here.
- **A new Flink stage or state.** This phase's logic runs inline inside the existing Stage B computation — no new `MapState`, no new broadcast, no new `KeyedProcessFunction`.

---

## 3. Data model

### `Calculation` — one new required field

| Field | Type | Description |
|---|---|---|
| `minimum_stay_recommendation` | `MinimumStayRecommendation` | Always present — computed from `los_floor_matrix` and `property_reference_price_eur`, both already part of this same decision. |

### `MinimumStayRecommendation` (new)

| Field | Type | Description |
|---|---|---|
| `recommended_min_stay` | `int >= 1` \| `null` | Shortest candidate `stay_length` in `LOS_CANDIDATES` whose floor is no longer `cost_protected`. `null` when even the longest candidate (14 nights) still is — a structural cost problem, not a dilution one (§4). |
| `floor_relief_eur` | `number >= 0` \| `null` | `minimum_price_eur` at `stay_length=1` minus `minimum_price_eur` at `recommended_min_stay`. `0.0` when `recommended_min_stay == 1` (nothing to relieve — already fine at one night). `null` iff `recommended_min_stay` is `null` (no candidate resolves it, so there is no relief figure to report). |
| `cost_per_reservation_eur` | `number >= 0` \| `null` | **Added 2026-09-07** on client request (a "how much does the whole booking cost, not just one night" question). Total cost for a reservation of `recommended_min_stay` nights: `recommended_min_stay × (fixed_cost_eur + variable_cost_eur) + one_time_cost_eur` — fixed/variable cost recur every night, the one-time cost is paid once per booking, the inverse of `minimum_price_eur`'s own per-night amortization (§4 of Phase 9's spec). `null` iff `recommended_min_stay` is `null`. |
| `suggested_price_per_reservation_eur` | `number >= 0` \| `null` | **Added 2026-09-07**, same client request — answers "what should I actually charge for the whole stay." The `recommended_min_stay` candidate's own `los_floor_matrix[].suggested_price_eur` (never itself `cost_protected` — by construction at or below the market reference) × its `stay_length`. `null` iff `recommended_min_stay` is `null`. |

Not touched: `los_floor_matrix` itself (Phase 9, unchanged — this phase only reads it), `output.*` (this phase never changes what price is actually suggested, only what it recommends about `minimum_stay` policy — same "recommend, don't act" boundary Phase 9 §2 drew for the matrix itself).

**Why a total reservation price, not just a total cost:** the client's ask was explicit — recommend a price per reservation that (a) covers `cost_per_reservation_eur` plus the PM's target margin, and (b) stays competitive against the market average, rather than just exposing the cost. `suggested_price_per_reservation_eur` already satisfies both by construction: it's `recommended_min_stay × suggested_price_eur` from the exact `los_floor_matrix` candidate whose `rule_applied` is `market_competitive`/`minimum_floor` (never `cost_protected`) — the same guarantee `los_floor_matrix` already gives per night, simply multiplied out to a reservation total. No new formula, no new guardrail — a restatement of data already proven correct.

---

## 4. Logic

`recommend_minimum_stay()` (`libs/pricing-formulas/src/pricing_formulas/engine.py`), pure function, no Flink dependency:

```python
def recommend_minimum_stay(
    candidates: list[LosFloorCandidate],
    property_reference_price_eur: float,
) -> MinimumStayRecommendation:
    ordered = sorted(candidates, key=lambda c: c.stay_length)
    at_one_night = ordered[0]

    if at_one_night.rule_applied != "cost_protected":
        return MinimumStayRecommendation(
            recommended_min_stay=at_one_night.stay_length,
            floor_relief_eur=0.0,
            decision_component=None,
        )

    for candidate in ordered[1:]:
        if candidate.rule_applied != "cost_protected":
            floor_relief_eur = round(
                at_one_night.minimum_price_eur - candidate.minimum_price_eur, 2
            )
            return MinimumStayRecommendation(
                recommended_min_stay=candidate.stay_length,
                floor_relief_eur=floor_relief_eur,
                decision_component=DecisionComponent(
                    code="minimum_stay_recommended",
                    label=(
                        f"At {at_one_night.stay_length} night(s), the cost floor "
                        f"({at_one_night.minimum_price_eur} EUR) exceeds the "
                        f"property reference price, forcing an uncompetitive "
                        f"price. A minimum stay of {candidate.stay_length} "
                        f"nights dilutes the one-time booking cost enough to "
                        f"clear it ({floor_relief_eur} EUR/night floor relief)."
                    ),
                    impact=floor_relief_eur,
                ),
            )

    longest = ordered[-1]
    residual_gap_eur = round(longest.minimum_price_eur - property_reference_price_eur, 2)
    return MinimumStayRecommendation(
        recommended_min_stay=None,
        floor_relief_eur=None,
        decision_component=DecisionComponent(
            code="minimum_stay_not_viable",
            label=(
                f"Even at {longest.stay_length} nights, the cost floor still "
                f"exceeds the property reference price by {residual_gap_eur} "
                f"EUR — the fixed/variable cost base, not the one-time "
                f"booking cost, is the binding constraint. A minimum-stay "
                f"policy alone will not fix this."
            ),
            impact=residual_gap_eur,
        ),
    )
```

**Why this threshold, and why it's well-defined (not a heuristic):** within one decision, `floor_type` is identical across every LOS candidate (Phase 9 §3 — selected by `days_to_arrival`, which doesn't vary with `stay_length`), and `market_reference_price_eur`/`property_reference_price_eur` are likewise constant across candidates. The only term that varies with `n` is `one_time_cost_eur / n` inside `minimum_price_eur`, which is strictly non-increasing as `n` grows. Since `rule_applied` is a pure comparison of `minimum_price_eur` (falling) against two constants, it can only move `cost_protected → minimum_floor → market_competitive` as `n` increases, never the reverse. The shortest non-`cost_protected` candidate is therefore the unique correct threshold — not one of several plausible heuristics.

**Why `cost_protected`, not `minimum_floor`, is the trigger:** `minimum_floor` still guarantees the target margin — it's just priced above the raw market average, which is a legitimate (if less competitive) outcome. `cost_protected` means the floor exceeds even this specific property's own Bonus/Malus-adjusted reference price (`rule_cost_protected`'s own existing definition, `guardrails.py`) — i.e. the price needed to break even is one this property could not realistically charge at all. That is the one state a minimum-stay policy is meant to fix.

### Worked example

Reusing Phase 9 spec §4's own worked example verbatim (`Cf=0`, `Cv=0`, `Cr=110.0`, `target_margin=0.05`, `commission_pct=0.15`, `avg_nightly_rate_eur=90.0`, `competitiveness_discount=0.05`, `property_attribute_factor=1.0`, `property_reference_price_eur=90.0`):

| `stay_length` | `minimum_price_eur` | `rule_applied` |
|---|---|---|
| 1 | 137.50 | `cost_protected` |
| 2 | 68.75 | `market_competitive` |
| 3 | 45.83 | `market_competitive` |
| 7 | 19.64 | `market_competitive` |
| 14 | 9.82 | `market_competitive` |

`at_one_night.rule_applied == "cost_protected"` → scan forward. `stay_length=2` already clears it (`68.75 <= market_reference_price_eur=85.50`, `market_competitive`). Result:

```json
{"recommended_min_stay": 2, "floor_relief_eur": 68.75}
```

(`137.50 - 68.75 = 68.75`). A PM reading this sees: *"a 1-night stay here can't be priced competitively — require at least 2 nights and the floor drops by €68.75/night, clearing the market rate."*

---

## 5. Acceptance criteria

- **AC-01 — Already-fine-at-LOS-1 case.** An apartment whose `stay_length=1` candidate is not `cost_protected` gets `recommended_min_stay=1`, `floor_relief_eur=0.0`, no decision component appended.
- **AC-02 — Recommendation-found case.** The §4 worked example (or an equivalent) produces the correct `recommended_min_stay`, a `floor_relief_eur` exactly equal to the floor delta between LOS 1 and the recommended candidate, and a `minimum_stay_recommended` decision component with that same value as `impact`.
- **AC-03 — Not-viable case.** An apartment whose `stay_length=14` candidate is still `cost_protected` gets `recommended_min_stay=None`, `floor_relief_eur=None`, and a `minimum_stay_not_viable` component whose `impact` equals the residual gap at the longest candidate.
- **AC-04 — Monotonicity invariant holds.** Property-style test: for a range of `(fixed_cost_eur, variable_cost_eur, one_time_cost_eur)` inputs, `LosFloorCandidate.minimum_price_eur` is non-increasing as `stay_length` increases across `LOS_CANDIDATES`, and `rule_applied` never regresses from a "better" state to a "worse" one (`market_competitive`/`minimum_floor` → `cost_protected`) as `stay_length` grows. This is the correctness precondition §4's algorithm relies on — tested directly, not just assumed.
- **AC-05 — `stage_price_decision.py` wires it correctly.** The emitted `PriceDecision.calculation.minimum_stay_recommendation` matches a standalone `recommend_minimum_stay()` call with the same `los_floor_matrix`/`property_reference_price_eur`; the extra decision component (when present) appears in `calculation.decision_components`, appended after the existing ones (order matches Phase 11/14's own "append, never reorder" convention).
- **AC-06 — `price_decision.v1` validates both shapes.** Contract test covers `recommended_min_stay: null` (AC-03's shape) and a populated one (AC-02's shape).
- **AC-07 — lakehouse-consumer carries the new required struct without error.** Schema/transform tests cover both `recommended_min_stay` value shapes; `dbt run`/`dbt test` against updated fixtures pass with `minimum_stay_recommendation` present on `fct_price_decision`.
- **AC-08 — Dashboard displays it.** `to_display_row()` surfaces `min_stay_reco`, `cost_per_reservation_eur`, `suggested_price_per_reservation_eur` correctly for both a populated and a `null` recommendation (`"—"`), and the per-night cost column is labeled `"Cost (1-night)"`.
- **AC-10 — Reservation totals are correct in both non-`null` cases.** For the already-fine case, `cost_per_reservation_eur`/`suggested_price_per_reservation_eur` equal the standalone `decide_price()` call's own `total_cost_eur`/`suggested_price_eur` at `stay_length=1`. For the recommendation-found case (§4's worked example), `cost_per_reservation_eur == recommended_min_stay × (fixed_cost_eur + variable_cost_eur) + one_time_cost_eur` and `suggested_price_per_reservation_eur == recommended_min_stay × los_floor_matrix[recommended_min_stay].suggested_price_eur`, both cross-checked against `recommend_minimum_stay()` directly.
- **AC-09 — Live verification.** Bring up the stack; confirm a real apartment/night whose live decision is `cost_protected` at LOS 1 (per the earlier live-verification session, e.g. BCN-002/BCN-004/BCN-006/MAD-007/MAD-009/MAD-010 are candidates) shows a sensible `minimum_stay_recommendation` in DynamoDB, that it reaches `fct_price_decision` via `dbt run`, and that the dashboard's new column renders it. No new topic/table/stage to verify (§2) — this AC exists to catch anything the unit suite can't (same standing practice as every prior phase), not because new infrastructure is involved.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `recommend_minimum_stay()` (AC-01–AC-04) — same tier `decide_price()`/`decide_price_los_matrix()` are tested at.
- **Integration, no infrastructure:** `stage_price_decision.py`'s `_build_price_decision()` wiring (AC-05) — extends the existing fakes-based `test_stage_price_decision.py` suite.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-06), lakehouse-consumer schema/transform unit tests (AC-07).
- **Dashboard unit test:** `test_hot_path.py` (AC-08), same style as Phase 6's own `price_status()` boundary tests.
- **Live verification against LocalStack (required, AC-09):** lighter-weight than Phase 11/14's (no new Postgres table, Kafka topic, or Flink stage to bring up) — confirm the new field on already-flowing decisions.

---

## 7. Known limitations

- **Not availability-aware.** Same gap Phase 9 already has (§7) — a recommended 7-night minimum stay is not checked against whether 7 consecutive nights are actually free; this project has no occupancy/availability calendar anywhere.
- **The "not viable" case gives a diagnosis, not a fix.** When `recommended_min_stay` is `null`, the real problem is the fixed/variable cost base itself (or the target margin/commission), not stay length — this phase surfaces that distinction (§4's `residual_gap_eur`) but doesn't recommend what to change about the cost or margin inputs.
- **`LOS_CANDIDATES` remains a fixed code constant** (Phase 9's own limitation, inherited unchanged) — the recommendation can only ever be one of `{1, 2, 3, 7, 14}`, never e.g. "5 nights," even if that would be the tighter true threshold.
- **No write-back to any channel or PMS.** The recommendation is informational only (§1) — nothing in this project can currently set a listing's actual minimum-stay policy.

---

## 8. Follow-ups

- **`docs/post-poc-roadmap.md`** — backlog #12 moves from "Next" to "Done" once this phase is implemented, unit-tested, and live-verified.
- **A dedicated dashboard "Minimum Stay Planner" view**, aggregating `minimum_stay_recommendation` across every apartment/night to show a PM which listings would benefit most from a minimum-stay policy change — the single-column addition here is the minimum useful surface, not the ceiling.
- **If a write-back path to a channel/PMS is ever built** (the natural extension of Phase 14's write-path precedent, applied to `minimum_stay` instead of price), this phase's `recommended_min_stay` is exactly the value such a mechanism would publish — no rework needed here to support it.

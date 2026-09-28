# ADR-0007 — `price_decision.v1`: a third `rule_applied` state (`cost_protected`), `below_market_by` always computed, `data_age_seconds` added

**Date:** 2026-07-28
**Status:** Accepted

## Context

`price_decision.v1.json` is the first event contract in this project that a later phase needs to change after an earlier phase already treated it as settled (Phase 4 modifying a schema Phase 4 itself introduced, before Phase 5 or 6 consume it — still the first "contract we already shipped, now changing" case per this project's own precedent, see ADR-0005's own note about catching issues at spec time). Two gaps surfaced during Phase 4 design discussion, on top of a mechanical addition:

**1. `rule_applied` currently only distinguishes "market won" from "floor won"; it doesn't distinguish *how badly* the floor won.** The confirmed business rule (Phase 4 design conversation, 2026-07-28) is: `minimum_price_eur` (cost + `target_margin`) is an **absolute, non-negotiable floor** — the system must never suggest a price below it under any circumstance. The only levers a property owner has to change that floor are business decisions outside this pipeline entirely (lower `target_margin`, or stop listing the apartment) — the pricing engine itself never overrides it. Given that guarantee, there are two structurally different ways the floor can "win," and they carry different operational meaning for the property manager reading a dashboard built on this data:

   - The floor wins, but only just — `minimum_price_eur` is still **at or below** the raw, undiscounted `avg_nightly_rate_eur`. The apartment is still priced competitively against the broad market; it just isn't getting the extra `competitiveness_discount` edge. Mildly informative, not alarming.
   - The floor wins so decisively that `minimum_price_eur` **exceeds** `avg_nightly_rate_eur` itself — the apartment is now priced *above* what similar listings charge on average, purely because its costs demand it. This is the actionable signal: the property manager's costs are pricing them out of their own market, and the only fix is the business decision the design conversation named explicitly (margin or listing).

   The existing two-value enum (`minimum_floor`, `market_competitive`) cannot express this distinction — both of the above collapse into `minimum_floor` today.

**2. `below_market_by` is currently nulled out whenever `rule_applied = minimum_floor`** (schema description: "Null when `rule_applied=minimum_floor` (market signal was irrelevant)"). That was true under the old two-state model, where "floor won" was treated as "market comparison doesn't matter." It stops being true once the distinction above exists — the *entire point* of the new third state is a market comparison (`avg_nightly_rate_eur` vs. `suggested_price_eur`), so nulling this field for any floor-wins case throws away the exact number a consumer needs to answer "by how much."

**3. `data_age_seconds`** — Decision E.1 (dead-man's-switch freshness watchdog) already committed to including this field on every `price_decision`, "computed inline (free, no new infrastructure)," but it was never added to the schema itself — an implementation detail decided in the design doc that hadn't yet landed in the actual contract.

## Decision

`specs/events/price_decision.v1.json` changes as follows (bump `schema_version` handling is out of scope here — see Consequences):

**`calculation.rule_applied`** — enum becomes `["market_competitive", "minimum_floor", "cost_protected"]`, with the boundary redefined against `avg_nightly_rate_eur`, not just `market_reference_price_eur`:

| Value | Condition | `suggested_price_eur` |
|---|---|---|
| `market_competitive` | `minimum_price_eur <= market_reference_price_eur` | `market_reference_price_eur` |
| `minimum_floor` | `market_reference_price_eur < minimum_price_eur <= avg_nightly_rate_eur` | `minimum_price_eur` |
| `cost_protected` | `minimum_price_eur > avg_nightly_rate_eur` | `minimum_price_eur` |

`market_competitive` and `minimum_floor`'s underlying formula is unchanged (`suggested_price_eur = max(minimum_price_eur, market_reference_price_eur)` still holds for both) — the only change is that "floor won" is now split into two enum values by a threshold check against `avg_nightly_rate_eur` that costs nothing extra to compute (`avg_nightly_rate_eur` is already a required field on `market_inputs`).

**`output.below_market_by`** — always computed as `avg_nightly_rate_eur - suggested_price_eur`, for all three `rule_applied` values. Type changes from `["number", "null"]` to `"number"` — it is never null now, since a `price_decision` is never emitted without `market_inputs` (the join in Decision D only fires when both sides of the segment's state have data). Sign convention: positive means priced below the raw market average (the common case, and always true for `market_competitive`); negative means priced above it (the `cost_protected` signal made visible as a number, not just a category).

**`market_inputs.data_age_seconds`** — new field, `"type": "integer", "minimum": 0`, description: "Seconds between `market_inputs.collected_at` and `decided_at`. Computed inline at decision time (Decision E.1) — freshness indicator, independent of and cheaper than the separate `data_stale` side-output, which only fires past the 48h dead-man's-switch threshold."

## Rationale

- **The threshold change (`market_reference_price_eur` → `avg_nightly_rate_eur`) is the actual business question, not a cosmetic rename.** `market_reference_price_eur` already has the `competitiveness_discount` baked in — comparing the floor against it answers "did we get to apply our usual discount," not "are we still a competitive listing at all." `avg_nightly_rate_eur` is the number that answers the second, more important question, and it's the one a property manager actually benchmarks against mentally.
- **This is exactly the guarantee Point 2 of the Phase 4 design conversation states**: cost + margin is inviolable, and crossing into "priced above the raw market average" is precisely the moment that guarantee has a real business cost — the schema should make that moment visible as data, not require a downstream dbt model or dashboard to re-derive it by comparing two other fields.
- **Un-nulling `below_market_by` costs nothing and removes an information gap** — the field's only purpose is to let a consumer see distance-from-market without recomputing `avg_nightly_rate_eur - suggested_price_eur` itself; nulling it in the one case where the number is most informative (large negative, i.e. `cost_protected`) was actively counterproductive under the old model.
- **`data_age_seconds` was already a committed design decision** (E.1); this ADR just closes the gap between "decided in the design doc" and "present in the actual contract," per this project's own standing complaint about `heartbeat.interval.ms` (Phase 2, `error-handling/debezium-heartbeat-topic-stalls-entire-connector.md`): "every setting... should be a decision, not an artifact" applies equally to schema fields that were decided but never written down where the contract lives.

### Alternatives considered and rejected

- **Keep two enum values, add a boolean `above_market_average` flag instead of a third enum state** — rejected: `rule_applied` is documented as "the rule applied," and there genuinely are three distinct rules/outcomes now, not two rules plus an orthogonal flag. A reader scanning `rule_applied` alone should see the full picture without cross-referencing a second field.
- **Name the third state `above_market` instead of `cost_protected`** — considered, rejected in favor of `cost_protected` because it names the *cause* (the cost floor protecting margin) rather than only the *symptom* (ending up above market) — consistent with `minimum_floor` and `market_competitive` already naming causes, not just price-position symptoms.
- **A fourth state for "no market data available"** — rejected: a `price_decision` is structurally never emitted without market data (Decision D's join only fires when both `MapState`s have an entry), so this case cannot occur under the current design; adding an enum value for it would document a scenario the pipeline cannot produce.

## Consequences

- `specs/phases/04-flink-processing/spec.md` (not yet written) must implement this three-way branch directly in the `KeyedProcessFunction` from Decision D, and its acceptance criteria must include a worked numeric example for all three states, per the design doc's own requirement ("el ejemplo numérico trabajado... debe quedar escrito dentro del spec mismo").
- Contract-test fixtures under `specs/contracts/` (pattern established in Phase 3, `test_market_price_contract.py`) will need an equivalent `test_price_decision_contract.py` covering all three `rule_applied` values plus the new `data_age_seconds` field, once that test file exists.
- No `schema_version` bump — `price_decision.v1.json` has never had a real consumer yet (Phase 4 hasn't shipped), so this is still pre-release evolution of `v1`, not a breaking change against data already written by a running system.

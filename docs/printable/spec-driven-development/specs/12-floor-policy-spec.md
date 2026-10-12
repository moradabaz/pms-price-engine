# Phase 12 — Explicit Hard/Soft floor policy

**Status:** Draft
**Depends on:** ADR-0009 (`floor_type`'s three tiers), Phase 9 (`los_floor_matrix`)
**Blocks:** nothing — pure classification of an existing field
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #10), [`docs/phase-12-floor-policy-design-decisions.md`](../../../docs/phase-12-floor-policy-design-decisions.md) (pre-spec, decisions A–E), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #10)

---

## 1. Executive summary

The external spec names two floor kinds explicitly: a Hard floor (absolute,
never crossed) and a Soft floor (a margin target that can be relaxed under
booking-window pressure). This repo already computes exactly that
distinction — `floor_type`'s `contribution` tier is the Hard floor, and
`structural_full_margin`/`structural_reduced_margin` are both Soft floor —
but only implicitly, via which of three tier names fired.

This phase adds `floor_policy: Literal["hard", "soft"]`, a pure,
one-line-derived classification of the existing `floor_type`, at both
places `floor_type` already appears (`Calculation`, `LosFloorCandidate`).
No pricing formula changes.

**Done when:** a real seeded apartment's live DynamoDB `price_decision` item
shows `calculation.floor_policy` matching `calculation.floor_type`
(`contribution` → `hard`, either structural tier → `soft`), and every one of
its `los_floor_matrix` candidates shows the same correct pairing.

**Not in this phase:** any change to which tier fires or its formula
(ADR-0009 is unchanged); a new `decision_components` reason code (pre-spec
§C — `floor_policy` restates `floor_type`, it doesn't add new information).

---

## 2. Scope

### In scope

- `streaming/flink-jobs/src/flink_jobs/pricing.py` — `FloorPolicy = Literal["hard", "soft"]`; `floor_policy_for(floor_type: FloorType) -> FloorPolicy` (pure, `"hard"` iff `floor_type == "contribution"`); `PriceCalculation`/`LosFloorCandidate` gain `floor_policy: FloorPolicy`, both populated via `floor_policy_for()` inside `decide_price()`/`decide_price_los_matrix()`.
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — `FloorPolicy` Literal; `Calculation.floor_policy`, `LosFloorCandidate.floor_policy` (both required).
- `specs/events/price_decision.v1.json` — `calculation.floor_policy` and `calculation.los_floor_matrix.items.floor_policy`, each a 2-value enum, both required.
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` — wires `calc.floor_policy` / `candidate.floor_policy` into the `Calculation`/`LosFloorCandidate` constructors.
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — two new `NestedField`s (IDs 60, 61 — last used: 59), appended, never inserted before existing IDs.
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — passthrough for both locations; for a record predating this phase (no `floor_policy` key), falls back to a locally re-derived value from that record's own `floor_type` (pre-spec §D) rather than a fabricated constant.
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `calculation.floor_policy` (the nested `los_floor_matrix[].floor_policy` rides along inside that existing struct-list column, no separate dbt column needed).
- `specs/contracts/fixtures/price_decision/*.json` (3 files) — add `floor_policy` at both nesting levels, computed from each fixture's own `floor_type`.
- Unit tests: `pricing.py` (`floor_policy_for()` both branches, `decide_price()`/`decide_price_los_matrix()` assembly across all three `floor_type` tiers), `stage_price_decision.py` (wiring), `shared_schemas` contract tests, `lakehouse-consumer` (schema migration, transform fallback-derivation for a missing key).
- Live LocalStack verification (required, AC-06) — lighter-weight than Phase 10/11's (pre-spec §E): confirms values, not a new shape.

### Out of scope

- Any change to `floor_type`'s tier boundaries or the pricing formula (ADR-0009 stays authoritative).
- A new `decision_components` reason code for `floor_policy` (pre-spec §C).
- Any change to `rule_applied` — a separate axis (which of `market_competitive`/`minimum_floor`/`cost_protected` won), orthogonal to `floor_policy` (which floor tier was in force).

---

## 3. Data model

`floor_policy: Literal["hard", "soft"]`, at:

| Location | Derived from |
|---|---|
| `calculation.floor_policy` | `calculation.floor_type` |
| `calculation.los_floor_matrix[].floor_policy` | that candidate's own `floor_type` |

| `floor_type` | `floor_policy` |
|---|---|
| `structural_full_margin` | `soft` |
| `structural_reduced_margin` | `soft` |
| `contribution` | `hard` |

---

## 4. Formula

```python
FloorPolicy = Literal["hard", "soft"]

def floor_policy_for(floor_type: FloorType) -> FloorPolicy:
    return "hard" if floor_type == "contribution" else "soft"
```

Called once per `decide_price()` invocation (top-level and once per LOS
candidate via `decide_price_los_matrix()`), immediately after `floor_type`
is decided. No other formula in this file changes.

---

## 5. Acceptance criteria

- **AC-01 — `floor_policy_for()` is correct for all three `floor_type` values.** `contribution` → `hard`; `structural_full_margin` and `structural_reduced_margin` → `soft`.
- **AC-02 — `decide_price()` sets `floor_policy` consistently with the `floor_type` it computed**, across the existing days-to-arrival boundary tests (`test_floor_type_boundaries`'s three tiers).
- **AC-03 — `decide_price_los_matrix()` sets each candidate's `floor_policy` from its own `floor_type`**, not copied from the top-level decision (still identical in practice today per pre-spec §B, but derived independently).
- **AC-04 — `price_decision.v1` validates `floor_policy` at both nesting levels** — contract test covers presence and the closed 2-value enum.
- **AC-05 — lakehouse-consumer handles both a present and a missing `floor_policy`.** Present: passthrough. Missing (pre-Phase-12 record): re-derived from that same record's `floor_type`, verified for both a `contribution` and a `structural_*` fixture.
- **AC-06 — Live LocalStack verification (required).** A real seeded apartment's live DynamoDB item shows `calculation.floor_policy` correctly paired with `calculation.floor_type`, and every `los_floor_matrix` candidate shows the same correct pairing — confirmed via a direct DynamoDB read and/or `dbt show` on `fct_price_decision`.

---

## 6. Test strategy

- **Pure logic:** `floor_policy_for()` (AC-01), `decide_price()`/`decide_price_los_matrix()` assembly (AC-02, AC-03).
- **Static/schema review:** `price_decision.v1.json` contract test (AC-04).
- **Infrastructure-adjacent, no LocalStack:** lakehouse-consumer schema-migration and transform fallback-derivation tests (AC-05).
- **Live LocalStack (required, AC-06):** full bring-up per `docs/manual/MANUAL.md`; read one apartment's `price_decision` item and confirm the `floor_type`/`floor_policy` pairing at both nesting levels.

---

## 7. Known limitations

- `floor_policy` is entirely determined by `floor_type`, which is itself entirely determined by `days_to_arrival` (ADR-0009) — it carries no independent information today. Its value is in aligning this repo's vocabulary with the external spec's terminology, not adding a new pricing dimension.
- If backlog #7 (layered RM engine) later introduces floor tiers that don't map cleanly to a strict `contribution`-is-Hard/everything-else-is-Soft split, `floor_policy_for()`'s single boolean rule would need revisiting — not a concern until that phase.

---

## 8. Follow-ups

- `docs/post-poc-roadmap.md` — backlog #10 moves from "Later" to "Done" (this spec).

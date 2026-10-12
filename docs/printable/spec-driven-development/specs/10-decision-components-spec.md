# Phase 10 — Decision Components (structured reason codes)

**Status:** Draft
**Depends on:** Phase 8 (`property_attributes.py`, Property Bonus/Malus), Phase 9 (`los_floor_matrix`, the first list field on `Calculation`)
**Blocks:** Backlog #5 (owner contract), #7 (layered RM engine), #9 (manual overrides) — all three need a place to record which rule/layer fired (ADR-0011)
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #4), [ADR-0009](../../../docs/adr/ADR-0009-profitability-floor-reform.md) (the floor/rule logic this phase explains), [`docs/phase-10-decision-components-design-decisions.md`](../../../docs/phase-10-decision-components-design-decisions.md) (pre-spec, decisions A–F), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #4), [`error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md`](../../../error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md) (named this field as the next thing to exercise the list-serialization path)

---

## 1. Executive summary

Two facts in every `price_decision` are currently opaque scalars with no attached reason: `property_attribute_factor` (Phase 8) hides four independent attribute contributions behind one multiplier, and `rule_applied` (ADR-0009) states which of the three pricing rules fired with no record of the margin/gap that decided it. Both are exactly the "which rule fired" gap ADR-0011 named as the reason explainability is a prerequisite for the layered RM engine (#7), owner contracts (#5), and manual overrides (#9) — all three need somewhere to record a fired rule before they can be built without redoing this schema change.

This phase adds `calculation.decision_components: list[DecisionComponent]` — a structured, closed-vocabulary breakdown combining (a) the four Property Bonus/Malus contributions and (b) one component explaining the fired pricing rule — plus the same rule-only breakdown on each `los_floor_matrix` candidate (Phase 9), since a candidate's rule can differ from the top-level decision's.

**Done when:** for a real seeded apartment with non-neutral attributes, `calculation.decision_components` in a live DynamoDB item contains exactly 5 entries (4 property + 1 rule) whose `impact` values reproduce `property_attributes.py`'s weights and the actual price/floor gap, and each of its 5 `los_floor_matrix` candidates carries its own single rule component — provably, via a live LocalStack read, not just unit tests.

**Not in this phase:** owner contracts (#5), the layered RM engine (#7), manual overrides (#9) — this phase builds the container those need, not their logic; decomposing `minimum_price_eur` into cost/margin/commission terms (already individually visible as separate fields, so a component list would add no new information — see pre-spec §A); a dashboard view rendering the components (Phase 6 territory); runtime-configurable reason codes or weights (still code constants, same stance every prior phase took on `property_attributes.py`'s and `pricing.py`'s constants).

---

## 2. Scope

### In scope

- `streaming/flink-jobs/src/flink_jobs/decision_components.py` (new) — the one `DecisionComponent` dataclass and the `ReasonCode`/`PropertyReasonCode`/`RuleReasonCode` `Literal` aliases, shared by `property_attributes.py` and `pricing.py` (pre-spec §C) — avoids two near-identical dataclasses.
- `streaming/flink-jobs/src/flink_jobs/property_attributes.py` — new `property_attribute_components(quality_tier, rating, has_view, has_parking) -> list[DecisionComponent]`; `property_attribute_factor()` refactored to derive its result from `property_attribute_components()`'s impacts (`1 + Σimpact`, then clamp) instead of repeating the weight arithmetic.
- `streaming/flink-jobs/src/flink_jobs/stage_cost_enrichment.py` — `CostEnrichmentFunction.process_element` calls `property_attribute_components(...)` alongside the existing `property_attribute_factor(...)` call, attaches the result to the emitted `CostAggregate`.
- `streaming/flink-jobs/src/flink_jobs/models.py` — `CostAggregate` gains `property_decision_components: tuple[DecisionComponent, ...] = ()` (default empty tuple keeps every pre-Phase-10 `CostAggregate` construction, tests included, valid unchanged — same convention Phase 8's `property_attribute_factor: float = 1.0` established).
- `streaming/flink-jobs/src/flink_jobs/pricing.py`:
  - new `rule_decision_component(rule_applied, minimum_price_eur, market_reference_price_eur, property_reference_price_eur) -> DecisionComponent` — pure helper, called once inside `decide_price()`.
  - `decide_price()` gains `property_decision_components: Sequence[DecisionComponent] = ()`; `PriceCalculation` gains `decision_components: list[DecisionComponent]` = `[*property_decision_components, rule_component]`.
  - `LosFloorCandidate` gains `decision_components: list[DecisionComponent]`, populated from each per-candidate `decide_price()` call's own result — `decide_price_los_matrix()` does **not** forward `property_decision_components` to those calls (pre-spec §E), so each candidate's list is naturally just `[rule_component]`.
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` — passes `cost.property_decision_components` into the top-level `decide_price()` call; converts `calc.decision_components`/each `candidate.decision_components` into `shared_schemas.price_decision.DecisionComponent` instances when building `Calculation`/`LosFloorCandidate`.
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — `ReasonCode` `Literal` (7 values); `DecisionComponent` Pydantic model (`extra="forbid"`); `Calculation.decision_components: list[DecisionComponent]`; `LosFloorCandidate.decision_components: list[DecisionComponent]`.
- `specs/events/price_decision.v1.json` — `calculation.decision_components` (required, array, `minItems: 1`) and `calculation.los_floor_matrix.items.decision_components` (required, array, `minItems: 1`), each item `{code, label, impact}` with `code` as a 7-value enum.
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — new `NestedField`s starting at 49 (last used: 48), appended (never inserted before existing IDs): one `ListType` for `calculation.decision_components`, one more (nested one level deeper) inside `los_floor_matrix`'s element struct for `decision_components`.
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — passthrough for both `decision_components` locations; defensive `.get(..., [])` defaults for DynamoDB Streams records predating this phase (same pattern as Phase 9's `los_floor_matrix` default).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through both `decision_components` columns (pure schema evolution, no new dbt model).
- `specs/contracts/fixtures/price_decision/*.json` (3 files) — updated with real `decision_components` arrays, computed by actually running the refactored functions and copying their exact output (same discipline Phase 9 used).
- Unit tests: `property_attributes.py` (component-level breakdown, factor regression), `pricing.py` (`rule_decision_component()` per branch, `decide_price()`'s assembled list, `decide_price_los_matrix()`'s rule-only candidates), `stage_price_decision.py` (wiring), `shared_schemas` contract tests, `lakehouse-consumer` (schema migration including the nested-list-in-list case, transform defaults, `dynamodb_sink.py` regression for a nested list of lists).
- **Live LocalStack verification is a required acceptance criterion for this phase** (AC-09) — not optional the way Phase 8's was. Unlike Phase 8's scalar addition, this phase repeats Phase 9's "first time this shape appears" pattern once more: a list nested inside `los_floor_matrix`'s own list has never been exercised by `dynamodb_sink.py`/`iceberg_writer.py` before, even though flat lists-of-dicts are already proven.

### Out of scope (explicitly deferred)

- **Owner contract / configurable commission base** (backlog #5), **layered Revenue Management engine** (backlog #7), **Manual Override + audit trail** (backlog #9) — this phase builds the `decision_components` container those three need, not any of their own logic.
- **Decomposing `minimum_price_eur`'s cost/margin/commission terms** — rejected in the pre-spec (§A): `fixed_cost_eur`, `variable_cost_eur`, `one_time_cost_eur`, `target_margin`, `commission_pct` are already individually visible as separate fields; a component list restating them adds no new information.
- **A dashboard view rendering `decision_components`** (Phase 6 territory) — a natural follow-up, not required here.
- **Runtime-configurable reason codes or weights** — `ReasonCode` stays a closed code enum and `property_attributes.py`'s weights stay code constants, the same stance every prior phase (Phase 8's Bonus/Malus weights, Phase 9's `LOS_CANDIDATES`) already took.
- **More than one rule component per decision** — there is exactly one `rule_applied` per `Calculation`/`LosFloorCandidate`, so exactly one rule component explains it; no "candidate rules considered but not chosen" entries.

---

## 3. Data model

### `decision_components.py` (new) — the shared shape

```python
PropertyReasonCode = Literal[
    "property_quality_tier", "property_rating", "property_view", "property_parking"
]
RuleReasonCode = Literal[
    "rule_market_competitive", "rule_minimum_floor", "rule_cost_protected"
]
ReasonCode = PropertyReasonCode | RuleReasonCode

@dataclass(frozen=True)
class DecisionComponent:
    code: ReasonCode
    label: str
    impact: float
```

### `CostAggregate` — one new derived field

`property_decision_components: tuple[DecisionComponent, ...] = ()` — computed once in Stage A from the four raw attributes (mirrors how `property_attribute_factor` is already resolved once in Stage A, never re-derived in Stage B).

### `price_decision.v1` — two new `decision_components` fields

| Field | Type | Description |
|---|---|---|
| `calculation.decision_components` | `array` (min 1) | 4 property components + 1 rule component, in that order, for the `stay_length = 1` decision. |
| `calculation.los_floor_matrix[].decision_components` | `array` (min 1) | Exactly 1 entry: that candidate's own rule component. Never repeats the property components (pre-spec §E). |

Each entry: `{code: ReasonCode, label: string, impact: number}`.

### Reason codes and `impact` units

| `code` | Emitted when | `impact` unit |
|---|---|---|
| `property_quality_tier` | Always (one of the 4 property components) | Signed adjustment fraction from `QUALITY_TIER_ADJUSTMENTS` (e.g. `+0.30` for `luxury`) |
| `property_rating` | Always | `(rating - RATING_BASELINE) × RATING_WEIGHT` |
| `property_view` | Always | `VIEW_ADJUSTMENT` if `has_view` else `0.0` |
| `property_parking` | Always | `PARKING_ADJUSTMENT` if `has_parking` else `0.0` |
| `rule_market_competitive` | `rule_applied == "market_competitive"` | `market_reference_price_eur - minimum_price_eur` (≥ 0, headroom in EUR) |
| `rule_minimum_floor` | `rule_applied == "minimum_floor"` | `minimum_price_eur - market_reference_price_eur` (> 0, EUR above market) |
| `rule_cost_protected` | `rule_applied == "cost_protected"` | `minimum_price_eur - property_reference_price_eur` (> 0, EUR above the property's own reference) |

All four `property_*` codes are always present, even at `impact = 0.0` (e.g. `quality_tier = "standard"`, `has_view = false`) — a deterministic, always-4-entry property block, not a variable-length list that silently omits no-op attributes (pre-spec §B).

---

## 4. Formula

`property_attributes.py`:

```python
def property_attribute_components(quality_tier, rating, has_view, has_parking) -> list[DecisionComponent]:
    return [
        DecisionComponent(
            code="property_quality_tier",
            label=f"Quality tier '{quality_tier}' ({QUALITY_TIER_ADJUSTMENTS[quality_tier]:+.0%})",
            impact=QUALITY_TIER_ADJUSTMENTS[quality_tier],
        ),
        DecisionComponent(
            code="property_rating",
            label=f"Rating {rating} vs baseline {RATING_BASELINE}",
            impact=round((rating - RATING_BASELINE) * RATING_WEIGHT, 4),
        ),
        DecisionComponent(
            code="property_view",
            label="Has view" if has_view else "No view",
            impact=VIEW_ADJUSTMENT if has_view else 0.0,
        ),
        DecisionComponent(
            code="property_parking",
            label="Has parking" if has_parking else "No parking",
            impact=PARKING_ADJUSTMENT if has_parking else 0.0,
        ),
    ]

def property_attribute_factor(quality_tier, rating, has_view, has_parking) -> float:
    adjustment = sum(
        c.impact for c in property_attribute_components(quality_tier, rating, has_view, has_parking)
    )
    return round(min(max(1 + adjustment, MIN_FACTOR), MAX_FACTOR), 4)
```

`pricing.py`:

```python
def rule_decision_component(rule_applied, minimum_price_eur, market_reference_price_eur, property_reference_price_eur) -> DecisionComponent:
    if rule_applied == "market_competitive":
        impact = round(market_reference_price_eur - minimum_price_eur, 2)
        return DecisionComponent(
            code="rule_market_competitive",
            label=f"Market reference price ({market_reference_price_eur}) clears the cost floor ({minimum_price_eur}) by {impact} EUR",
            impact=impact,
        )
    if rule_applied == "minimum_floor":
        impact = round(minimum_price_eur - market_reference_price_eur, 2)
        return DecisionComponent(
            code="rule_minimum_floor",
            label=f"Cost floor ({minimum_price_eur}) exceeds market reference ({market_reference_price_eur}) by {impact} EUR but stays within property reference ({property_reference_price_eur})",
            impact=impact,
        )
    impact = round(minimum_price_eur - property_reference_price_eur, 2)
    return DecisionComponent(
        code="rule_cost_protected",
        label=f"Cost floor ({minimum_price_eur}) exceeds property reference price ({property_reference_price_eur}) by {impact} EUR",
        impact=impact,
    )
```

`decide_price()` (additions only — the floor/rule logic from ADR-0009/ADR-0011 backlog #6/#1 is unchanged):

```python
rule_component = rule_decision_component(rule_applied, minimum_price_eur, market_reference_price_eur, property_reference_price_eur)
decision_components = [*property_decision_components, rule_component]
```

`decide_price_los_matrix()` is unchanged except that its per-candidate `decide_price()` calls never pass `property_decision_components` — each candidate's `decision_components` is therefore exactly `[rule_component]` by construction, not by a second code path.

### Worked example (reusing Phase 8's two-apartment comparison)

Apartment A (`quality_tier="standard"`, `rating=4.0`, no view/parking, `property_attribute_factor=1.0`) vs. Apartment B (`quality_tier="luxury"`, `rating=4.8`, view + parking, `property_attribute_factor=1.47`), same `avg_nightly_rate_eur=200.00`, `competitiveness_discount=0.05`, both with `minimum_price_eur=210.00`:

| | Apartment A | Apartment B |
|---|---|---|
| `property_reference_price_eur` | `200.00` | `294.00` |
| `market_reference_price_eur` | `190.00` | `279.30` |
| `rule_applied` | `cost_protected` | `market_competitive` |
| `decision_components[0..3]` (property) | `quality_tier +0%`, `rating +0.0%`, `no view +0%`, `no parking +0%` | `quality_tier +30%`, `rating +8.0%`, `has view +5%`, `has parking +4%` |
| `decision_components[4]` (rule) | `rule_cost_protected`, `impact = 210.00 - 200.00 = 10.00` | `rule_market_competitive`, `impact = 279.30 - 210.00 = 69.30` |

Same cost floor, genuinely different rule components and property breakdown — the explainability payoff this phase adds on top of Phase 8's plain scalars.

---

## 5. Acceptance criteria

- **AC-01 — `property_attribute_components()` is correct per attribute.** Unit-tested for each of the four attributes independently (quality tier across all four values, rating above/below/at baseline, view/parking true/false) — same matrix Phase 8's AC-01 already used for `property_attribute_factor()`, now against the component breakdown.
- **AC-02 — `property_attribute_factor()`'s refactor is a pure regression.** Every value `property_attribute_factor()` returned before this phase (Phase 8's existing test suite) returns identically after being rewritten to sum `property_attribute_components()`'s impacts.
- **AC-03 — `rule_decision_component()` is correct per branch.** Unit-tested for all three `rule_applied` values, asserting both `code` and the exact signed `impact` gap.
- **AC-04 — `decide_price()`'s `decision_components` assembles correctly.** Default (`property_decision_components=()`) yields exactly `[rule_component]`; passing 4 property components yields exactly `[*property_components, rule_component]` in that order.
- **AC-05 — LOS candidates never carry property components.** Every `LosFloorCandidate.decision_components` returned by `decide_price_los_matrix()` has exactly 1 entry, and it is a `rule_*`-coded component — never a `property_*` one.
- **AC-06 — `price_decision.v1` validates both `decision_components` locations.** Contract test covers `calculation.decision_components` and `calculation.los_floor_matrix[].decision_components` presence, `minItems: 1`, and the closed `code` enum.
- **AC-07 — lakehouse-consumer handles the list-nested-in-list shape.** `iceberg_writer.py`'s migration test extends to a table missing `decision_components` in both locations (top-level and nested inside `los_floor_matrix`'s struct); `transform.py` defaults both to `[]` for records predating this phase; a `dynamodb_sink.py` regression test confirms a `PriceDecision` with populated nested `decision_components` inside `los_floor_matrix` serializes without raising (the one genuinely new shape per pre-spec §F).
- **AC-08 — dbt marts carry both new columns.** `fct_price_decision` exposes `calculation.decision_components` and the nested `los_floor_matrix[].decision_components`, verified by `dbt run` + `dbt test` against updated fixtures.
- **AC-09 — Live LocalStack verification (required, not optional).** A real seeded apartment with non-neutral attributes produces a DynamoDB `price_decision` item whose `calculation.decision_components` has exactly 5 entries matching `property_attributes.py`'s actual weights for that apartment, and whose 5 `los_floor_matrix` candidates each carry exactly 1 rule-only component — read directly from DynamoDB and/or via `dbt show` on `fct_price_decision`, not inferred from unit tests alone.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `property_attribute_components()`/`property_attribute_factor()` (AC-01, AC-02), `rule_decision_component()` (AC-03), `decide_price()`/`decide_price_los_matrix()`'s assembly (AC-04, AC-05) — same kind of test `pricing.py`/`property_attributes.py` already have.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-06).
- **Infrastructure-adjacent, still no LocalStack:** `lakehouse-consumer`'s schema-migration and transform-defaults tests (AC-07), using the same `SqlCatalog`-backed fixture pattern Phase 9's `test_iceberg_writer.py` already established.
- **dbt:** `dbt run`/`dbt test` against updated seed fixtures (AC-08).
- **Live LocalStack (required, AC-09):** full bring-up per `docs/manual/MANUAL.md`, verify a real apartment's `decision_components` against `property_attributes.py`'s weights computed by hand for that apartment's actual seeded attributes.

---

## 7. Known limitations

- **`impact`'s unit is contextual to `code`'s family** (a signed fraction for `property_*`, a EUR gap for `rule_*`) — not a single consistent unit across the whole list. Documented explicitly (§3) rather than forcing an artificial common unit that would lose meaning for one family or the other.
- **Reason codes and weights are still code constants**, not runtime-configurable — same limitation Phase 8/9 already accepted for their own constants; genuine runtime configurability is backlog #7 (layered RM engine) territory.
- **Only Property Bonus/Malus and the fired rule are explained** — cost/margin/commission terms are not decomposed (§2, explicitly rejected in the pre-spec as redundant with existing fields).

---

## 8. Follow-ups

- **Backlog #7 (layered RM engine)** — once pricing becomes a composable pipeline of structural/market/performance/etc. layers, each layer's own contribution becomes a natural new `ReasonCode` family in this same `decision_components` list, rather than a new container.
- **Backlog #9 (Manual Override + audit trail)** — an override's own justification becomes another `ReasonCode` family appended to this list, at the point a write path for overrides exists.
- **Phase 6 dashboard** — rendering `decision_components` as a human-readable "why this price" breakdown is a natural follow-up once this data exists in `fct_price_decision`.
- **`docs/post-poc-roadmap.md`** — backlog #4 moves from "Next" to "done" (this spec); #5/#7/#9 remain "Later" but are now unblocked on their shared prerequisite.

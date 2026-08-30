# Phase 13 — Layered Revenue Management engine

**Status:** Draft
**Depends on:** ADR-0009 (floor formula, antelación tiers), Phase 8 (Property Bonus/Malus), Phase 10 (Decision Components), Phase 11 (commission-base netting), Phase 12 (floor_policy)
**Blocks:** Backlog #11 (real market data) has a pre-cut landing spot in the Market/Performance/Inventory layers once this phase ships
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #7), [`docs/phase-13-layered-rm-engine-design-decisions.md`](../../../docs/phase-13-layered-rm-engine-design-decisions.md) (pre-spec, decisions A–H), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #7, §6 the libs/pricing-formulas extraction this phase fulfills)

---

## 1. Executive summary

`streaming/flink-jobs/src/flink_jobs/pricing.py` computes a real price today, but as one function with an inline if/elif chain — five of the external spec's seven named RM layers (Structural, Market, Booking-Window, Commercial, Guardrails) already exist as *logic*, just not as named, independently-testable units. This phase reorganizes that logic into seven explicit layer modules inside a new shared package, `libs/pricing-formulas`, orchestrated by a thin `engine.decide_price()` — and adds the two layers with no anchor anywhere in this repo today (Performance, Inventory) as real, wired, permanently-neutral stubs.

**This is a pure internal reorganization plus two inert stubs — not new pricing behavior.** Structural, Booking-Window, Commercial, and Guardrails are structurally and functionally unchanged (moved, not rewritten). Market is functionally trivial today, intentionally — it's the pre-agreed landing spot for backlog #11's real market data. Performance and Inventory are structurally real (invoked, composed, unit-tested) but always return a neutral factor until backlog #11 exists.

**Done when:** every existing pricing test passes unmodified against the new module layout, two new boundary-case tests prove the rounding-order seam didn't shift `rule_applied`, the two stub layers are provably wired (not dead code), and a real seeded apartment's live `price_decision` is byte-for-byte identical before and after this refactor.

**Not in this phase:** any new pricing behavior, any new event-schema field, real logic for Performance or Inventory (backlog #11), per-channel pricing (backlog #2), the remaining 10 `concept` values in cost aggregation (backlog #3 remainder), manual overrides (backlog #9).

---

## 2. Scope

### In scope

- **New package `libs/pricing-formulas`** (hatchling, src-layout, zero external dependencies, `py.typed` — same shape as `libs/lakehouse-shared`):
  - `decision_components.py` — moved verbatim from `flink_jobs/decision_components.py`.
  - `layers/structural.py` — moved verbatim from `flink_jobs/property_attributes.py`, plus a new `property_reference_price(avg_nightly_rate_eur, property_attribute_factor) -> float` (pure extraction of `decide_price()`'s former inline multiplication).
  - `layers/market.py` — new: `market_reference_price(property_reference_price_eur, competitiveness_discount) -> float`.
  - `layers/performance.py` — new stub: `performance_layer() -> float`, always `1.0`, zero parameters.
  - `layers/inventory.py` — new stub: `inventory_layer() -> float`, always `1.0`, zero parameters.
  - `layers/commercial.py` — moved verbatim from `pricing.py`: `CommissionBase`, `netted_commission_amount()`, `commission_base_netting_component()`.
  - `layers/booking_window.py` — extracted from `pricing.py`'s if/elif/else: `FloorType`, `FloorPolicy`, the antelación thresholds, `floor_policy_for()`, and a new `booking_window_floor(...) -> BookingWindowResult{floor_type, floor_policy, minimum_price_eur}`.
  - `layers/guardrails.py` — extracted from `pricing.py`'s final rule selection: `RuleApplied`, `rule_decision_component()` (moved verbatim), and a new `apply_guardrails(...) -> GuardrailsResult{rule_applied, suggested_price_eur, below_market_by, rule_component}`.
  - `engine.py` — `decide_price()`/`decide_price_los_matrix()`/`PriceCalculation`/`LosFloorCandidate`/`LOS_CANDIDATES`, orchestrating the 7 layers. External signature and behavior are a pure regression of today's `flink_jobs.pricing`.
  - `tests/` — every existing test from `test_pricing.py`/`test_property_attributes.py` redistributed across `test_structural.py`, `test_market.py`, `test_performance.py`, `test_inventory.py`, `test_commercial.py`, `test_booking_window.py`, `test_guardrails.py`, `test_engine.py`, plus new boundary-case and stub-wiring tests.
- **`streaming/flink-jobs`**: delete `pricing.py`, `property_attributes.py`, `decision_components.py` and their dedicated test files; update every import site (`models.py`, `stage_cost_enrichment.py`, `stage_price_decision.py`, `test_stage_cost_enrichment.py`, `test_stage_price_decision.py`) to import from `pricing_formulas`; add the new workspace dependency (`pyproject.toml`, root workspace members, `uv.lock` regeneration).
- **`docs/post-poc-roadmap.md`** — backlog #7 marked Done; §6's extraction decision marked fulfilled.

### Out of scope

- Any change to what price gets computed for any existing input — this phase must be a pure regression for Structural/Booking-Window/Commercial/Guardrails, and Market's formula is unchanged too.
- Real logic for Performance or Inventory (backlog #11) — both stay permanently-neutral stubs.
- Relocating the `[0.5, 2.0]` attribute-factor clamp into `guardrails.py` (pre-spec §C) — it stays inside `layers/structural.py`.
- `services/market-ingestor` — its own `pricing.py`/`seasonality.py` has no import relationship with the modules moved here (confirmed by repo-wide grep); the roadmap §6 mention of "a future market-ingestor consumer" is aspirational, not a real call site today.
- Backlog #2 (per-channel pricing), #3's remainder, #9 (manual overrides), #11 (real market data) — all separate backlog items.

---

## 3. Data model

No change to any event schema, Pydantic model, Iceberg schema, or dbt model. `Calculation`/`LosFloorCandidate`/`PriceCalculation` keep every existing field, type, and semantic, unchanged.

### Layer module map

| Module | Owns | Invoked from |
|---|---|---|
| `layers/structural.py` | `property_attribute_factor`, `property_attribute_components`, `property_reference_price` | `property_attribute_factor`/`property_attribute_components`: Stage A (`stage_cost_enrichment.py`), once per apartment per segment-broadcast update. `property_reference_price`: `engine.decide_price()`, per decision. |
| `layers/market.py` | `market_reference_price` | `engine.decide_price()` |
| `layers/performance.py` | `performance_layer` (stub) | `engine.decide_price()` |
| `layers/inventory.py` | `inventory_layer` (stub) | `engine.decide_price()` |
| `layers/commercial.py` | `CommissionBase`, `netted_commission_amount`, `commission_base_netting_component` | Stage B (`stage_price_decision.py`'s `_build_price_decision()`), per decision — unchanged call site |
| `layers/booking_window.py` | `FloorType`, `FloorPolicy`, `floor_policy_for`, `booking_window_floor` | `engine.decide_price()` |
| `layers/guardrails.py` | `RuleApplied`, `rule_decision_component`, `apply_guardrails` | `engine.decide_price()` |

Structural is the one layer split across two call sites by design (pre-spec
§A/H) — its factor is resolved upstream, once per apartment/segment update,
not per decision; `property_reference_price()` (the per-decision half) lives
in the same module for cohesion but is called from `engine.py`.

---

## 4. Formula

No formula changes. `engine.decide_price()` reproduces the exact arithmetic
`flink_jobs.pricing.decide_price()` has today, restructured as:

```python
def decide_price(..., property_attribute_factor=1.0, stay_length=1,
                  commission_netting_eur=0.0, property_decision_components=(),
                  commission_decision_components=()) -> PriceCalculation:
    one_time_cost_per_night_eur = one_time_cost_eur / stay_length

    booking_window = booking_window_floor(
        days_to_arrival, fixed_cost_eur, variable_cost_eur,
        one_time_cost_per_night_eur, target_margin, commission_pct,
        commission_netting_eur,
    )

    property_reference_price_eur = property_reference_price(
        avg_nightly_rate_eur, property_attribute_factor
    )
    # Backlog #7 stubs — multiplying by 1.0 is a no-op today, becomes real
    # signal once backlog #11 lands real market/comp-set data.
    property_reference_price_eur *= performance_layer() * inventory_layer()

    market_reference_price_eur = market_reference_price(
        property_reference_price_eur, competitiveness_discount
    )

    guardrails = apply_guardrails(
        booking_window.minimum_price_eur, market_reference_price_eur,
        property_reference_price_eur,
    )

    total_cost_eur = fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
    effective_margin = (
        (guardrails.suggested_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )
    decision_components = [
        *property_decision_components, *commission_decision_components,
        guardrails.rule_component,
    ]
    return PriceCalculation(
        minimum_price_eur=round(booking_window.minimum_price_eur, 2),
        floor_type=booking_window.floor_type,
        floor_policy=booking_window.floor_policy,
        property_attribute_factor=property_attribute_factor,
        property_reference_price_eur=round(property_reference_price_eur, 2),
        market_reference_price_eur=round(market_reference_price_eur, 2),
        rule_applied=guardrails.rule_applied,
        suggested_price_eur=round(guardrails.suggested_price_eur, 2),
        below_market_by=round(guardrails.below_market_by, 2),
        effective_margin=round(effective_margin, 4),
        decision_components=decision_components,
    )
```

`apply_guardrails()` preserves the exact original rounding order (pre-spec
§F): comparisons on unrounded values, rounded copies only for
`rule_decision_component()`'s label arguments.

`total_cost_eur`/`effective_margin` stay in `engine.py` — a derived
reporting metric, not one of the external spec's seven named layers.

`decide_price_los_matrix()` is unchanged in shape: loops `decide_price()`
per stay length, still deliberately not forwarding
`property_decision_components`/`commission_decision_components` (guarded by
an existing test, moved verbatim — not "fixed").

---

## 5. Acceptance criteria

- **AC-01 — Full regression.** Every test currently in `test_pricing.py` (32 tests) and `test_property_attributes.py`, moved with unchanged assertions (only import paths updated) into `libs/pricing-formulas/tests/`, passes unmodified.
- **AC-02 — Per-layer isolation.** Each of the 7 layers has its own dedicated unit-test module, testing that layer's function(s) without going through `engine.py`.
- **AC-03 — Pipeline-level regression.** `engine.decide_price()`/`decide_price_los_matrix()` produce `PriceCalculation`/`LosFloorCandidate` field-for-field identical to the pre-refactor functions across the full moved test matrix, including `decision_components` ordering and the LOS-matrix's non-forwarding behavior.
- **AC-04 — Rounding-order boundary cases (new).** Two new tests construct inputs where `minimum_price_eur` sits within 0.005 EUR of `market_reference_price_eur` and of `property_reference_price_eur` respectively, confirming `rule_applied` selection is unchanged from unrounded-comparison behavior.
- **AC-05 — Stub wiring is provable.** `unittest.mock.patch("pricing_formulas.engine.performance_layer", return_value=X)` (and the `inventory_layer` equivalent) changes `property_reference_price_eur`/`suggested_price_eur` proportionally — proof the multiplication is reachable code, not dead.
- **AC-06 — mypy stays fully strict.** `libs/pricing-formulas` passes strict mypy with zero new entries in root `pyproject.toml`'s `[[tool.mypy.overrides]]`.
- **AC-07 — Lint clean.** `ruff check .`/`ruff format --check .` pass repo-wide.
- **AC-08 — No dangling references.** A repo-wide grep for `flink_jobs.pricing`, `flink_jobs.property_attributes`, `flink_jobs.decision_components` returns zero hits.
- **AC-09 — Zero schema diff.** No changes to `specs/events/price_decision.v1.json`, `libs/shared-schemas`, `services/lakehouse-consumer`'s schema/transform, or any `transform/*.sql` model — confirmed via `git diff --stat` scoped to those paths.
- **AC-10 — Live LocalStack verification (required, regression-framed).** Against a real seeded apartment, the resulting DynamoDB `price_decision` item is byte-for-byte identical to a pre-refactor capture of the same seed — every `Calculation`/`Output`/`los_floor_matrix`/`decision_components` field. Flink REST `/jobs/<id>/exceptions` and raw TaskManager logs both show zero exceptions.
- **AC-11 — Roadmap updated.** `docs/post-poc-roadmap.md` backlog #7 marked Done, cross-referenced to this spec.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** all 7 layer modules' own unit tests (AC-02), `engine.py`'s pipeline-assembly tests including the moved 32 + new boundary cases (AC-01, AC-03, AC-04), the stub-wiring mock tests (AC-05).
- **Static/tooling:** mypy (AC-06), ruff (AC-07), grep-based dangling-reference check (AC-08), `git diff --stat` schema-diff check (AC-09).
- **Live LocalStack (required, AC-10):** full bring-up per `docs/manual/MANUAL.md`; capture one real apartment's `price_decision` before deploying this phase's code, redeploy, capture again, diff the two JSON payloads field-by-field.

---

## 7. Known limitations

- **The external spec's actual layer semantics are unknown** (pre-spec §A) — this phase's Market/Performance/Inventory design is this project's own inference, not a faithful reproduction of an unavailable source document. If the original document ever surfaces, these three layers are the ones most likely to need revision.
- **Guardrails has two plausible anchors in this repo** (pre-spec §C); this phase treats the final floor-vs-market arbitration as canonical and leaves the attribute-factor clamp inside Structural. A future reader auditing "why isn't the clamp in guardrails.py" should read this note, not assume an oversight.
- **Performance and Inventory add no new pricing behavior** (pre-spec §E/H) — their value is the seam and the proven wiring, not new logic. Backlog #11 is required before either does anything.
- **Market is functionally trivial today** — one multiplication, no `DecisionComponent`. Its value is being the pre-agreed landing spot for backlog #11's real market data (pre-spec §D).

---

## 8. Follow-ups

- **Backlog #11 (real market data / comp-set)** — the natural consumer of the seams this phase cuts: `layers/market.py` gains real `occupancy_rate`/`sample_size`-driven logic; `layers/performance.py` and `layers/inventory.py` gain real parameters and logic together (never speculatively ahead of the data, per this phase's own discipline). Inventory specifically will need a new broadcast CDC source, chained after Stage A2 the same way `owner_contracts` was (PyFlink's `connect()` accepts one broadcast stream per call).
- **`docs/post-poc-roadmap.md`** — backlog #7 moves from "Later" to "Done" (this spec); §6's extraction decision is fulfilled.

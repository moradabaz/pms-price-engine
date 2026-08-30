# Phase 8 — Property Bonus/Malus (Property Reference Price)

**Status:** Draft
**Depends on:** Phase 1 (`apartment_market_segments`), Phase 4 (Stage A/B, `decide_price()`)
**Blocks:** Nothing downstream depends on this yet — it's the first backlog item picked up from `docs/post-poc-roadmap.md`
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #6), [ADR-0009](../../../docs/adr/ADR-0009-profitability-floor-reform.md) (the floor formula this phase extends), [`docs/phase-8-property-bonus-malus-design-decisions.md`](../../../docs/phase-8-property-bonus-malus-design-decisions.md) (pre-spec, decisions A–F), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #6, §5 renumbered to Phase 9 for LOS)

---

## 1. Executive summary

Every apartment in the same market segment (`city`, `neighborhood`, `property_type`, `bedrooms`) currently receives an **identical** market reference price — `avg_nightly_rate_eur` comes from `market-ingestor`'s 18 static segments, shared by every apartment mapped to that segment. Nothing distinguishes a well-reviewed apartment with a view and parking from a plain one in the same segment.

This phase closes that gap with a scoped version of the source spec's Property Bonus/Malus model (ADR-0011): four new per-apartment attributes (`quality_tier`, `rating`, `has_view`, `has_parking`) combine, via configurable weights, into a **Property Attribute Factor** that adjusts the segment's raw market rate into a **Property Reference Price** — `avg_nightly_rate_eur × factor` — before that adjusted price is used anywhere in the floor/competitiveness comparison.

**Done when:** two apartments in the same segment, same cost structure, but different attributes, produce genuinely different `rule_applied`/`suggested_price_eur` outcomes for the same market snapshot — provably, not just structurally possible.

**Not in this phase:** the four raw attributes are not forwarded into `price_decision.v1` or `dim_apartment` (Postgres-only, same limitation already accepted for `property_type`/`bedrooms`, spec 05 §13); no layered Revenue Management engine (backlog #7); no Decision Components/reason codes (backlog #4) — this phase produces two new scalar fields, not a structured breakdown.

---

## 2. Scope

### In scope

- `specs/phases/01-mock-app-db/apartment_market_segments.sql` + `services/mock-pm-app/src/mock_pm_app/migrations.py` — four new columns: `quality_tier`, `rating`, `has_view`, `has_parking` (kept byte-identical between the two files, same convention this table already follows for `commission_pct`).
- `services/mock-pm-app/src/mock_pm_app/data.py` — `Apartment` dataclass gains the four fields; `build_apartment_pool()` takes a `random.Random` and assigns plausible synthetic values per apartment.
- `services/mock-pm-app/src/mock_pm_app/seed.py` — `seed_apartment_market_segments()` inserts the four new columns.
- `streaming/flink-jobs/src/flink_jobs/models.py` — `ApartmentSegmentRow`/`SegmentAssignment` gain the four raw attributes; `CostAggregate` gains one new **derived** field, `property_attribute_factor: float`.
- `streaming/flink-jobs/src/flink_jobs/property_attributes.py` (new) — the weighting function and its named constants (pre-spec §B).
- `streaming/flink-jobs/src/flink_jobs/stage_cost_enrichment.py` — `CostEnrichmentFunction.process_element` calls the new function once per cost update, attaches the factor to the emitted `CostAggregate`.
- `streaming/flink-jobs/src/flink_jobs/pricing.py` — `decide_price()` gains `property_attribute_factor: float = 1.0`; computes `property_reference_price_eur`; `market_reference_price_eur`, `rule_applied`, and `below_market_by` are now relative to it instead of raw `avg_nightly_rate_eur` (pre-spec §D).
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` — `_build_price_decision()` passes `cost.property_attribute_factor` through to `decide_price()` and into the new `Calculation` fields.
- `libs/shared-schemas/src/shared_schemas/price_decision.py` + `specs/events/price_decision.v1.json` — `Calculation` gains `property_attribute_factor` and `property_reference_price_eur` (additive, no `schema_version` bump).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through the two new columns (pure schema evolution).
- `specs/contracts/test_price_decision_contract.py` + fixtures — updated for the two new required fields.
- Unit tests: `property_attributes.py`'s weighting function (pure, all four attributes independently), `decide_price()`'s new parameter (default `1.0` reproduces every existing ADR-0009 worked example unchanged; a non-1.0 factor changes `rule_applied` for a case that was previously borderline).

### Out of scope (explicitly deferred)

- **Forwarding the four raw attributes into `price_decision.v1`/`dim_apartment`.** Pre-existing limitation (spec 05 §13) for `property_type`/`bedrooms`; this phase doesn't fix it for these four either — same reasoning, not this phase's job.
- **A layered Revenue Management engine** (backlog #7) — the factor computed here is one flat multiplier, not a composable pipeline of structural/market/performance/etc. layers.
- **Decision Components / structured reason codes** (backlog #4) — `property_attribute_factor` is one more scalar in `calculation`, not a breakdown of which attribute contributed how much. A future Decision Components phase can decompose this later using the same weights.
- **More attributes than the four listed** (surface, terrace, pool, elevator, AC, workspace, distance to POIs, neighborhood attractiveness — all present in the source spec, §6.1/§6.2). Deferred as a "Bonus/Malus v2" if the four-attribute version proves the concept.
- **Making weights configurable at runtime** (e.g. a database table an operator edits) — weights are code constants for this phase, per the pre-spec's "configurable, not hardcoded" principle applied at the level of "named and documented," not "editable without a deploy." A runtime-configurable version is natural backlog #7 territory (the same rule-versioning concern that engine needs anyway).

---

## 3. Data model

### `apartment_market_segments` — four new columns

| Column | Type | Constraint | Default |
|---|---|---|---|
| `quality_tier` | `TEXT` | `CHECK (quality_tier IN ('basic','standard','premium','luxury'))` | `'standard'` |
| `rating` | `NUMERIC(2,1)` | `CHECK (rating >= 1.0 AND rating <= 5.0)` | `4.0` |
| `has_view` | `BOOLEAN` | — | `false` |
| `has_parking` | `BOOLEAN` | — | `false` |

Added via `ADD COLUMN IF NOT EXISTS`, same pattern ADR-0009 already established for `commission_pct` — a volume from before this phase still picks up the columns with sane defaults on next `mock-pm-app` startup.

### `CostAggregate` — one new derived field

`property_attribute_factor: float` — computed once in Stage A from the four raw attributes (never re-derived in Stage B). `SegmentAssignment`/`ApartmentSegmentRow` carry the four raw attributes (mirroring how they already carry `city`/`neighborhood`/`property_type`/`bedrooms`); `CostAggregate` carries only the resolved factor (mirroring how it already carries resolved `target_margin`/`competitiveness_discount`/`commission_pct`, not raw source rows).

### `price_decision.v1` — two new fields on `calculation`

| Field | Type | Description |
|---|---|---|
| `property_attribute_factor` | `number` | The multiplier applied to `avg_nightly_rate_eur` for this apartment (pre-spec §B/§D). |
| `property_reference_price_eur` | `number` | `avg_nightly_rate_eur × property_attribute_factor` — this apartment's market reference, before the competitiveness discount. |

`market_reference_price_eur`'s description updates to "`property_reference_price_eur × (1 - competitiveness_discount)`" (was: "`avg_nightly_rate_eur × (1 - competitiveness_discount)`"). `output.below_market_by`'s description updates to "`property_reference_price_eur - suggested_price_eur`" (was: "`avg_nightly_rate_eur - suggested_price_eur`").

---

## 4. Formula

`streaming/flink-jobs/src/flink_jobs/property_attributes.py` (new):

```
QUALITY_TIER_ADJUSTMENTS = {"basic": -0.15, "standard": 0.00, "premium": 0.15, "luxury": 0.30}
RATING_BASELINE = 4.0
RATING_WEIGHT = 0.10
VIEW_ADJUSTMENT = 0.05
PARKING_ADJUSTMENT = 0.04
MIN_FACTOR = 0.5
MAX_FACTOR = 2.0

def property_attribute_factor(quality_tier, rating, has_view, has_parking) -> float:
    adjustment = QUALITY_TIER_ADJUSTMENTS[quality_tier]
    adjustment += (rating - RATING_BASELINE) * RATING_WEIGHT
    adjustment += VIEW_ADJUSTMENT if has_view else 0.0
    adjustment += PARKING_ADJUSTMENT if has_parking else 0.0
    return clamp(1 + adjustment, MIN_FACTOR, MAX_FACTOR)
```

`streaming/flink-jobs/src/flink_jobs/pricing.py` (`decide_price()`, additions only — the floor-side formula from ADR-0009 is unchanged):

```
property_reference_price_eur = avg_nightly_rate_eur * property_attribute_factor
market_reference_price_eur   = property_reference_price_eur * (1 - competitiveness_discount)

if minimum_price_eur <= market_reference_price_eur:
    rule_applied = "market_competitive"; suggested_price_eur = market_reference_price_eur
elif minimum_price_eur <= property_reference_price_eur:
    rule_applied = "minimum_floor"; suggested_price_eur = minimum_price_eur
else:
    rule_applied = "cost_protected"; suggested_price_eur = minimum_price_eur

below_market_by = property_reference_price_eur - suggested_price_eur
```

(Every occurrence of raw `avg_nightly_rate_eur` in the old comparison logic is replaced by `property_reference_price_eur`; `avg_nightly_rate_eur` itself is unchanged as an input and still reported as-is in `market_inputs`.)

### Worked example

Two apartments, same segment (Barcelona/Eixample, apartment, 1 bedroom), same cost structure, same `avg_nightly_rate_eur = 200.00`, same `competitiveness_discount = 0.05`:

| | Apartment A | Apartment B |
|---|---|---|
| `quality_tier` | `standard` | `luxury` |
| `rating` | `4.0` | `4.8` |
| `has_view` | `false` | `true` |
| `has_parking` | `false` | `true` |
| Adjustment | `0.00 + 0.00 + 0 + 0 = 0.00` | `0.30 + 0.08 + 0.05 + 0.04 = 0.47` |
| `property_attribute_factor` | `1.00` | `1.47` |
| `property_reference_price_eur` | `200.00` | `294.00` |
| `market_reference_price_eur` | `190.00` | `279.30` |

If both apartments' cost floor (`minimum_price_eur`) sits at `210.00`: Apartment A gets `rule_applied = "cost_protected"`, `suggested_price_eur = 210.00` — its floor exceeds even its own (unboosted) market reference of `200.00`, so its costs are pricing it out of its own market. Apartment B gets `rule_applied = "market_competitive"`, `suggested_price_eur = 279.30` — its boosted market reference (`294.00`) comfortably clears the same floor, with room to spare. Same cost, same raw market rate, genuinely different outcomes — this is the phase's entire point.

---

## 5. Acceptance criteria

- **AC-01 — `property_attribute_factor()` is correct and bounded.** Unit-tested for each attribute independently (quality tier across all four values, rating above/below/at baseline, view/parking true/false) and for the clamp at both ends.
- **AC-02 — `decide_price()`'s default preserves every existing ADR-0009 worked example.** `property_attribute_factor=1.0` (the default) reproduces `pricing.py`'s current test suite unchanged — this is the regression guarantee for the phase.
- **AC-03 — A non-1.0 factor changes the outcome.** At least one test case where the same cost/market inputs produce a different `rule_applied` purely from varying `property_attribute_factor` (the worked example in §4, as a test).
- **AC-04 — The four columns exist and default correctly on an existing volume.** `ADD COLUMN IF NOT EXISTS` verified against a pre-Phase-8 volume, same check ADR-0009 already established for `commission_pct`.
- **AC-05 — `price_decision.v1`'s new fields validate.** Contract test (`specs/contracts/test_price_decision_contract.py`) covers `property_attribute_factor`/`property_reference_price_eur` presence and range.
- **AC-06 — dbt marts carry the two new columns.** `fct_price_decision` exposes `property_attribute_factor`/`property_reference_price_eur`, verified by `dbt run` + `dbt test` against updated fixtures.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `property_attribute_factor()` (AC-01), `decide_price()`'s new branch behavior (AC-02, AC-03) — the same kind of test `pricing.py`/`cost_aggregation.py` already have.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-05), `ADD COLUMN IF NOT EXISTS` behavior against a volume fixture (AC-04).
- **dbt:** `dbt run`/`dbt test` against updated seed fixtures for the two new columns (AC-06) — no new dbt model needed, existing staging/marts pass the columns through.
- **Not required for this phase:** a live end-to-end LocalStack run — the change is additive and covered by unit/contract tests; a full local stack run remains good practice before merging but isn't a separate acceptance criterion here (same bar Phase 3's smaller changes used).

---

## 7. Known limitations

- **The four attributes are synthetic**, assigned plausibly by `mock-pm-app`, not derived from any real property data — same caveat every other synthetic field in this project already carries (`docs/manual/MANUAL.md`'s own framing).
- **Raw attributes don't reach the lakehouse** (§2, out of scope) — anyone querying `fct_price_decision` sees the resolved factor, not which attribute drove it. Revisit once Decision Components (backlog #4) exists.
- **Weights are code constants**, not runtime-configurable — changing them requires a deploy, same as `REDUCED_MARGIN_FACTOR` today. Acceptable for a PoC; the source spec's "configurable, not hardcoded" principle is satisfied at the "named and documented" level, not "editable without a deploy."
- **Only four of the source spec's many attributes are modeled** — a deliberate scope cut (§2), not an oversight; more can be added later without changing the combination method.

---

## 8. Follow-ups

- **Backlog #4 (Decision Components)** would let this phase's single `property_attribute_factor` scalar be decomposed into named reason codes (e.g. `BONUS_LUXURY_TIER`, `BONUS_VIEW`) — natural next step once that container exists.
- **Backlog #7 (layered RM engine)** is where runtime-configurable weights would actually make sense — this phase's constants become that engine's first "Structural" layer rule.
- **The `property_type`/`bedrooms` lakehouse gap** (spec 05 §13) and this phase's own raw-attribute gap are now the same shape — worth fixing together if either is ever picked up, rather than twice.
- **`docs/post-poc-roadmap.md`** — backlog #6 is now "in progress" (this spec), not just "Later"; LOS-aware floor is renumbered to Phase 9.

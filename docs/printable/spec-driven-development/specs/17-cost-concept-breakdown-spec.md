# Phase 17 — Cost Concept Breakdown

**Status:** Implemented — unit-tested (`cost_aggregation.py`, `transform.py`, `dashboard/marts.py`), contract-tested against all 6 valid `price_decision.v1` fixtures, and live-verified against a running LocalStack stack on 2026-09-12: `cost_inputs.cost_breakdown` confirmed flowing end-to-end (Flink → DynamoDB → DynamoDB Streams → lakehouse-consumer → Iceberg → dbt → dashboard) on real accumulated data — 86 `fct_price_decision` rows, `cost_breakdown` populated with real per-concept groupings (e.g. `electricity`, `ota_fee`, `cleaning`, `maintenance`, `insurance`, each with a plausible EUR/day amount).
**Depends on:** Phase 11 (`OTA_RELATED_CONCEPTS`/`CLEANING_CONCEPTS`, the precedent this phase generalizes), Phase 16 (`channel_price_matrix`'s "required list, sparse content" convention, reused here for `cost_breakdown`)
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #3), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #3)

---

## 1. Executive summary

`payment_line.concept` has 13 values, but `cost_aggregation.py` has only ever special-cased 2 of them (`ota_fee`/`channel_manager` → `ota_related_cost_eur`, `cleaning` → `cleaning_cost_eur`, both added by Phase 11 purely to feed commission-base netting). The other 10 (`electricity`, `water`, `gas`, `internet`, `pms_subscription`, `office_rent`, `maintenance`, `insurance`, `community_fee`, `other`) fall through undifferentiated into `fixed_cost_eur`/`variable_cost_eur`, with no way to see what actually makes up an apartment's cost base.

This phase adds `cost_inputs.cost_breakdown` — a per-concept, per-day EUR breakdown of every concept observed in the current billing period. Reporting granularity only: every amount in it is already fully counted inside `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur`, exactly the same "additional breakdown, not a new cost" relationship Phase 11 established for `ota_related_cost_eur`/`cleaning_cost_eur`. Nothing about the pricing formula changes — `decide_price()`'s signature is untouched.

**Not in this phase:** any new cost dimension beyond `concept` itself (that's backlog #13, `CostDefinition`/`CostAllocationRule` — a bigger, structural schema redesign, correctly deferred separately); company-cost imputation (#14, depends on #13); using the breakdown to change any floor/margin calculation (it's display-only, by design — see §4).

## 2. Design decision

**Where the breakdown surfaces:** a new field on `PriceDecision`, not a Flink-internal-only value and not a dbt/dashboard-only computation. Same structural precedent as Phase 9's `los_floor_matrix` and Phase 16's `channel_price_matrix` — computed once in Flink, carried through the DynamoDB/Iceberg contract, so both the hot path and the cold path see the identical number without recomputing it differently in two places.

**All 13 concepts, not just the 10 "remaining" ones:** a breakdown that silently omitted `ota_fee`/`cleaning` (because they're "already" exposed via the two Phase 11 fields) would be confusing to read — a dashboard user shouldn't have to know to look in two different places to see a night's full cost composition. `cost_breakdown` is comprehensive; `ota_related_cost_eur`/`cleaning_cost_eur` continue to exist unchanged for their own purpose (commission-base netting), and the same EUR amounts legitimately appear in both.

**Canonical order, not input order:** entries are ordered by `payment_line.v1`'s own concept enum order (`CONCEPT_ORDER` in `cost_aggregation.py`), not by whichever concept happened to appear first in a given billing period's lines. Keeps the field's output deterministic and stable for snapshot-style tests and dashboard rendering, independent of Kafka delivery order.

**Sparse, not zero-filled:** only concepts with at least one matching line in the current billing period get an entry — a concept with no cost that period has no entry, never a fabricated `{"concept": "gas", "amount_eur": 0.0}`. Same "no entry" convention Phase 16 uses for a channel with no observed rate yet.

## 3. In scope

**`streaming/flink-jobs` (`cost_aggregation.py`):**
- New `ConceptAmount` frozen dataclass (`concept: str`, `amount_eur: float`).
- New `CONCEPT_ORDER` tuple — the 13 concepts in `payment_line.v1`'s own enum order.
- `aggregate_cost()` groups `matching` (the current billing period's lines) by `concept`, sums `amount_gross` per concept, divides by `available_days` (same per-day convention `fixed_cost_eur`/`ota_related_cost_eur` already use), rounds to 2 decimals. Result ordered by `CONCEPT_ORDER`, filtered to concepts actually observed.
- `CostAggregationResult` gains `cost_breakdown: tuple[ConceptAmount, ...]`.

**`streaming/flink-jobs` (`models.py`, `stage_cost_enrichment.py`):**
- `CostAggregate` gains `cost_breakdown: tuple[ConceptAmount, ...] = field(default_factory=tuple)`, resolved in Stage A (`CostEnrichmentFunction`) alongside `ota_related_cost_eur`/`cleaning_cost_eur`, from the same `aggregate_cost()` call — no new upstream data source.

**`streaming/flink-jobs` (`stage_price_decision.py`):**
- `_build_price_decision()` maps `cost.cost_breakdown` into `CostConceptAmount` Pydantic instances when constructing `CostInputs`. Doesn't touch `decide_price()`/`decide_price_los_matrix()`/`decide_price_by_channel()` — the formula's inputs are unchanged.

**Shared schema / event contract:**
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — new `CostConcept` literal (mirrors `payment_line.v1`'s concept enum), new `CostConceptAmount` model (`extra="forbid"`); `CostInputs` gains `cost_breakdown: list[CostConceptAmount] = Field(default_factory=list)`.
- `specs/events/price_decision.v1.json` — `cost_inputs.cost_breakdown`: required array (structurally required like `channel_price_matrix`, legitimately empty), element shape `{concept, amount_eur}`.

**Lakehouse / dbt:**
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — new **required** `NestedField` list/struct group inside `cost_inputs` (field 6), IDs continuing from 89 (Phase 16's last-used ID): struct/list id 90, element id 91, leaves 92 (`concept`), 93 (`amount_eur`).
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — new `_cost_breakdown(cost_inputs)` helper, same shape as `_channel_price_matrix()` (list comprehension, `[]` when the key is absent on a pre-Phase-17 record).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `cost_inputs.cost_breakdown` unchanged (same nested-list mechanics as `channel_price_matrix`/`los_floor_matrix`).

**Dashboard:**
- New `render_cost_breakdown()` view/tab: per-apartment bar chart + table of `cost_breakdown` from the most recent decision (cold path, same `marts.py` pattern `channel_pricing()` uses). Not scoped to a single night — the breakdown is the same for every night within one billing period, so an apartment-level query (most recent decision) is the natural grain, unlike `channel_price_matrix` which genuinely varies per night.
- `src/dashboard/marts.py` — new `cost_breakdown(settings, apartment_id)` query function.

**Contracts / tests:**
- All 6 valid `specs/contracts/fixtures/price_decision/*.json` gain `cost_inputs.cost_breakdown` (populated, roughly consistent with each fixture's own `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur`/`cost_lines_count`) — `invalid_missing_required.json` untouched (it's testing a different, higher-level missing-field failure).
- Unit tests: `aggregate_cost()`'s grouping/ordering/sparse-entry behavior (`test_cost_aggregation.py`); `_cost_breakdown()`'s missing-key and populated coercion (`test_transform.py`); `marts.cost_breakdown()`'s most-recent-decision and unknown-apartment cases (`test_marts.py`).

## 4. Why display-only, not a formula input (yet)

`decide_price()` still only knows `fixed_cost_eur`/`variable_cost_eur`/`one_time_cost_eur` — three buckets, not thirteen. Feeding `cost_breakdown` into the formula (e.g. a per-concept allocation rule, or letting a specific concept opt out of the commission-base netting Phase 11 already does for `ota_fee`/`cleaning`) is explicitly what backlog #13 (`CostDefinition`/`CostAllocationRule`) is for — a genuine schema redesign of what a "cost" is, not an extension of this phase. Keeping this phase display-only avoids conflating a cheap reporting win with that bigger, structural change.

## 5. Not yet done

- Any change to `mock-pm-app`'s synthetic data generation to produce a richer concept mix — the live check (§ above) already observed 5 distinct concepts naturally; whether all 13 ever appear for a given apartment/period depends on `mock-pm-app`'s own synthetic generation, unchanged by this phase.

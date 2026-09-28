# Phase 11 — Owner Contract & Commission Base

**Status:** Draft
**Depends on:** Phase 1 (`apartment_market_segments`), Phase 2 (CDC pipeline / Debezium connector), Phase 4 (Stage A/B), Phase 10 (`decision_components`, the container this phase's netting adjustment plugs into)
**Blocks:** Nothing downstream depends on this yet
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #5), [ADR-0009](../../../docs/adr/ADR-0009-profitability-floor-reform.md) (D2, the flat `commission_pct` this phase replaces), [`docs/phase-11-owner-contract-commission-base-design-decisions.md`](../../../docs/phase-11-owner-contract-commission-base-design-decisions.md) (pre-spec, decisions A–G), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #5)

---

## 1. Executive summary

`commission_pct` (ADR-0009 D2) has always behaved as if commission is charged against the full suggested price ("Total Revenue") — an implicit assumption, never a modeled one (ADR-0011 §1). This phase makes the commission base an explicit, configurable choice, backed by a real owner/contract entity: a small pool of synthetic owners, each owning several apartments, with each apartment's own contract stating both `commission_pct` and which revenue base it applies against (`total_revenue`, `revenue_minus_ota`, or `revenue_minus_ota_minus_cleaning`).

This is the first phase since ADR-0009 that is not purely additive: `commission_pct` **moves out of** `apartment_market_segments` into a new `owner_contracts` table — a deliberate, ADR-0011-pre-authorized remodel, not a bug. It also introduces this project's second genuine CDC-broadcast join in Flink (a new `owner_contracts` stream, chained through a new Stage A2 after the existing segment-config Stage A), and the minimal slice of backlog #3 (reading `PaymentLine.concept`) that a netted commission base actually requires.

**Done when:** two apartments with identical costs, margin, and market conditions, but different `commission_base` values on their owner contract, produce genuinely different `minimum_price_eur` — provably, via a live LocalStack read showing a real seeded apartment's floor reflect its actual contract's netting, with a `commission_base_netting` decision component present when the base nets anything and absent when it doesn't.

**Not in this phase:** a general commission-base solver (ADR-0011's "solved algebraically or via a solver" — this phase supports exactly 3 named, closed bases, each with its own derived formula, not an arbitrary configurable netting expression); backlog #3's full 13-`concept` breakdown (only `ota_fee`/`channel_manager`/`cleaning` are split out — the minimal slice this phase's 3 bases need); surfacing `owner_name`/owner-level rollups in the dashboard or dbt marts (the pricing engine only needs the resolved `(commission_base, commission_pct)` pair per apartment, not the owner dimension itself); backlog #7 (layered RM engine) or #9 (manual overrides) — this phase is a leaf consumer of Decision Components (Phase 10), not their next builder.

---

## 2. Scope

### In scope

**Postgres / mock-pm-app:**
- `specs/phases/01-mock-app-db/owners.sql` (new), `owner_contracts.sql` (new) — see §3 for DDL.
- `specs/phases/01-mock-app-db/apartment_market_segments.sql` — `commission_pct` and its `CHECK` constraint are **dropped** (`ALTER TABLE ... DROP COLUMN IF EXISTS commission_pct`), not left alongside the new table.
- `services/mock-pm-app/src/mock_pm_app/migrations.py` — `ensure_owners_schema`/`ensure_owner_contracts_schema` (new, same `CREATE TABLE IF NOT EXISTS` + `ADD COLUMN IF NOT EXISTS` self-healing pattern every other table here uses); `ensure_apartment_market_segments_schema` gains the `DROP COLUMN IF EXISTS commission_pct` statement.
- `services/mock-pm-app/src/mock_pm_app/data.py` — new `Owner` dataclass + `build_owner_pool(count, rng)`; new `OwnerContract` dataclass + `build_owner_contracts(apartments, owners, rng)` assigning each apartment round-robin to an owner, then a randomized `commission_base` (weighted so `total_revenue` isn't the only value ever seen) and `commission_pct` (same `[0.10, 0.20]` plausible-range synthesis style as other numeric fields here).
- `services/mock-pm-app/src/mock_pm_app/seed.py` — `seed_owners`/`seed_owner_contracts` (+ their `already_seeded_*` checks), same shape as `seed_apartment_market_segments`.
- `services/mock-pm-app/src/mock_pm_app/main.py` — wires the two new schema-ensure calls and seed calls into the existing startup sequence.

**Debezium / infra:**
- `infra/debezium/postgres-connector.json` — `owner_contracts` added to `table.include.list`; a new `routeOwnerContracts` `RegexRouter` transform to `owner-contracts.v1`. **No new connector, slot, or publication** — extends the existing one (pre-spec §B). `owners` itself is **not** CDC-captured (nothing downstream needs it).
- `docs/manual/MANUAL.md` — a fourth manually-created Kafka topic, `owner-contracts.v1`, alongside the three this doc's bring-up sequence already creates.

**Flink (`streaming/flink-jobs`):**
- `streaming/flink-jobs/src/flink_jobs/models.py` — new `OwnerContractRow`/`OwnerContractAssignment` (mirrors `ApartmentSegmentRow`/`SegmentAssignment`'s own shape, §3). `ApartmentSegmentRow`/`SegmentAssignment` **lose** `commission_pct`. `CostAggregate` **loses** its previous unconditional `commission_pct` source (Stage A no longer resolves it) and gains: `commission_pct: float = 0.15` (now just a pre-Stage-A2 default, same pattern `property_attribute_factor`'s `1.0` default already established), `commission_base: str = "total_revenue"`, `ota_related_cost_eur: float = 0.0`, `cleaning_cost_eur: float = 0.0`.
- `streaming/flink-jobs/src/flink_jobs/cost_aggregation.py` — `CostAggregationResult` gains `ota_related_cost_eur`/`cleaning_cost_eur`, computed the same `sum(matching lines) / available_days` way as `fixed_cost_eur`/`variable_cost_eur`, filtered by `line.concept` instead of `line.cost_type` (two new named constants, `OTA_RELATED_CONCEPTS = frozenset({"ota_fee", "channel_manager"})`, `CLEANING_CONCEPTS = frozenset({"cleaning"})`).
- `streaming/flink-jobs/src/flink_jobs/stage_cost_enrichment.py` — `CostEnrichmentFunction` passes the two new sub-totals from `aggregate_cost()`'s result onto `CostAggregate`; **stops** setting `commission_pct` (falls back to the dataclass default until Stage A2 resolves it).
- `streaming/flink-jobs/src/flink_jobs/stage_owner_contract_enrichment.py` (new) — `OwnerContractEnrichmentFunction(KeyedBroadcastProcessFunction)`, Stage A2 (pre-spec §C): on `process_element` (a `CostAggregate`), looks up the broadcast owner-contract assignment for `apartment_id`; skips (no emission) if absent yet, same "skip, don't buffer" rule Stage A already follows for missing segment config; otherwise emits `dataclasses.replace(cost_aggregate, commission_base=assignment.commission_base, commission_pct=assignment.commission_pct)`. `process_broadcast_element` resolves an incoming `OwnerContractRow` into the broadcast state, keyed by `apartment_id`.
- `streaming/flink-jobs/src/flink_jobs/decision_components.py` — `CommissionReasonCode = Literal["commission_base_netting"]`; `ReasonCode` becomes the union of `PropertyReasonCode | RuleReasonCode | CommissionReasonCode` (8 values total).
- `streaming/flink-jobs/src/flink_jobs/pricing.py` — `CommissionBase = Literal["total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"]`; `decide_price()` gains `commission_base: CommissionBase = "total_revenue"`, `ota_related_cost_eur: float = 0.0`, `cleaning_cost_eur: float = 0.0`; the netting adjustment (§4) applies uniformly to all three floor tiers; a new `commission_base_netting` component is appended to `decision_components` only when the netted amount `N > 0` (pre-spec §F).
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` — passes `cost.commission_base`/`cost.ota_related_cost_eur`/`cost.cleaning_cost_eur` into the top-level `decide_price()` call; `decide_price_los_matrix()`'s per-candidate calls receive the same `commission_base`/netting inputs (the netting amount doesn't vary with `stay_length`, same reasoning Phase 10 already applied to property components — see §4) but, like property components, the resulting `commission_base_netting` component is **not** duplicated into each `LosFloorCandidate.decision_components` (which stays rule-only, per Phase 10's own established convention).
- `streaming/flink-jobs/src/flink_jobs/job.py` — new `owner_contracts` `KafkaSource`, a new `_parse_owner_contract_row` parser (with defensive defaults for CDC messages predating this phase, same pattern `_parse_apartment_segment_row` already uses), `OWNER_CONTRACT_BROADCAST_DESCRIPTOR`, and the new Stage A2 hop wired between Stage A's output and Stage B's `key_by(segment_key)` (pre-spec §C's diagram).
- `streaming/flink-jobs/src/flink_jobs/settings.py` — `owner_contracts_topic: str` (same pattern as `apartment_segments_topic`).

**Shared schema / event contract:**
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — `ReasonCode` gains `"commission_base_netting"` (8 values); `Calculation` gains `commission_base: Literal["total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"]`.
- `specs/events/price_decision.v1.json` — `calculation.commission_base` (new required field); the shared `decision_component` `$defs` entry's `code` enum gains the 8th value.

**Lakehouse / dbt:**
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — one new scalar `NestedField` for `commission_base` (next ID after Phase 10's 58 → 59) — a cheap addition, not a new shape (a string, like `floor_type`/`rule_applied` already are).
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — passthrough + a defensive default (`"total_revenue"` — the same default `decide_price()`'s new parameter and the DB column both use) for DynamoDB Streams records predating this phase.
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `commission_base` (pure schema evolution).

**Contracts / tests:**
- 3 existing `specs/contracts/fixtures/price_decision/*.json` gain `calculation.commission_base: "total_revenue"` (no netting — kept minimally changed, since their purpose is exercising `rule_applied`'s three values, a different dimension than `commission_base`).
- Unit tests: `cost_aggregation.py` (the two new sub-totals), `pricing.py` (netting formula per base, regression at `total_revenue`, `commission_base_netting` component present/absent correctly, LOS candidates never carry it), `stage_owner_contract_enrichment.py` (new test file, mirrors `test_stage_cost_enrichment.py`'s shape), `stage_price_decision.py` (wiring), `lakehouse-consumer` (schema/transform defaults).
- **Live LocalStack verification is a required acceptance criterion (AC-11)** — this phase adds a new Postgres table pair, a new Debezium-captured table, a new Kafka topic, a new Flink broadcast stage, and removes an existing column; every one of those is exactly the kind of change Phase 9's incident doc already showed unit tests alone can't fully vouch for.

### Out of scope (explicitly deferred)

- **A general/solver-based commission base** — 3 named, closed bases only; ADR-0011's "solved algebraically or via a solver" framing is satisfied here by hand-deriving 3 closed-form cases (§4), not by building a general solver.
- **Backlog #3's full `concept` breakdown** — only `ota_fee`/`channel_manager`/`cleaning` are split out of the existing `fixed_cost_eur`/`variable_cost_eur` totals; the other 10 `concept` values stay folded in undifferentiated, exactly as today.
- **Owner-level dashboard/dbt exposure** (`owner_name`, per-owner rollups, a `dim_owner` mart) — no anchor needs it yet; `owners` isn't even CDC-captured (§2), so it isn't reachable from the lakehouse side at all in this phase.
- **Backlog #7 (layered RM engine) / #9 (manual overrides)** — this phase is Decision Components' first real consumer beyond Phase 10 itself, not the next phase to extend the container's own mechanics.
- **Historized contracts** (a contract changing over time, with an effective-date range) — `owner_contracts` is current-state-only, same "no history" convention every other config table in this project already follows.

---

## 3. Data model

### `owners` (new)

```sql
CREATE TABLE IF NOT EXISTS public.owners (
    owner_id    TEXT PRIMARY KEY,
    owner_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Not CDC-captured — a plain Postgres dimension, seeded once, referenced only by `owner_contracts.owner_id`'s FK.

### `owner_contracts` (new)

```sql
CREATE TABLE IF NOT EXISTS public.owner_contracts (
    apartment_id     TEXT PRIMARY KEY REFERENCES public.apartment_market_segments(apartment_id),
    owner_id         TEXT NOT NULL REFERENCES public.owners(owner_id),
    commission_base  TEXT NOT NULL DEFAULT 'total_revenue'
                          CHECK (commission_base IN ('total_revenue', 'revenue_minus_ota', 'revenue_minus_ota_minus_cleaning')),
    commission_pct   NUMERIC(5,4) NOT NULL DEFAULT 0.15
                          CHECK (commission_pct >= 0 AND commission_pct <= 1),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ
);
```

One row per apartment (its PK), several rows may share the same `owner_id` — the actual point of a real owner entity (pre-spec §A). Reuses the existing `dbz_publication`/connector (pre-spec §B); routed to `owner-contracts.v1`.

### `apartment_market_segments` — `commission_pct` removed

```sql
ALTER TABLE public.apartment_market_segments
    DROP CONSTRAINT IF EXISTS apartment_market_segments_commission_pct_check;
ALTER TABLE public.apartment_market_segments
    DROP COLUMN IF EXISTS commission_pct;
```

The first genuinely destructive DDL statement since ADR-0009 introduced this table's self-healing-migration convention — deliberate (pre-spec §A), not accidental scope creep. `target_margin`, `competitiveness_discount`, and the four Phase 8 Bonus/Malus columns are unaffected and stay exactly where they are.

### `CostAggregate` — new fields

| Field | Type | Source |
|---|---|---|
| `commission_base` | `str` | Resolved by Stage A2 from `owner_contracts`. Default `"total_revenue"` until Stage A2 runs. |
| `commission_pct` | `float` | Same as today, but now resolved by Stage A2, not Stage A. Default `0.15` until Stage A2 runs. |
| `ota_related_cost_eur` | `float` | Resolved by Stage A from `cost_aggregation.py`'s new sub-total. |
| `cleaning_cost_eur` | `float` | Resolved by Stage A from `cost_aggregation.py`'s new sub-total. |

### `price_decision.v1` — one new field on `calculation`

| Field | Type | Description |
|---|---|---|
| `commission_base` | `string` (enum, 3 values) | Which revenue base `commission_pct` is charged against for this decision (ADR-0011 backlog #5). |

---

## 4. Formula

`cost_aggregation.py` (additions only):

```python
OTA_RELATED_CONCEPTS = frozenset({"ota_fee", "channel_manager"})
CLEANING_CONCEPTS = frozenset({"cleaning"})

ota_related_total = sum(line.amount_gross for line in matching if line.concept in OTA_RELATED_CONCEPTS)
cleaning_total = sum(line.amount_gross for line in matching if line.concept in CLEANING_CONCEPTS)

ota_related_cost_eur = round(ota_related_total / available_days, 2) if available_days > 0 else 0.0
cleaning_cost_eur = round(cleaning_total / available_days, 2) if available_days > 0 else 0.0
```

Both amounts stay fully counted inside the existing `fixed_cost_eur`/`variable_cost_eur` totals — these are additional breakdowns for the commission-base netting below, not new costs (pre-spec §D).

`pricing.py` (`decide_price()`, netting adjustment applied uniformly to every floor tier):

```python
def _netted_amount(commission_base, ota_related_cost_eur, cleaning_cost_eur) -> float:
    if commission_base == "total_revenue":
        return 0.0
    if commission_base == "revenue_minus_ota":
        return ota_related_cost_eur
    return ota_related_cost_eur + cleaning_cost_eur  # revenue_minus_ota_minus_cleaning

net = _netted_amount(commission_base, ota_related_cost_eur, cleaning_cost_eur)

# structural_full_margin
minimum_price_eur = (fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur - commission_pct * net) / (1 - target_margin - commission_pct)
# structural_reduced_margin
minimum_price_eur = (fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur - commission_pct * net) / (1 - reduced_margin - commission_pct)
# contribution
minimum_price_eur = (variable_cost_eur + one_time_cost_per_night_eur - commission_pct * net) / (1 - commission_pct)
```

Derivation (pre-spec §E): commission charged against `P − net` instead of `P` gives `P = costs + margin·P + commission_pct·(P − net)`, solved for `P`: `P = (costs − commission_pct·net) / (1 − margin − commission_pct)`. At `commission_base = "total_revenue"` (`net = 0`), this is algebraically identical to every pre-Phase-11 worked example — the same regression-safety convention every prior phase's new parameter has used.

`decision_components` gains, appended after the existing rule component, **only when `net > 0`**:

```python
if net > 0:
    impact = -round(commission_pct * net, 2)
    decision_components.append(DecisionComponent(
        code="commission_base_netting",
        label=f"Commission base '{commission_base}' nets {net} EUR/night, reducing the floor by {-impact} EUR",
        impact=impact,
    ))
```

### Worked example (pre-spec §E, verified numerically before implementation)

`fixed_cost_eur=20` (incl. `channel_manager_eur=15`), `variable_cost_eur=100` (incl. `ota_fee_eur=30`, `cleaning_eur=25`), `one_time_cost_eur=0`, `target_margin=0.05`, `commission_pct=0.15`, `days_to_arrival=45` (→ `structural_full_margin`):

| `commission_base` | `net` | `minimum_price_eur` | `commission_base_netting` component |
|---|---|---|---|
| `total_revenue` | `0` | `120 / 0.80 = 150.00` | not emitted |
| `revenue_minus_ota` | `45` | `(120 − 6.75) / 0.80 = 141.56` | `impact = -6.75` |
| `revenue_minus_ota_minus_cleaning` | `70` | `(120 − 10.50) / 0.80 = 136.88` | `impact = -10.50` |

A narrower base (more netted) produces a strictly lower floor, all else equal — the sign-correctness check this derivation was verified against (same discipline that caught ADR-0009's `n×Cf`/`n×Cv` bug during Phase 9).

---

## 5. Acceptance criteria

- **AC-01 — `cost_aggregation.py`'s new sub-totals are correct.** `ota_related_cost_eur`/`cleaning_cost_eur` computed correctly from a mix of `concept` values, both still fully counted inside `fixed_cost_eur`/`variable_cost_eur`.
- **AC-02 — `decide_price()`'s default (`total_revenue`) is a pure regression.** Every existing ADR-0009/Phase 8/9/10 worked example and test passes unchanged with the new parameters at their defaults.
- **AC-03 — The netting formula is correct per base.** Unit-tested against the worked example (§4) for all three `commission_base` values, both `minimum_price_eur` and the sign/magnitude of the netting.
- **AC-04 — The netting adjustment applies to all three floor tiers**, not just `structural_full_margin` — one test per tier confirming the same `− commission_pct·net` term appears.
- **AC-05 — `commission_base_netting` is emitted only when `net > 0`.** Absent at `total_revenue`; present, with the correct negative `impact`, at the other two bases.
- **AC-06 — LOS candidates never carry `commission_base_netting`.** Every `LosFloorCandidate.decision_components` stays rule-only (1 entry), matching Phase 10's own convention — the netting amount doesn't vary with `stay_length`, so it's not re-emitted per candidate any more than property components are.
- **AC-07 — `OwnerContractEnrichmentFunction` resolves correctly and skips safely.** No emission for a `CostAggregate` whose apartment has no owner-contract broadcast entry yet; the correct `commission_base`/`commission_pct` once one arrives.
- **AC-08 — The Postgres schema migrates correctly.** `owners`/`owner_contracts` created on a pre-Phase-11 volume; `apartment_market_segments.commission_pct` is dropped (verified against a volume that had it).
- **AC-09 — `price_decision.v1` validates the new field.** Contract test covers `calculation.commission_base`'s presence and 3-value enum.
- **AC-10 — dbt marts carry `commission_base`.** `fct_price_decision` exposes it, verified by `dbt run` + `dbt test`.
- **AC-11 — Live LocalStack verification (required).** A real seeded apartment whose contract has a non-`total_revenue` base shows the correct netted `minimum_price_eur` and a `commission_base_netting` decision component in a live DynamoDB item and in `fct_price_decision` via `dbt show`; the new `owner-contracts.v1` CDC stream is confirmed actually flowing (not just configured) via a raw Kafka console-consumer read; Flink reports zero exceptions with the new Stage A2 hop active.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `cost_aggregation.py`'s new sub-totals (AC-01), `decide_price()`'s netting formula and `decision_components` wiring (AC-02–AC-06) — same kind of test this project's pricing/aggregation modules already have.
- **Flink stage, fakes only:** `OwnerContractEnrichmentFunction` (AC-07), using the same `FakeBroadcastContext`/`FakeReadOnlyContext` fixtures `test_stage_cost_enrichment.py` already established.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-09), Postgres migration behavior against a volume fixture (AC-08).
- **dbt:** `dbt run`/`dbt test` against updated fixtures (AC-10).
- **Live LocalStack (required, AC-11):** full bring-up per the updated `docs/manual/MANUAL.md` (new topic, connector config with the new table), confirming the new CDC stream, the new Flink stage, and the netted formula's real output all agree with hand-computed expectations for an actual seeded apartment.

---

## 7. Known limitations

- **Only 3 closed commission bases**, not a general solver — matches ADR-0011's examples exactly, but a future base (e.g. "Revenue − OTA − Cleaning − Maintenance") needs its own `_netted_amount()` branch and its own derivation, not a config-only change.
- **`owners` carries only a synthetic name**, unused by pricing itself — exists to make the 1-to-N grain real (pre-spec §A), not because any current consumer needs owner identity.
- **Contracts have no history** — a contract is current-state-only, same limitation every other config table in this project (`apartment_market_segments`, its `target_margin`/`competitiveness_discount`) already carries.
- **`commission_pct`'s removal from `apartment_market_segments` is irreversible for any volume that already ran this migration** — acceptable per ADR-0011's own "no production consumers yet" reasoning, but the first phase in this project's history where that reasoning is exercised for an actual column drop, not just an additive change.

---

## 8. Follow-ups

- **Backlog #7 (layered RM engine)** — a future "Commercial" or "Guardrails" layer could reuse this phase's `_netted_amount()` shape for its own revenue-base-sensitive rules.
- **Backlog #3 (full `concept` breakdown)** — the other 10 `concept` values remain folded into undifferentiated `fixed_cost_eur`/`variable_cost_eur`; a future phase needing e.g. `insurance` or `community_fee` broken out separately follows this phase's exact pattern.
- **Owner-level dashboard rollups** — `owners` existing as a real entity (not CDC-captured yet) is the natural anchor for a future `dim_owner`/portfolio view, if a use case for it appears.
- **`docs/post-poc-roadmap.md`** — backlog #5 moves from "Later" to "done" (this spec).

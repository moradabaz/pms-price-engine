# Phase 9 — LOS floor matrix

**Status:** Draft
**Depends on:** Phase 4 (`decide_price()`, Stage B), Phase 8 (`property_attribute_factor`, the most recent shape of `Calculation`)
**Blocks:** Nothing downstream depends on this yet — backlog item #1 from `docs/post-poc-roadmap.md`
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #1), [ADR-0009](../../../docs/adr/ADR-0009-profitability-floor-reform.md) (D5, the `n=1` deferral this phase resolves), [`docs/phase-9-los-floor-matrix-design-decisions.md`](../../../docs/phase-9-los-floor-matrix-design-decisions.md) (pre-spec, decisions A–E), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #1, §4 Flink learning note, §5 original A/B outline — superseded by this spec's §1), [`error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md`](../../../error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md) (two bugs found live-verifying this phase — read before touching `dynamodb_sink.py`/`iceberg_writer.py` again)

---

## 1. Executive summary

ADR-0009 (D5) fixed `stay_length` at `1` because no phase of this project models a real multi-night reservation — a conservative choice that never under-covers the one-time cost `Cr`, but means the engine can never show what a booking's floor would look like at 3, 7, or 14 nights, where `Cr` (a one-time booking cost, e.g. cleaning+laundry) amortizes very differently across the stay (`docs/post-poc-roadmap.md` §1's own worked example: €110/booking is €110/night at LOS 1, €11/night at LOS 10).

The roadmap originally framed doing this for real as a fork: precompute a `price_decision` row per LOS candidate (breaking DynamoDB's primary key), or compute the matrix at read time outside Flink (duplicating the pricing formula). Neither is necessary: `market_reference_price_eur` and everything derived from it depend only on the market snapshot and this apartment's `property_attribute_factor`/`competitiveness_discount` — never on `stay_length` or `days_to_arrival`. Only the floor (`minimum_price_eur`) varies with `n`. This phase computes the floor (and what it implies for `rule_applied`/`suggested_price_eur`/`effective_margin`) for a fixed set of LOS candidates **inside the same decision Stage B already emits per (apartment, night)** — one new field, no new emission, no PK change.

**Done when:** every `price_decision.v1` event carries a `calculation.los_floor_matrix` with one entry per candidate LOS (`{1, 2, 3, 7, 14}` nights), each entry's `minimum_price_eur` matching what `decide_price()` would compute standalone for that `stay_length`, and the top-level `calculation` (still `stay_length=1`) is unaffected.

**Not in this phase:** any change to Stage B's fan-out, join keys, or DynamoDB schema; a dashboard view rendering the matrix (Phase 6 territory, a natural follow-up, not required here); runtime-configurable LOS candidates (code constant for now, same stance Phase 8 took on Bonus/Malus weights); Decision Components / structured reason codes (backlog #4 — this phase's list field is a scoped precedent for that mechanism, not an implementation of it).

---

## 2. Scope

### In scope

- `streaming/flink-jobs/src/flink_jobs/pricing.py`:
  - `decide_price()` gains `stay_length: int = 1`, replacing the hardcoded `STAY_LENGTH_NIGHTS` constant. **Formula correction, not a mechanical substitution** (pre-spec §B): `fixed_cost_eur`/`variable_cost_eur` are already per-night rates (`cost_aggregation.py`) and are no longer multiplied by `n`; only `one_time_cost_eur` is divided by `stay_length` to amortize it across the stay. ADR-0009's own `(n×Cf)+(n×Cv)+Cr` notation, taken literally, never dilutes anything when `Cf`/`Cv` are small — verified numerically in the pre-spec. For `stay_length=1` this is algebraically identical to today's behavior.
  - New `LOS_CANDIDATES: tuple[int, ...] = (1, 2, 3, 7, 14)` constant.
  - New `LosFloorCandidate` frozen dataclass: `stay_length`, `minimum_price_eur`, `floor_type`, `rule_applied`, `suggested_price_eur`, `effective_margin`.
  - New `decide_price_los_matrix(...)` function: same parameters as `decide_price()` (minus `stay_length`, plus `stay_lengths: tuple[int, ...] = LOS_CANDIDATES`), returns `list[LosFloorCandidate]` by calling `decide_price()` once per candidate. No formula logic duplicated — a thin composition over the existing function (SOLID: single responsibility stays with `decide_price()`; DRY: one formula, N invocations).
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` — `_build_price_decision()` calls `decide_price_los_matrix()` once, alongside the existing `decide_price()` call, and assembles `Calculation.los_floor_matrix` from the result.
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — new `LosFloorCandidate` Pydantic sub-model (`extra="forbid"`, matching every other sub-model); `Calculation` gains `los_floor_matrix: list[LosFloorCandidate]`.
- `specs/events/price_decision.v1.json` — `calculation.los_floor_matrix`: array of objects, same shape, `minItems: 1`, required.
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — `pyiceberg.types.ListType` wrapping a `StructType`, fresh field IDs continuing from 40 (Phase 8's last-used IDs were 39/40).
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — builds the list of dicts for `calculation.los_floor_matrix` from the DynamoDB item.
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `calculation.los_floor_matrix` unchanged (DuckDB handles nested list/struct columns natively via `iceberg_scan()`).
- `specs/contracts/test_price_decision_contract.py` + fixtures — updated for the new required field.
- `streaming/flink-jobs/src/flink_jobs/dynamodb_sink.py` — `_python_to_dynamodb()` gains a `list` case (`{"L": [...]}`). Found live, not anticipated in the original scope: this was this project's first `list`-typed field on `PriceDecision`, and nothing before it had ever exercised this function's handling of a non-scalar, non-dict value (`error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md` #1).
- `services/lakehouse-consumer/src/lakehouse_consumer/iceberg_writer.py` — `ensure_table()` now reconciles an already-existing table to the current `ICEBERG_SCHEMA` via `table.update_schema().union_by_name(...)` on every call, not just at first creation. Also found live, not anticipated: `create_table_if_not_exists` never migrates an existing table, and no prior phase's live verification had reused an existing table long enough to hit this (`error-handling/...` #2). Same self-healing-migration shape `mock-pm-app`'s Postgres migration already uses.
- Unit tests: `decide_price()`'s `stay_length` parameter (default `1` reproduces every existing ADR-0009/Phase 8 worked example unchanged; larger `n` dilutes `one_time_cost_eur` correctly and leaves `fixed_cost_eur`/`variable_cost_eur` unchanged); `decide_price_los_matrix()` (exactly `len(LOS_CANDIDATES)` entries, each matching a standalone `decide_price()` call with the same `stay_length`); `stage_price_decision.py` (the emitted `PriceDecision.calculation.los_floor_matrix` has the right shape and values); `dynamodb_sink.py`'s list serialization (previously zero coverage); `iceberg_writer.py`'s schema migration (an existing table missing a field gets it; an up-to-date table gets no new schema version).

### Out of scope (explicitly deferred)

- **Any Stage B redesign, new `MapState`, or DynamoDB PK change.** The entire point of §1 of the pre-spec is that this phase needs none of that.
- **A dashboard "LOS Matrix" view** (BiLemon §26, Panel G) — the data now exists in `fct_price_decision`, rendering it is a stretch goal for a future dashboard increment, not blocking here (same framing Phase 5 used for `fct_price_decision`'s own drill-down, and Phase 6 used for the explainability view).
- **Runtime-configurable LOS candidates** — `LOS_CANDIDATES` is a code constant, same stance as Phase 8's attribute weights; backlog #7 (layered RM engine) is where a real rule-versioning mechanism belongs.
- **Using the matrix to actually adjust `minimum_stay` or publish LOS-specific rates** (BiLemon §10.2's "Minimum Stay as a profitability lever") — this phase computes and exposes the matrix for audit/explainability; acting on it (e.g. recommending a minimum-stay change) is a Revenue Management decision, backlog #7 territory.

---

## 3. Data model

### `Calculation` — one new field

| Field | Type | Description |
|---|---|---|
| `los_floor_matrix` | `list[LosFloorCandidate]` | One entry per candidate stay length (`pricing.LOS_CANDIDATES`), computed from the same cost/market inputs as the top-level `calculation` — only `stay_length` varies. |

### `LosFloorCandidate` (new)

| Field | Type | Description |
|---|---|---|
| `stay_length` | `int >= 1` | Candidate LOS this entry evaluates. |
| `minimum_price_eur` | `number >= 0` | The floor for this LOS — `(Cf+Cv+Cr/n)/(1-margin-Cp)` or `(Cv+Cr/n)/(1-Cp)` (§4 — amortizes `Cr` across `n` nights; `Cf`/`Cv` are already per-night and unaffected by `n`), same formula as the top-level `calculation.minimum_price_eur`, evaluated at this `stay_length`. |
| `floor_type` | enum, same 3 values as top-level `floor_type` | Unaffected by `stay_length` — selected by `days_to_arrival`, same for every candidate in one decision. |
| `rule_applied` | enum, same 3 values as top-level `rule_applied` | Can genuinely differ per candidate — a longer stay's diluted floor can flip `cost_protected` into `market_competitive`. |
| `suggested_price_eur` | `number >= 0` | What this LOS candidate would be priced at. |
| `effective_margin` | `number` | Varies per candidate — `total_cost_eur` scales with `n`. |

Not repeated per candidate (identical across every entry in one decision, per pre-spec §D): `property_attribute_factor`, `property_reference_price_eur`, `market_reference_price_eur`, `below_market_by`.

---

## 4. Formula

### `decide_price()`'s internal formula — corrected for `stay_length > 1` (pre-spec §B)

```python
n = stay_length
one_time_cost_per_night_eur = one_time_cost_eur / n

if days_to_arrival > STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS:
    floor_type = "structural_full_margin"
    minimum_price_eur = (
        fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
    ) / (1 - target_margin - commission_pct)
elif days_to_arrival >= STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS:
    floor_type = "structural_reduced_margin"
    reduced_margin = target_margin * REDUCED_MARGIN_FACTOR
    minimum_price_eur = (
        fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
    ) / (1 - reduced_margin - commission_pct)
else:
    floor_type = "contribution"
    minimum_price_eur = (variable_cost_eur + one_time_cost_per_night_eur) / (
        1 - commission_pct
    )

# ... property_reference_price_eur / market_reference_price_eur / rule_applied: unchanged (ADR-0011) ...

total_cost_eur = fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
effective_margin = (suggested_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
```

`fixed_cost_eur`/`variable_cost_eur` are dropped from the `n ×` multiplication entirely — they're
already nightly rates. Only `one_time_cost_eur` is amortized (`/ n`). For `stay_length=1`,
`one_time_cost_per_night_eur == one_time_cost_eur`, so this is a no-op relative to every existing
test.

### `decide_price_los_matrix()` — thin composition over `decide_price()`, no formula duplicated

```python
LOS_CANDIDATES: tuple[int, ...] = (1, 2, 3, 7, 14)


@dataclass(frozen=True)
class LosFloorCandidate:
    stay_length: int
    minimum_price_eur: float
    floor_type: FloorType
    rule_applied: RuleApplied
    suggested_price_eur: float
    effective_margin: float


def decide_price_los_matrix(
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
    target_margin: float,
    commission_pct: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
    days_to_arrival: int,
    property_attribute_factor: float = 1.0,
    stay_lengths: tuple[int, ...] = LOS_CANDIDATES,
) -> list[LosFloorCandidate]:
    return [
        LosFloorCandidate(
            stay_length=n,
            minimum_price_eur=calc.minimum_price_eur,
            floor_type=calc.floor_type,
            rule_applied=calc.rule_applied,
            suggested_price_eur=calc.suggested_price_eur,
            effective_margin=calc.effective_margin,
        )
        for n in stay_lengths
        for calc in [
            decide_price(
                fixed_cost_eur=fixed_cost_eur,
                variable_cost_eur=variable_cost_eur,
                one_time_cost_eur=one_time_cost_eur,
                target_margin=target_margin,
                commission_pct=commission_pct,
                avg_nightly_rate_eur=avg_nightly_rate_eur,
                competitiveness_discount=competitiveness_discount,
                days_to_arrival=days_to_arrival,
                property_attribute_factor=property_attribute_factor,
                stay_length=n,
            )
        ]
    ]
```

### Worked example

One apartment, `days_to_arrival=45` (structural_full_margin), `Cf=0`, `Cv=0`, `Cr=110.0`
(cleaning+laundry — `docs/post-poc-roadmap.md` §1's own example amount), `target_margin=0.05`,
`commission_pct=0.15`, `avg_nightly_rate_eur=90.0`, `competitiveness_discount=0.05`,
`property_attribute_factor=1.0` — verified numerically (pre-spec §B):

| `stay_length` | `minimum_price_eur` = `(0 + 0 + 110/n) / 0.80` | `rule_applied` | `suggested_price_eur` |
|---|---|---|---|
| 1 | `110.00 / 0.80 = 137.50` | `cost_protected` | `137.50` |
| 3 | `36.67 / 0.80 = 45.83` | `market_competitive` | `85.50` |
| 7 | `15.71 / 0.80 = 19.64` | `market_competitive` | `85.50` |
| 14 | `7.86 / 0.80 = 9.82` | `market_competitive` | `85.50` |

`market_reference_price_eur = 90.0 × 0.95 = 85.50` for every row (§A's premise: only the floor
moves). At LOS 1, a single night can't absorb the €110 turnover cost profitably at a competitive
price, so the floor (`137.50`) wins — `cost_protected`. By LOS 3, the same €110 diluted across 3
nights (`€36.67/night`) comfortably clears the market reference — `market_competitive`, same as
every longer stay. This is exactly the "minimum stay as a profitability lever" effect the source
spec describes (§10.2) — visible here as data the engine now computes, not yet acted on (§2, out of
scope). Note this same scenario computed with ADR-0009's literal `(n×Cf)+(n×Cv)+Cr` notation would
return `137.50` unchanged at every `stay_length` (`Cf=Cv=0`, so `n×Cf`/`n×Cv` stay `0` and `Cr` is
never divided) — the exact bug the pre-spec's §B correction fixes.

---

## 5. Acceptance criteria

- **AC-01 — `decide_price()`'s `stay_length` default preserves every existing test.** `stay_length=1` (the default) reproduces the full existing `test_pricing.py` suite (ADR-0009, Phase 8) unchanged.
- **AC-02 — `stay_length` correctly amortizes `Cr` (one-time cost), never `Cf`/`Cv`.** Unit test comparing `stay_length=1` vs. `stay_length=10` with a nonzero `one_time_cost_eur` and zero `fixed_cost_eur`/`variable_cost_eur`: `minimum_price_eur` at LOS 10 is exactly `1/10` of LOS 1's (pure `Cr/n` dilution). A second test with nonzero `fixed_cost_eur`/`variable_cost_eur` and zero `one_time_cost_eur` confirms `minimum_price_eur` is unchanged across every `stay_length` — `Cf`/`Cv` are already per-night rates and must never scale with `n`.
- **AC-03 — `decide_price_los_matrix()` returns exactly `len(LOS_CANDIDATES)` entries, each matching a standalone call.** For every candidate, calling `decide_price()` directly with that `stay_length` (same other inputs) produces identical `minimum_price_eur`/`rule_applied`/`suggested_price_eur`/`effective_margin` to the matching matrix entry.
- **AC-04 — At least one worked case where `rule_applied` differs across candidates in the same matrix** (the §4 worked example, as a test) — proves the phase's actual point, not just that the function runs.
- **AC-05 — `price_decision.v1`'s `los_floor_matrix` validates and round-trips.** Contract test covers presence, `minItems: 1`, and field types; `stage_price_decision.py`'s emitted `PriceDecision` carries a matrix whose `stay_length=1` entry matches the top-level `calculation`'s own values exactly.
- **AC-06 — Iceberg/dbt carry the new list field without error.** `lakehouse-consumer`'s schema/transform tests cover the new `ListType` field; `dbt run`/`dbt test` against updated fixtures pass with `los_floor_matrix` present on `fct_price_decision`.
- **AC-07 — `DynamoDbSinkFunction` serializes a `list`-valued field without raising.** `test_dynamodb_sink.py` (found live, `error-handling/los-floor-matrix-list-field-broke-sink-and-iceberg-migration.md` #1 — zero prior coverage of `_python_to_dynamodb()`'s non-scalar handling).
- **AC-08 — `ensure_table()` migrates an existing table to the current schema, idempotently.** A table missing `los_floor_matrix` gains it on the next `ensure_table()` call; a table already current gets no new schema version on repeated calls (found live, same file, #2).

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `decide_price()`'s `stay_length` parameter (AC-01, AC-02), `decide_price_los_matrix()` (AC-03, AC-04) — same style as every existing `pricing.py` test.
- **Integration, no infrastructure:** `stage_price_decision.py`'s `_build_price_decision()` emits the matrix correctly (AC-05's second half) — extends the existing `test_stage_price_decision.py` fakes-based suite.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-05's first half), `lakehouse-consumer` schema/transform unit tests (AC-06).
- **Live verification against LocalStack:** per the standing project practice (Phase 8's own verification pass) — bring up the stack, confirm real `price_decision` items in DynamoDB carry a `los_floor_matrix` with sensible per-candidate values, and confirm `fct_price_decision` exposes it after a `dbt run`. This is exactly the pass that found AC-07/AC-08's bugs (neither reachable from the unit suite alone) — deliberately run this time against a *reused* volume, not a fresh one (Phase 8 used a fresh volume and would not have caught AC-08's gap).

---

## 7. Known limitations

- **LOS candidates are a fixed code constant**, not runtime-configurable — same limitation Phase 8 accepted for attribute weights, for the same reason (no rule-versioning mechanism exists yet; that's backlog #7).
- **No dashboard view renders the matrix yet** — the data reaches `fct_price_decision`, but Phase 6's dashboard doesn't query it. A natural, low-effort follow-up once picked up.
- **The matrix doesn't validate against real availability** — same structural gap this project has had since Phase 1 (no occupancy/availability calendar); a 14-night candidate is evaluated economically, not checked against whether 14 consecutive available nights actually exist.
- **Minimum-stay policy is not derived from this data** — the matrix shows the lever exists (§4's worked example); using it to recommend a minimum-stay change is explicitly deferred (§2).

---

## 8. Follow-ups

- **Backlog #4 (Decision Components)** — this phase is the first precedent for a `list[...]`-typed field on `PriceDecision`; a future `decision_components: list[DecisionComponent]` follows the same Pydantic/JSON-Schema/Iceberg-`ListType`/dbt-passthrough mechanics this phase establishes.
- **Backlog #7 (layered RM engine)** — `LOS_CANDIDATES` and the attribute weights (Phase 8) both become that engine's first configurable inputs once a real rule-versioning mechanism exists.
- **A future dashboard "LOS Matrix" panel** (BiLemon §26 Panel G) can read `fct_price_decision.los_floor_matrix` directly — no new pipeline work needed, per this project's established schema-evolution story (Phase 5 §5/AC-05).
- **`docs/post-poc-roadmap.md`** — item #1 moves from "Now" to "done" (this spec); its original §5 A/B outline is superseded by this spec's §1 reframing, kept in the roadmap as historical context rather than rewritten.

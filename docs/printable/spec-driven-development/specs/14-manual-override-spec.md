# Phase 14 — Manual Override & Audit Trail

**Status:** Implemented — all acceptance criteria (§5) verified live against the running stack on 2026-09-07, including AC-09's full round trip (insert → CDC → Stage C applies it → DynamoDB → dbt mart, then an expiry-based revert, all without a Flink restart)
**Depends on:** Phase 1 (`apartment_market_segments`), Phase 2 (CDC pipeline / Debezium connector), Phase 4 (Stage A/B), Phase 10 (`decision_components`, the container this phase's override reason code plugs into), Phase 13 (the layered engine whose output this phase enriches, not extends)
**Blocks:** Backlog #8 (channel gross-up) and #2 (per-channel) are independent of this phase — no ordering dependency either way
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #9), [`docs/profitable-pricing-glossary.md`](../../../docs/profitable-pricing-glossary.md) §11 (plain-language explanation), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§3 backlog #9, reprioritized 2026-09-07 from "Later" to "Now" on client-value grounds)

---

## 1. Executive summary

Every phase since Phase 1 has been a one-way pipeline: Postgres/Kinesis → Flink → DynamoDB/Iceberg. Nothing downstream of Flink ever writes back. That is correct for an algorithmic recommendation engine, but the external spec's own conflict-resolution table (spec §13) is explicit that not every conflict should be resolved by the algorithm: sometimes the business needs to sell below the Profitable Floor on purpose — to avoid an empty apartment before an event, to honor a one-off commercial relationship — and that decision must be **authorized, reasoned, time-boxed, and audited**, never silent (spec §11.1's `ManualOverride` entity; test scenario T12).

This phase adds exactly that: a new `manual_overrides` Postgres table (one human-authored row per `(apartment_id, target_date)`), CDC-captured through the existing Debezium publication, broadcast into a new **Stage C** in Flink — `ManualOverrideEnrichmentFunction`, chained after Stage B (`PriceDecisionFunction`) — which replaces `output.suggested_price_eur` with the authorized value whenever an unexpired override exists for that apartment/night, records the expected loss (if any) versus the algorithmic floor, and appends a `manual_override_applied` decision component. This is the first phase in this project's history that introduces a **human write path** into the pipeline — not a new data source Flink merely reads faster, but the first case of "a person tells the system what to do, and the system complies while keeping a permanent record of who, why, and until when."

**Done when:** inserting a row into `manual_overrides` for a real seeded apartment/night causes that night's live DynamoDB `price_decision` item to show `output.suggested_price_eur` equal to the override's price, `calculation.manual_override` populated with the reason/author/expected loss, and a `manual_override_applied` decision component present — without restarting the Flink job, and without affecting any other apartment or night, or that apartment's own `los_floor_matrix`.

**Not in this phase:**
- A UI or REST API for creating overrides. Per this project's existing convention (owner contracts are inserted via seed script, not a UI), an override is authored by directly inserting a row into `manual_overrides` — documented as a manual step in `docs/manual/MANUAL.md`, the same way this repo already documents manual Kafka/psql commands for every other phase's live verification. Building an authoring UI is real product work with no bearing on this project's learning focus (CDC/streaming), and is out of scope.
- **Role-based authorization.** `authorized_by` is a free-text column, recorded for audit, not checked against any identity/roles system — this project has no auth system anywhere today, and inventing one only for this table would be scope creep disconnected from the CDC/streaming learning focus.
- **Propagating a `DELETE` on `manual_overrides` as a live retraction.** The existing Debezium transform (`transforms.unwrap.delete.handling.mode: "drop"`) already drops delete events for every captured table, project-wide — changing that global behavior for one table is out of scope. Cancelling a live override is done by **updating** its `valid_until` to a past timestamp (§4), not by deleting the row.
- **Overriding an entire LOS floor matrix** or a date range in one row. One override row is one specific `(apartment_id, target_date)` — the same "no reservation entity, hypothetical stay candidates only" scoping Phase 9 already applied to `los_floor_matrix`. A range of nights needs a range of rows; not solved by this phase.
- **Recomputing `below_market_by`** against the override price. It keeps its pre-override, algorithmic meaning (§4) — it answers "how did the algorithm's own suggestion compare to market," not "how does the final published price compare to market."
- **Contract history for `manual_overrides` itself.** Same "current-state-only, no history" convention every other config table here follows (Phase 11 §7) — the audit trail lives in the `price_decision` history this override produces while active, in Iceberg, not in a Postgres row history.

---

## 2. Scope

### In scope

**Postgres / mock-pm-app:**
- `specs/phases/01-mock-app-db/manual_overrides.sql` (new) — see §3 for DDL.
- `services/mock-pm-app/src/mock_pm_app/migrations.py` — `ensure_manual_overrides_schema` (new, same `CREATE TABLE IF NOT EXISTS` self-healing pattern every other table here uses, embedding the same SQL as the `.sql` file above). **No seed function** — unlike every other table in this project, this one starts empty and stays empty unless a human inserts a row (§1).
- `services/mock-pm-app/src/mock_pm_app/main.py` — wires `ensure_manual_overrides_schema` into the existing startup schema-ensure sequence only (not the seed sequence, since there is nothing to seed).

**Debezium / infra:**
- `infra/debezium/postgres-connector.json` — `manual_overrides` added to `table.include.list`; a new `routeManualOverrides` `RegexRouter` transform to `manual-overrides.v1`. **No new connector, slot, publication, or `message.key.columns` entry** — reuses the existing `dbz_publication` (same pattern Phase 11 established for `owner_contracts`), and this table's own primary key (`apartment_id, target_date`) is already the natural Debezium message key, unlike `payment_lines`' surrogate `event_id` which needed an explicit override.
- `docs/manual/MANUAL.md` — a fifth manually-created Kafka topic, `manual-overrides.v1`, alongside the four this doc's bring-up sequence already creates; a new walkthrough section, "Authoring a Manual Override," showing the `psql INSERT` a human runs, and how to verify the resulting `price_decision` item live (DynamoDB read + `dbt show` on `fct_price_decision`).

**Flink (`streaming/flink-jobs`):**
- `streaming/flink-jobs/src/flink_jobs/models.py` — new `ManualOverrideRow` (one CDC row: `apartment_id`, `target_date`, `override_price_eur`, `reason`, `authorized_by`, `valid_until`) and `ManualOverrideAssignment` (the same fields minus the two used as the broadcast-state key), mirroring `OwnerContractRow`/`OwnerContractAssignment`'s shape (Phase 11).
- `streaming/flink-jobs/src/flink_jobs/stage_manual_override_enrichment.py` (new) — `ManualOverrideEnrichmentFunction(KeyedBroadcastProcessFunction)`, Stage C (§4): on `process_element` (a `PriceDecision` from Stage B), looks up the broadcast override keyed by `f"{apartment_id}:{target_date.isoformat()}"`; if absent, or present but `valid_until` has passed, yields the `PriceDecision` unchanged; otherwise yields a copy with `calculation.manual_override`, an appended `manual_override_applied` decision component, and `output.suggested_price_eur`/`output.effective_margin` recomputed against the override price (§4). `process_broadcast_element` resolves an incoming `ManualOverrideRow` into the broadcast state, keyed by `f"{apartment_id}:{target_date}"`.
- `streaming/flink-jobs/src/flink_jobs/job.py` — new `manual_overrides` `KafkaSource`, a new `_parse_manual_override_row` parser, `MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR`, and the new Stage C hop wired between Stage B's output (`price_decisions`, re-keyed by `apartment_id`) and the DynamoDB sink map — **after** `price_decisions.get_side_output(DATA_STALE_TAG)` is retrieved, so the freshness-watchdog side output is unaffected by this phase (§7's ordering note).
- `streaming/flink-jobs/src/flink_jobs/settings.py` — `manual_overrides_topic: str` (same pattern as `owner_contracts_topic`).
- `streaming/flink-jobs/src/flink_jobs/dynamodb_sink.py` — **no changes.** `_python_to_dynamodb` already converts `None` to `{"NULL": True}` recursively (line 65-66), so a nullable `calculation.manual_override` flows through the existing generic serializer unmodified.

**Shared schema / event contract:**
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — new `ManualOverrideDetails` model (`override_price_eur`, `reason`, `authorized_by`, `valid_until`, `expected_loss_eur`); `ReasonCode` gains `"manual_override_applied"` (9 values); `Calculation` gains `manual_override: ManualOverrideDetails | None = None`. **`libs/pricing-formulas` is not touched** — this is a human action layered on top of the algorithmic decision, not itself a Revenue Management rule, so it does not belong in the engine's own `ReasonCode`/`decide_price()` pipeline (§4).
- `specs/events/price_decision.v1.json` — `calculation.manual_override` (new, nullable object, default `null`); the shared `decision_component` `$defs` entry's `code` enum gains the 9th value.

**Lakehouse / dbt:**
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — one new **optional** (`required=False`) `NestedField` group for `manual_override` (IDs 61-66: the struct itself plus its 5 scalar children) — the first optional nested struct this schema has ever had; every prior nested field here has been `required=True` (§7).
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — a new `_manual_override(calculation: dict) -> dict | None` helper (mirrors `_decision_components`'s coercion style): returns `None` when `calculation.get("manual_override")` is `None` or absent (every record predating this phase, and every record for a night with no active override), else a coerced dict (`_num`/`_ts` on the numeric/timestamp fields).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `calculation.manual_override` (pure schema evolution, nullable struct column).

**Contracts / tests:**
- 3 existing `specs/contracts/fixtures/price_decision/*.json` gain `calculation.manual_override: null` (no override — kept minimally changed, same convention Phase 11 followed for `commission_base`).
- 1 new fixture, `specs/contracts/fixtures/price_decision/manual_override_active.json`, exercising the populated shape (all 5 `manual_override` fields, plus the `manual_override_applied` component present in `decision_components`).
- Unit tests: `stage_manual_override_enrichment.py` (new test file, mirrors `test_stage_owner_contract_enrichment.py`'s shape: absent override → pass-through, present-and-valid → override applied with correct `expected_loss_eur` sign, present-but-expired → pass-through), `shared_schemas` contract tests (both fixture shapes), `lakehouse-consumer` (schema migration, `_manual_override` helper for both `None` and populated inputs).
- **Live LocalStack verification is a required acceptance criterion (AC-09)** — this phase adds a new Postgres table, a new Debezium-captured table, a new Kafka topic, and a new Flink stage chained after Stage B; per Phase 11's own precedent (§2), that combination is exactly what unit tests alone can't fully vouch for.

### Out of scope (explicitly deferred)

- UI/API authoring path, role-based authorization, `DELETE`-as-retraction, multi-night/range overrides, `below_market_by` recomputation, and override history — all listed in §1's "Not in this phase," not repeated here.
- **State TTL for the override broadcast state.** An override whose `valid_until` has passed is correctly ignored (§4) but never evicted from broadcast state — it just sits inert. `docs/post-poc-roadmap.md` §4's learning note (State TTL, flagged when Phase 9/LOS was picked up) applies here too and is not re-solved by this phase (§7).
- **Backlog #2/#8 (per-channel pricing, channel gross-up).** Independent of this phase; no ordering dependency either way (per-header `Blocks`).

---

## 3. Data model

### `manual_overrides` (new)

```sql
CREATE TABLE IF NOT EXISTS public.manual_overrides (
    apartment_id         TEXT NOT NULL
                              REFERENCES public.apartment_market_segments(apartment_id),
    target_date          DATE NOT NULL,
    override_price_eur   NUMERIC(10,2) NOT NULL
                              CHECK (override_price_eur >= 0),
    reason               TEXT NOT NULL,
    authorized_by        TEXT NOT NULL,
    valid_until          TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ,
    PRIMARY KEY (apartment_id, target_date)
);
```

One row per `(apartment_id, target_date)` — the natural key, and already Debezium's default message key (no `message.key.columns` override needed, §2). Reuses the existing `dbz_publication`/connector; routed to `manual-overrides.v1`. Cancelling an active override is an `UPDATE ... SET valid_until = now()`, not a `DELETE` (§1, §7) — `updated_at`'s trigger (same pattern as `owner_contracts`) makes that visible on read.

### `ManualOverrideAssignment` (Stage C broadcast state, new)

| Field | Type | Source |
|---|---|---|
| `override_price_eur` | `float` | `manual_overrides.override_price_eur` |
| `reason` | `str` | `manual_overrides.reason` |
| `authorized_by` | `str` | `manual_overrides.authorized_by` |
| `valid_until` | `datetime` | `manual_overrides.valid_until` |

Keyed in broadcast state by `f"{apartment_id}:{target_date.isoformat()}"` — a composite string key, the same convention `stage_price_decision.py`'s own `nights` `MapState` already uses for date-keyed lookups.

### `price_decision.v1` — one new nullable field on `calculation`

| Field | Type | Description |
|---|---|---|
| `manual_override` | object \| `null` (5 required fields when present) | Populated iff an unexpired override applied to this decision (ADR-0011 backlog #9). |

`ManualOverrideDetails`:

| Field | Type |
|---|---|
| `override_price_eur` | `float` (≥0) |
| `reason` | `string` |
| `authorized_by` | `string` |
| `valid_until` | `datetime` |
| `expected_loss_eur` | `float` (≥0) |

`los_floor_matrix` is **not** touched — a `LosFloorCandidate` never carries `manual_override`, same "decision-level context lives only on the top-level `Calculation`, never duplicated per LOS candidate" convention Phase 10/11 already established for property/commission components.

---

## 4. Logic

`stage_manual_override_enrichment.py` (Stage C, chained after Stage B):

```python
def process_element(self, value: PriceDecision, ctx):
    key = f"{value.apartment_id}:{value.target_date.isoformat()}"
    assignment = ctx.get_broadcast_state(MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR).get(key)
    if assignment is None or datetime.now(UTC) > assignment.valid_until:
        yield value  # no active override — pass through unchanged
        return
    yield _apply_override(value, assignment)

def process_broadcast_element(self, value: ManualOverrideRow, ctx):
    key = f"{value.apartment_id}:{value.target_date.isoformat()}"
    ctx.get_broadcast_state(MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR).put(key, value.to_assignment())
```

`_apply_override` (pure function, unit-testable without Flink fakes):

```python
def _apply_override(decision: PriceDecision, assignment: ManualOverrideAssignment) -> PriceDecision:
    total_cost_eur = (
        decision.cost_inputs.fixed_cost_eur
        + decision.cost_inputs.variable_cost_eur
        + decision.cost_inputs.one_time_cost_eur
    )
    new_effective_margin = (
        (assignment.override_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )
    expected_loss_eur = round(
        max(0.0, decision.calculation.minimum_price_eur - assignment.override_price_eur), 2
    )
    override_component = DecisionComponent(
        code="manual_override_applied",
        label=(
            f"Manual override by {assignment.authorized_by}: {assignment.reason} "
            f"(published {assignment.override_price_eur} EUR instead of the "
            f"{decision.output.suggested_price_eur} EUR algorithmic suggestion)"
        ),
        impact=round(assignment.override_price_eur - decision.output.suggested_price_eur, 2),
    )
    return decision.model_copy(update={
        "calculation": decision.calculation.model_copy(update={
            "manual_override": ManualOverrideDetails(
                override_price_eur=assignment.override_price_eur,
                reason=assignment.reason,
                authorized_by=assignment.authorized_by,
                valid_until=assignment.valid_until,
                expected_loss_eur=expected_loss_eur,
            ),
            "decision_components": [*decision.calculation.decision_components, override_component],
        }),
        "output": decision.output.model_copy(update={
            "suggested_price_eur": assignment.override_price_eur,
            "effective_margin": round(new_effective_margin, 4),
        }),
    })
```

**Deliberately unchanged by an active override** (§1, §7): `calculation.rule_applied` (kept as the algorithm's own, informational classification — "what would have applied absent the override," the same "orthogonal axis" reasoning Phase 12 used to keep `floor_policy` separate from `rule_applied`); `calculation.los_floor_matrix` (§3); `output.below_market_by` (kept as the algorithm's own market comparison, not recomputed against the override).

`expected_loss_eur` is `max(0, floor − override_price)`, not the raw (possibly negative) difference — an override priced **at or above** the floor is still audited (it's still a human overriding the algorithm's own suggestion, e.g. to force a price up for a strategic reason) but registers zero expected loss, matching spec §13's framing that "expected loss" is specifically the below-floor case, not every override.

### Worked example

Algorithmic decision: `minimum_price_eur=142.00`, `output.suggested_price_eur=130.00` (a `minimum_floor` `rule_applied` case, i.e. floor already binding), `cost_inputs` totalling `100.00 EUR`. An active override: `override_price_eur=110.00`, `reason="Pre-event availability push"`, `authorized_by="ops@bilemon.example"`.

| Field | Before override | After override |
|---|---|---|
| `output.suggested_price_eur` | 130.00 | 110.00 |
| `output.effective_margin` | 0.30 | 0.10 |
| `calculation.manual_override.expected_loss_eur` | — | `max(0, 142.00 - 110.00) = 32.00` |
| `manual_override_applied` component `impact` | — | `110.00 - 130.00 = -20.00` |
| `calculation.rule_applied` | `minimum_floor` | `minimum_floor` (unchanged) |

---

## 5. Acceptance criteria

- **AC-01 — No active override → pass-through.** A `PriceDecision` for an `(apartment_id, target_date)` with no broadcast entry is yielded byte-for-byte unchanged (`calculation.manual_override is None`).
- **AC-02 — Expired override → pass-through.** A broadcast entry whose `valid_until` is in the past does not alter the decision — same as AC-01.
- **AC-03 — Active override → correctly applied.** `output.suggested_price_eur` equals `override_price_eur`; `output.effective_margin` is recomputed against it; `calculation.manual_override` carries the correct `reason`/`authorized_by`/`valid_until`/`expected_loss_eur`.
- **AC-04 — `expected_loss_eur` is correctly clamped at zero.** Verified for an override below the floor (positive loss) and one at/above the floor (zero loss) — both branches of `max(0, ...)`.
- **AC-05 — `manual_override_applied` component's sign is correct.** Negative `impact` when the override lowers the price versus the algorithmic suggestion, positive when it raises it, both unit-tested.
- **AC-06 — Unaffected fields stay unaffected.** `calculation.rule_applied`, `calculation.los_floor_matrix`, and `output.below_market_by` are bit-identical before/after an override is applied (§4).
- **AC-07 — `price_decision.v1` validates both shapes.** Contract test covers `calculation.manual_override: null` (regression) and the fully-populated shape (new fixture).
- **AC-08 — lakehouse-consumer handles the new optional struct.** `_manual_override()` returns `None` for both a missing key (pre-Phase-14 record) and an explicit `null` (post-Phase-14 record, no active override that day) — same value, verified from two different input shapes; returns the coerced dict when populated.
- **AC-09 — Live LocalStack verification (required).** Insert a `manual_overrides` row (via `psql`, per the new `docs/manual/MANUAL.md` walkthrough) for a real seeded apartment/night; confirm, without restarting the Flink job: (a) the `manual-overrides.v1` topic actually carries the CDC message (raw Kafka console-consumer read); (b) that night's live DynamoDB `price_decision` item shows the overridden `suggested_price_eur` and populated `manual_override` block; (c) `fct_price_decision` via `dbt show` carries the same; (d) updating `valid_until` to a past timestamp causes the **next** reprice of that apartment/night (triggered by any new cost or market event, per the existing recalculation triggers) to revert to the algorithmic price; (e) Flink reports zero exceptions with Stage C active.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `_apply_override()` (AC-03–AC-06) — a plain function, no Flink fakes needed, same testing tier `pricing_formulas`' own functions use.
- **Flink stage, fakes only:** `ManualOverrideEnrichmentFunction.process_element`/`process_broadcast_element` (AC-01, AC-02), using the same `FakeBroadcastContext`/`FakeReadOnlyContext` fixtures `test_stage_owner_contract_enrichment.py` already established.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-07), Postgres migration behavior against a volume fixture that predates this table.
- **Infrastructure-adjacent, no LocalStack:** lakehouse-consumer schema-migration and `_manual_override()` tests (AC-08).
- **Live LocalStack (required, AC-09):** full bring-up per the updated `docs/manual/MANUAL.md`; insert, verify, then expire an override against a real seeded apartment, confirming the full round trip end to end.

---

## 7. Known limitations

- **Broadcast state grows without bound.** An expired override's entry is correctly ignored (§4) but never evicted — same class of concern `docs/post-poc-roadmap.md` §4 already flagged for Stage A/B's hand-rolled eviction, worth revisiting with Flink's native State TTL rather than solved ad hoc here.
- **Cancelling an override requires an `UPDATE`, not a `DELETE`** (§1) — a `DELETE` is silently dropped by the existing global `delete.handling.mode: "drop"` Debezium setting, project-wide. Documented in `docs/manual/MANUAL.md`'s new walkthrough so this isn't rediscovered as a bug.
- **`rule_applied` does not gain a 4th value for "overridden."** An active override is detected solely by `calculation.manual_override is not None` — kept this way to avoid widening an enum several other places (`floor_policy_for`, guardrail tests) already pattern-match on exhaustively; adding a value there would be a wider blast radius than this phase's actual scope.
- **This is the first optional (`required=False`) nested struct in the lakehouse schema.** Every prior `NestedField` addition (Phase 8-13) has been `required=True`. If a future phase needs another optional nested field, this phase's `schema.py`/`transform.py` changes are the precedent to follow, not Phase 10/11/12's.
- **No authorization enforcement, no UI** (§1) — this phase proves the mechanism (write → CDC → broadcast → repriced, audited), not a production-ready override workflow.

---

## 8. Follow-ups

- **State TTL** — if backlog #1 (LOS)'s own deferred State TTL work is ever picked up, extend it to this phase's broadcast state too, rather than treating them as separate problems.
- **`docs/post-poc-roadmap.md`** — backlog #9 moves from "Now" (spec drafted) to "Done" once this phase is implemented, unit-tested, and live-verified.
- **An authoring UI/API**, if this project's scope ever grows beyond a streaming/CDC learning PoC into something a real Property Manager would touch directly — explicitly out of scope here (§1), but this phase's `manual_overrides` table and Stage C are the correct foundation for one, needing no rework to add a write API in front of the same table.

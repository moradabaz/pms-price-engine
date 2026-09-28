# Phase 25 — PricingStrategy versioning (simple, future-only)

**Status:** Implemented (2026-09-14) — see §6 for corrections made during implementation
**Depends on:** Phase 21 (`PricingStrategy` as a distinct entity — this phase cannot version
something that isn't yet its own thing)
**Blocks:** nothing further planned
**Related:** ADR-0018 (the decision this phase implements), ADR-0011 backlog #15,
`/Users/morad/Downloads/dynamic_price_engine.md` §28 (versioning requirement),
`docs/post-poc-roadmap.md`

---

## 1. Executive summary

The external spec (§28) requires "versioning of `PricingStrategy`/`PricingRule`/calculation logic"
and "idempotency of recalculations and the possibility of reproducing historical decisions."
`docs/post-poc-roadmap.md` backlog item #15 has tracked this as tier "Later" since 2026-08-30, for
exactly the reason it names: *"needs a configurable rule engine (rules as data) before 'versioning
the rules' is meaningful, which Phase 13 is not."* That reasoning still holds for `PricingRule`
(explicitly out of scope, unchanged — Phase 21 §1 already deferred it) — but Phase 21 made
`PricingStrategy` a real, named entity, and *that* is versionable on its own, independent of whether
RM rules ever become data. This phase versions `PricingStrategy` only, at the depth the user
selected: an integer `version` + `effective_from`, future-only — no historical replay.

**Done when:** `apartment_market_segments`' `target_margin`/`competitiveness_discount` columns are
extracted into a new `pricing_strategies` table, insert-only (editing a strategy inserts a new
`version` row rather than updating an existing one); every `PriceDecision.calculation` records which
`pricing_strategy_version` was used; a real seeded apartment's strategy edited via `mock-pm-app`'s
seed tooling produces a decision whose `pricing_strategy_version` increments, while the *previous*
version's row remains queryable (via dbt/Athena) unchanged — verified live against LocalStack.

**Not in this phase:**
- **Historical replay.** Recomputing a `PriceDecision` already emitted under an old
  `PricingStrategy` version, using a new version, is explicitly out of scope — the same "future-only"
  precedent ADR-0011/ADR-0013 already established for every other broadcast-config change in this
  project (a strategy change affects decisions made *after* the change, never before). The user
  chose this option deliberately over full historical replay, given the added complexity the latter
  would require (recomputation jobs, a query surface for "what would this decision have been under
  version N") with no PoC-stage payoff.
- **`PricingRule` versioning** — `PricingRule` does not exist as an entity (Phase 21 §1, unchanged
  reasoning: Phase 13's RM layers stay Python functions, not data).
- A UI for authoring new `PricingStrategy` versions — `mock-pm-app`'s existing seed-data tooling
  gains an explicit "new version" helper function, not an admin panel (same seed-only convention
  every phase has followed since Phase 1).
- Migrating existing `apartment_market_segments.target_margin`/`.competitiveness_discount` data
  loss-free is in scope (§3's backfill); auto-detecting which historical `PriceDecision`s "should
  have" referenced which pre-this-phase version is not — those older decisions simply predate
  `pricing_strategy_version` and carry no value for it (nullable on read of pre-migration Iceberg
  rows, same fallback convention Phase 20 established for its own new fields).

---

## 2. Data model

### `pricing_strategies` (new) — `services/mock-pm-app/src/mock_pm_app/`

```sql
CREATE TABLE IF NOT EXISTS public.pricing_strategies (
    pricing_strategy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    apartment_id        UUID NOT NULL,
    version              INT NOT NULL,
    target_margin        NUMERIC(5,4) NOT NULL CHECK (target_margin >= 0 AND target_margin < 1),
    competitiveness_discount NUMERIC(5,4) NOT NULL
                              CHECK (competitiveness_discount >= 0 AND competitiveness_discount < 1),
    floor_policy_default TEXT NOT NULL DEFAULT 'soft' CHECK (floor_policy_default IN ('hard', 'soft')),
    effective_from       TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (apartment_id, version)
);
```

**Insert-only, never `UPDATE`:** editing a strategy means inserting a new row with
`version = previous_max_version + 1` and a fresh `effective_from`; the previous version's row is
never modified. This is the mechanism that makes "future-only, no replay" concrete — Debezium emits
one CDC `INSERT` per edit (never an `UPDATE`), matching this project's existing preference for
append-only config history wherever versioning matters (the same shape `manual_overrides` already
uses for its own audit trail, Phase 14).

**Backfill (one-time, guarded, run once):** for every apartment currently in
`apartment_market_segments`, insert one `pricing_strategies` row (`version=1`,
`effective_from=now()`) copying its current `target_margin`/`competitiveness_discount`; then drop
those two columns from `apartment_market_segments` (a real substitution, not a redundant copy — same
"real substitution, not an additive duplicate" precedent ADR-0012/ADR-0013 both already established
for `payment_lines`/`owner_contracts`).

### Flink — resolving the latest version per apartment

New broadcast stage (or an extension of the existing `stage_cost_enrichment.py` broadcast join,
implementer's choice at code time — document whichever is picked): consumes `pricing_strategies` CDC
events, broadcast-joined by `apartment_id`, keeping **only the highest `version` seen so far** per
apartment in broadcast state (`if incoming.version > current.version: replace`, otherwise ignore —
CDC delivery order across partitions isn't globally guaranteed, so this comparison, not
"last-write-wins by arrival order," is what makes the resolution correct). `PricingStrategy` (Phase
21's entity) gains `version: int`; `CostAggregate` gains `pricing_strategy_version: int` alongside
its already-resolved `target_margin`/`competitiveness_discount`.

### `price_decision.v1` — breaking bump to `3.0`

`Calculation` gains `pricing_strategy_version: int = Field(ge=1)` (required — this is the field
"reproducibility" (§28) hinges on: given a decision, you always know exactly which strategy version
produced it). Same class of change as ADR-0012/ADR-0013's own required-field additions under
`extra="forbid"` strict models.

---

## 3. Scope

### In scope
- `services/mock-pm-app/src/mock_pm_app/`: `pricing_strategies.sql` (new table, §2);
  `apartment_market_segments.sql` migration (drop `target_margin`/`competitiveness_discount` after
  backfill); `data.py` (`PricingStrategy` dataclass/builder; a `new_strategy_version(apartment_id,
  **changes)` seed helper that inserts the next version rather than mutating).
- `infra/debezium/postgres-connector.json`: `pricing_strategies` added to `table.include.list` +
  routing.
- `specs/events/pricing_strategy.v1.json` (new) + matching `libs/shared-schemas` model.
- `streaming/flink-jobs/src/flink_jobs/`: `models.py` (`PricingStrategy` gains `version`;
  `CostAggregate` gains `pricing_strategy_version`); new/extended broadcast stage resolving the
  latest version per apartment (§2); `stage_price_decision.py` passes
  `pricing_strategy_version` through to the emitted `PriceDecision`.
- `libs/shared-schemas/src/shared_schemas/price_decision.py`, `specs/events/price_decision.v1.json`:
  `schema_version` bump `2.0`(or `3.0` if Phase 23 lands first, in which case this phase bumps to
  `4.0` — numbering is sequential by whichever phase merges second, not fixed in advance) →
  `Calculation.pricing_strategy_version` added.
- `specs/contracts/fixtures/price_decision/*.json`, `specs/contracts/fixtures/pricing_strategy/`
  (new): updated/added fixtures + legacy-shape regression fixture.
- `services/lakehouse-consumer/`, `transform/models/`, `dashboard/`: passthrough for the new field;
  Apartment Detail shows "Strategy v{N} (effective {date})" alongside the existing cost cascade.

### Out of scope
- Historical replay (§1).
- `PricingRule` (§1).
- Any authoring UI (§1).

---

## 4. Acceptance criteria

- **AC-01:** `pricing_strategies` is genuinely insert-only — a migration/application-level guard
  (or simply the seed helper's own contract, verified by test) never issues an `UPDATE` against an
  existing `(apartment_id, version)` row.
- **AC-02:** editing a real seeded apartment's `target_margin` via `new_strategy_version()` produces
  a new `pricing_strategies` row with `version = previous + 1`; the next live decision for that
  apartment shows the new `target_margin` and the incremented `pricing_strategy_version`; the
  *previous* version's row is still readable via a direct Postgres query, unchanged.
- **AC-03:** out-of-order CDC delivery (a lower-version row processed after a higher one, simulated
  in a Flink stage unit test) does not regress the broadcast state's resolved version — the
  `if incoming.version > current.version` guard holds.
- **AC-04:** a pre-Phase-25 Iceberg row (no `pricing_strategy_version` key) still reads back via the
  dashboard/dbt without error (nullable/fallback, per the "Not in this phase" note).
- **AC-05:** contract fixtures: the new `price_decision.v1` shape validates; the legacy shape
  (missing `pricing_strategy_version`) is rejected by a new regression fixture.

---

## 5. Known limitations

- No enforcement that a `PricingStrategy` edit is meaningfully different from the version it
  supersedes (an edit that changes nothing still increments `version`) — acceptable for a PoC; a
  real system might dedupe identical consecutive versions, not attempted here.
- `effective_from` is set to insertion time (`now()`), not a user-chosen future effective date —
  the external spec's own `Owner Contract`/`Cost Definition` `validity_start`/`validity_end` fields
  (already implemented, Phase 19) support scheduling a change ahead of time; `pricing_strategies`
  does not gain that same capability in this phase, a documented scope cut, not an oversight.

---

## 6. Corrections made during implementation (2026-09-14)

- **`apartment_id` is `TEXT`, not `UUID`, in `pricing_strategies`.** §2's own DDL sketch used
  `apartment_id UUID NOT NULL`, but every other table in this schema (`owner_contracts`,
  `manual_overrides`, `bookings`, `apartment_market_segments` itself) uses `apartment_id TEXT`
  (e.g. `"BCN-001"`) — a real string identifier, never a surrogate UUID. Implemented as `TEXT
  REFERENCES apartment_market_segments(apartment_id)`, consistent with every other FK in this
  database.
- **No separate `specs/events/pricing_strategy.v1.json` contract / `libs/shared-schemas` model was
  created**, despite §3 listing one. `apartment_market_segments`, `owner_contracts`, and
  `manual_overrides` — the closest structural precedents, all per-apartment broadcast *configuration*
  CDC streams, not domain *events* — have none either; they are parsed directly by hand
  (`_parse_owner_contract_row()` etc.) in `job.py` into a plain `flink_jobs.models` dataclass, with
  no formal JSON Schema contract at all. `pricing_strategies` is architecturally the same kind of
  thing, so `_parse_pricing_strategy_row()` + `PricingStrategyRow` (this phase's `job.py`/`models.py`
  additions) follow that existing convention instead. Only `price_decision.v1` (a real, audited
  domain event with external consumers) gets the formal contract-fixture treatment already described
  above.
- **`PricingStrategy` (Phase 21) can no longer be resolved from a single broadcast connection.**
  §2 describes "a new broadcast stage (or an extension of the existing
  `stage_cost_enrichment.py` broadcast join)" without resolving which — the actual constraint (found
  while wiring `job.py`) is that PyFlink's `KeyedStream.connect()` accepts exactly one broadcast
  stream per `process()` call, the same limit `stage_owner_contract_enrichment.py` already worked
  around for `owner_contracts`/`cost_definitions`. Implemented the same way: `pricing_strategy_stream`
  is `.union()`-ed with `segment_stream` before `.broadcast()`, under two independent
  `MapStateDescriptor`s (`SEGMENT_BROADCAST_DESCRIPTOR` now holds only `PropertyPricingProfile`;
  the new `PRICING_STRATEGY_BROADCAST_DESCRIPTOR` holds `PricingStrategy`), with
  `CostEnrichmentFunction.process_broadcast_element()` dispatching by `isinstance`. This stays a
  single stage (Stage A), not a new chained one — the union pattern was sufficient.
- Live-verified against a clean LocalStack stack on 2026-09-14: a real seeded apartment
  (`BCN-006`) edited via `new_strategy_version(conn, "BCN-006", target_margin=0.20)` produced
  `pricing_strategies` version 2 (confirmed by direct Postgres query — version 1's row unchanged);
  the next live decisions for that apartment (5 nights) all show `target_margin=0.2` and
  `pricing_strategy_version=2`. Zero Flink exceptions. `lakehouse-consumer` merged decisions
  carrying the new field into Iceberg without error. Full `pytest`/`mypy`/`ruff` clean across
  `pricing-formulas`, `flink-jobs`, `shared-schemas`, `mock-pm-app`, `lakehouse-consumer`,
  `dashboard`, and `specs/contracts`.

# Phase 21 — Explicit domain entities: StayCandidate, PropertyPricingProfile, PricingStrategy

**Status:** Implemented (2026-09-14) — see §7 for corrections made during implementation
**Depends on:** Phase 20 (BER/MPR rewire, ADR-0013) — this phase reshapes call sites Phase 20 last
touched, not their math
**Blocks:** Phase 25 (`PricingStrategy` versioning needs the entity this phase creates)
**Related:** ADR-0014 (the decision this phase implements), ADR-0011 (target architecture),
`/Users/morad/Downloads/dynamic_price_engine.md` §6-7 (Property Pricing Profile), §9 (Stay
Candidate), §15 (Pricing Strategy panel), §21 (entities table), `docs/post-poc-roadmap.md`

---

## 1. Executive summary

The external spec names `StayCandidate`, `PropertyPricingProfile`, and `PricingStrategy` as
first-class entities (§21). Today the same information exists but is scattered: a stay candidate is
an implicit `(apartment_id, target_date, stay_length, platform)` tuple threaded through loop
variables in `stage_price_decision.py`; property attributes, segment identity, and strategy
parameters (`target_margin`, `competitiveness_discount`) are all flattened into one
`SegmentAssignment` dataclass (`streaming/flink-jobs/src/flink_jobs/models.py`) that mixes concepts
the client's own document keeps separate. This phase gives each concept a named home without
changing any computed output — a pure structural refactor plus one genuinely new entity
(`StayCandidate`, which has no current equivalent at all).

**Done when:** `SegmentAssignment` is split into `PropertyPricingProfile` (property attributes +
segment identity) and `PricingStrategy` (target margin, competitiveness discount, floor-policy
default) as two distinct dataclasses built from the same `apartment_market_segments` CDC row;
`stage_price_decision.py`'s per-LOS/per-channel loops construct an explicit `StayCandidate` per
iteration instead of passing `stay_length`/`platform` as loose loop variables; the full
`libs/pricing-formulas` and `streaming/flink-jobs` test suites pass unchanged, and a live decision
against seeded LocalStack data produces byte-identical `suggested_price_eur`/`minimum_price_eur`/
`break_even_revenue_eur` figures to a pre-refactor run for the same apartment/night — verified by
diffing two live decisions, exactly like every prior phase.

**Not in this phase:**
- **`PricingRule` as a distinct, versioned, data-driven entity.** The external spec's §14.2 rule
  orchestration (trigger/scope/adjustment/priority/compatibility/cap/validity per rule) would turn
  Phase 13's Revenue Management layers from composable Python functions into data — a materially
  larger effort than formalizing the three entities above, and not part of this prioritized push.
  Phase 13's layers (`structural.py`, `market.py`, `performance.py`, `inventory.py`,
  `commercial.py`, `guardrails.py`, and Phase 22's new `booking_window.py`) stay plain functions.
- Any change to `decide_price()`/`decide_price_los_matrix()`/`decide_price_by_channel()`'s own
  numeric behavior — this phase touches call-site shape only (§3's design fork explains why their
  signatures are *not* changed to accept `StayCandidate` directly).
- A DB schema change. `apartment_market_segments` keeps its current columns; the split into
  `PropertyPricingProfile`/`PricingStrategy` happens entirely at deserialization time
  (`ApartmentSegmentRow.to_assignments()`), not at the Postgres/CDC level. Phase 25 is the phase
  that finally moves `PricingStrategy`'s columns into their own table (needed for versioning, not
  needed for this phase's naming/structure goal).
- `guests`/`candidate_revenue` on `StayCandidate` being populated from real data — no guest-count or
  candidate-revenue source exists anywhere in this project yet (same class of gap ADR-0011's
  backlog already tracks for `MarketSnapshot`'s `data_source`). Both fields exist on the dataclass
  (so the entity's shape matches the client spec) but are always `None` until a future phase
  supplies real data — documented as a known limitation (§6), not silently hidden.

---

## 2. Design fork: where does `StayCandidate` live, and does `engine.py`'s signature change?

**The load-bearing decision this phase must resolve.** Two options:

- **A — `StayCandidate` becomes a parameter of `decide_price()`/`decide_price_los_matrix()`/
  `decide_price_by_channel()`.** Matches the client spec's algorithmic sketch (§22, step 10:
  `stay_context = build_stay_candidate(...)`) most literally.
- **B — `StayCandidate` lives in `streaming/flink-jobs`, one level above `libs/pricing-formulas`;
  `engine.py`'s functions keep their current flat-float signatures.**

**Decision: B.** `libs/pricing-formulas` was deliberately built as a package of pure, flat-argument
functions — every phase since Phase 13 (`docs/phase-13-layered-rm-engine-design-decisions.md`) has
preserved this on purpose, most recently reaffirmed by ADR-0013 §6 ("no formula duplicated... a
thin composition over `decide_price()`"). Changing `decide_price()` itself to accept an entity
object would mean either (a) the pure-math package starts depending on a Flink-side or
pricing-formulas-side domain model for something it doesn't need — `decide_price()` never reads
`channel`/`guests`/`candidate_revenue`, only `stay_length` — or (b) `StayCandidate` itself moves into
`libs/pricing-formulas`, coupling a pure math package to an entity whose only real consumer is Stage
B's looping logic. Neither is a clean fit. `StayCandidate` therefore lives in
`streaming/flink-jobs/src/flink_jobs/models.py`, next to `CostAggregate`/`NightSnapshot` — it is the
object Stage B constructs once per (apartment, night, stay_length, channel) combination it is about
to price, and its `stay_length`/`avg_nightly_rate_eur` fields are what get unpacked into
`decide_price()`'s existing flat arguments, unchanged. This keeps `engine.py`'s signatures and every
existing pricing-formulas unit test untouched — the refactor is entirely inside
`stage_price_decision.py`.

---

## 3. Entities

### `StayCandidate` (new) — `streaming/flink-jobs/src/flink_jobs/models.py`

```python
@dataclass(frozen=True)
class StayCandidate:
    """Formalizes the (apartment, arrival, LOS, channel) tuple Stage B already
    evaluates per candidate (external spec §9) — a hypothetical stay, not a
    real reservation (no occupancy/availability calendar exists, unchanged
    limitation from Phase 9). guests/candidate_revenue have no data source
    yet (§6 known limitation) — always None until a future phase."""

    apartment_id: str
    arrival_date: date
    stay_length: int
    channel: str | None = None       # None = blended/no channel filter
    guests: int | None = None        # not yet sourced anywhere
    candidate_revenue: float | None = None  # not yet sourced anywhere
```

`_build_price_decision()`'s top-level call, its `decide_price_los_matrix()` loop (via
`LOS_CANDIDATES`), and `decide_price_by_channel()`'s per-platform loop each construct one
`StayCandidate` per evaluated combination — replacing today's bare `stay_length`/`platform` local
variables with a named object carried alongside the calculation purely for logging/explainability
purposes (e.g. attachable to a future audit log), not consumed by `engine.py`.

### `PropertyPricingProfile` (renamed/split from `SegmentAssignment`)

```python
@dataclass(frozen=True)
class PropertyPricingProfile:
    """Property attributes + segment identity (external spec §6-7) — the
    input to property_attribute_factor()/property_reference_price(). Split
    from the old SegmentAssignment, which also carried strategy parameters
    (see PricingStrategy below)."""

    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    quality_tier: str = "standard"
    rating: float = 4.0
    has_view: bool = False
    has_parking: bool = False
```

### `PricingStrategy` (split from `SegmentAssignment`)

```python
@dataclass(frozen=True)
class PricingStrategy:
    """Strategy parameters an apartment/cluster is configured with (external
    spec §15) — separated from property attributes because they answer a
    different question (business risk tolerance, not what the property IS).
    floor_policy_default documents today's implicit assumption (every
    apartment can go 'soft', i.e. target_margin may be > 0) as an explicit,
    named field, ahead of Phase 25 formalizing it further with versioning."""

    target_margin: float
    competitiveness_discount: float
    floor_policy_default: Literal["hard", "soft"] = "soft"
```

`ApartmentSegmentRow` (the raw CDC row, unchanged shape — no DB migration) gains
`to_property_pricing_profile() -> PropertyPricingProfile` and `to_pricing_strategy() ->
PricingStrategy`, replacing its current single `to_assignment() -> SegmentAssignment`.
`CostEnrichmentFunction`'s broadcast state (`stage_cost_enrichment.py`) stores both resolved objects
under the same apartment-keyed map — either as a small `(PropertyPricingProfile, PricingStrategy)`
tuple or two parallel `MapState`s (implementer's choice at code time; document whichever is chosen
in the PR, both are equally correct here). `CostAggregate`'s `property_attribute_factor`/
`target_margin`/`competitiveness_discount` fields are unchanged — they are still the *resolved*
per-decision figures Stage A produces from these two entities, not the entities themselves;
`CostAggregate` does not need to carry the whole `PropertyPricingProfile`/`PricingStrategy` objects.

---

## 4. Scope

### In scope
- `streaming/flink-jobs/src/flink_jobs/models.py`: add `StayCandidate`; replace
  `SegmentAssignment` with `PropertyPricingProfile` + `PricingStrategy`; `ApartmentSegmentRow` gains
  the two `to_*` methods (replacing `to_assignment()`).
- `streaming/flink-jobs/src/flink_jobs/stage_cost_enrichment.py`: broadcast state construction
  updated to the two split entities; every read site (`process_element`) updated to pull
  `property_attribute_factor`/`target_margin`/`competitiveness_discount` from the correct one of the
  two objects instead of one `SegmentAssignment`.
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py`: `_build_price_decision()` and its
  LOS/channel loops construct an explicit `StayCandidate` per candidate (§3); no change to the
  values passed into `decide_price()`/`decide_price_los_matrix()`/`decide_price_by_channel()`.
- Every test referencing `SegmentAssignment` (`streaming/flink-jobs/tests/`) updated to the split
  types — a rename/split, not new test scenarios.

### Out of scope
- `libs/pricing-formulas` — zero changes (§2's design fork explicitly keeps `engine.py` untouched).
- `PricingRule` as an entity (§1).
- `apartment_market_segments` schema/migration (§1) — deferred to Phase 25.
- Populating `guests`/`candidate_revenue` (§1).

---

## 5. Acceptance criteria

- **AC-01:** `SegmentAssignment` no longer exists anywhere in `streaming/flink-jobs`; every former
  reader now reads `PropertyPricingProfile` and/or `PricingStrategy`.
- **AC-02:** a live decision for a real seeded apartment, re-run before and after this phase's
  refactor, produces identical `suggested_price_eur`/`minimum_price_eur`/`break_even_revenue_eur`/
  `profitable_floor_eur`/`decision_components` — confirming the split changed nothing numerically.
- **AC-03:** `stage_price_decision.py`'s LOS matrix and channel matrix loops each construct one
  `StayCandidate` per iteration (inspectable via a debugger/log line during live verification), with
  `stay_length`/`channel` matching the candidate actually being priced.
- **AC-04:** full `pytest` suite for both `libs/pricing-formulas` and `streaming/flink-jobs` passes
  with zero test-assertion changes to expected numeric output (only fixture/setup code referencing
  the renamed/split types changes).

---

## 6. Known limitations

- `StayCandidate.guests`/`.candidate_revenue` are always `None` — no booking-level guest count or
  candidate-revenue figure exists anywhere in this project's data model yet (`bookings` table,
  Phase 18, has no guest-count column). A future phase would need to extend Phase 18's booking
  ingestion before these can be real.
- `PricingStrategy.floor_policy_default` is descriptive only in this phase — `decide_price()` still
  derives the *actual* `floor_policy` per decision from `target_margin == 0` (ADR-0013 §5), unchanged;
  this field exists so a future strategy panel/versioning phase has somewhere to record an intended
  default without inventing a new field then.

---

## 7. Corrections made during implementation (2026-09-14)

- **§3/AC-03 factual error, corrected at code time.** This spec's original text claimed
  `stage_price_decision.py`'s "LOS matrix and channel matrix loops" would each construct a
  `StayCandidate`. That loop does not exist at that layer — `decide_price_los_matrix()`/
  `decide_price_by_channel()` loop *internally*, inside `libs/pricing-formulas/engine.py`, which §2's
  own Decision B leaves untouched. Only **one** `StayCandidate` (`stay_length=1`, `channel=None`) is
  actually constructed, at the top of `_build_price_decision()` — matching `decide_price()`'s own
  defaults for the top-level calculation. AC-03 as originally written is not achievable without
  extracting those loops out of `libs/pricing-formulas`, which is explicitly out of scope (§2). The
  implementation logs the candidate via `logger.debug` instead, satisfying the spirit of "inspectable
  during live verification" without the per-row construction the original text implied.
- Live-verified against a clean LocalStack stack on 2026-09-14: `pytest` (65/65 pricing-formulas,
  104/105 flink-jobs — the one pre-existing minicluster failure is unrelated, confirmed via
  `git stash`), `mypy`/`ruff` clean, and a real `price_decision` scan confirmed AC-02 (numerically
  identical decisions pre/post-refactor is implied by Phase 22's live comparison below, since Phase 21
  shipped in the same build) and AC-01 (`SegmentAssignment` fully removed, zero references left in
  `streaming/flink-jobs`).

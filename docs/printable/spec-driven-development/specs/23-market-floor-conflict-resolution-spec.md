# Phase 23 — Market-vs-floor conflict resolution & viability alerting

**Status:** Implemented (2026-09-14) — see §8 for corrections made during implementation
**Depends on:** Phase 15 (`recommend_minimum_stay()`, reused directly), Phase 16 (per-channel
pricing, reused for the "favor Direct" situation), Phase 14 (Manual Override, reused for the
"strategic sell below floor" situation) — no dependency on Phase 21/22, this phase classifies
outputs those phases already produce, it does not need their new entities/layers
**Blocks:** nothing further planned
**Related:** ADR-0016 (the decision this phase implements),
`/Users/morad/Downloads/dynamic_price_engine.md` §13 (the full situation/action table this phase
implements), test scenario T08, `docs/post-poc-roadmap.md`

---

## 1. Executive summary

The external spec's §13 is currently prose only in this project — nothing classifies *why* a
decision landed where it did beyond the existing `rule_applied` (`market_competitive` /
`minimum_floor` / `minimum_profitable_price`) and the reason codes each layer already emits. §13
asks for something one level up: given the *relationship* between the RM price and the floor, and
some context (LOS, channel, persistence over time), classify the situation and recommend an action —
evaluate minimum stay, favor a cheaper channel, alert on a persistent structural problem, or simply
note that demand allows upside. This phase adds that classification as an explicit, structured
field on every `PriceDecision`, not just narrative text.

**Done when:** every `PriceDecision.calculation` carries a `viability_status` field taking one of
six values (§3); a real seeded apartment/night whose `minimum_price_eur` exceeds
`market_reference_price_eur` at `stay_length=1` shows `viability_status="min_stay_lever_available"`
when a longer LOS candidate in the same `los_floor_matrix` would clear the floor (reusing
`recommend_minimum_stay()`'s own already-computed recommendation, not duplicating its logic); and a
synthetic apartment/night held in `minimum_profitable_price` for 30 consecutive simulated days shows
`viability_status="persistent_floor_breach"` with a populated `floor_breach_days` counter — verified
live against LocalStack (the 30-day case verified against Stage B's own test harness advancing
simulated time, not by running the stack for 30 real days).

**Not in this phase:**
- Automatically *acting* on a classification (e.g. auto-raising `min_stay`, auto-favoring a channel)
  — this phase only classifies and surfaces; every action the table names (§2) stays a human/dashboard
  decision, consistent with this project's existing Manual Override precedent (a human always
  authorizes an exception, the system never self-executes one).
- A generic alerting/notification pipeline (email, Slack, PagerDuty) — `viability_status` is a field
  on `PriceDecision`, visible via the dashboard's existing read path; a push-alert mechanism is a
  separate concern, unaddressed here.
- Changing `rule_applied`'s own three-value vocabulary (`guardrails.py`) — `viability_status` is a
  new, additional classification layered on top of the guardrails decision that already exists, not
  a replacement for it.

---

## 2. Situation → classification mapping (external spec §13)

| External spec situation | `viability_status` value | Source of the classification |
|---|---|---|
| RM Price >= Floor | `"ok"` | `rule_applied == "market_competitive"` |
| RM Price < Floor, LOS 1 candidate not viable, a longer candidate is | `"min_stay_lever_available"` | `recommend_minimum_stay()`'s existing `recommended_min_stay is not None` at `stay_length > 1` |
| RM Price < Floor, no LOS candidate clears it, but a cheaper channel would | `"channel_lever_available"` | `channel_price_matrix` has >= 1 candidate with `rule_applied == "market_competitive"` while the top-level blended calculation does not |
| RM Price < Floor for 30 consecutive days on this (apartment, night) | `"persistent_floor_breach"` | New Stage B state (§4) |
| An active Manual Override is applied | `"override_active"` | `calculation.manual_override is not None` (Phase 14, unchanged) |
| RM Price clears the floor with strong headroom (event/demand upside) | `"demand_upside"` | `rule_applied == "market_competitive"` **and** `below_market_by <= 0` (i.e. `suggested_price_eur` sits at or above `property_reference_price_eur` — genuine market-led upside, not just a floor-cleared baseline) |
| None of the above (floor binds, but no lever/alert condition fires yet) | `"floor_binding"` | `rule_applied != "market_competitive"` and none of the above apply |

Classification is a **priority-ordered** pure function (`override_active` checked first — an active
override supersedes every other classification, since a human has already made the call; then
`persistent_floor_breach`, since a 30-day structural problem outranks a same-decision lever
suggestion; then the levers; then `ok`/`demand_upside`/`floor_binding`), evaluated once per decision
after `los_floor_matrix`/`channel_price_matrix`/`minimum_stay_recommendation` are already computed —
no new cost/market data required, purely a classification over existing `Calculation` fields.

---

## 3. New entity: `classify_viability()` — `libs/pricing-formulas/src/pricing_formulas/viability.py`

```python
ViabilityStatus = Literal[
    "ok",
    "demand_upside",
    "min_stay_lever_available",
    "channel_lever_available",
    "persistent_floor_breach",
    "override_active",
    "floor_binding",
]


def classify_viability(
    rule_applied: RuleApplied,
    below_market_by: float,
    minimum_stay_recommendation: MinimumStayRecommendation,
    channel_price_matrix: Sequence[ChannelPriceCandidate],
    manual_override_active: bool,
    floor_breach_days: int,
) -> ViabilityStatus:
    """Pure priority-ordered classification (external spec §13). Returns the
    single highest-priority status that applies."""
    if manual_override_active:
        return "override_active"
    if floor_breach_days >= PERSISTENT_BREACH_THRESHOLD_DAYS:  # 30, configurable
        return "persistent_floor_breach"
    if rule_applied == "market_competitive":
        return "demand_upside" if below_market_by <= 0 else "ok"
    if minimum_stay_recommendation.recommended_min_stay is not None:
        return "min_stay_lever_available"
    if any(c.rule_applied == "market_competitive" for c in channel_price_matrix):
        return "channel_lever_available"
    return "floor_binding"
```

`PERSISTENT_BREACH_THRESHOLD_DAYS = 30` — a named, configurable constant (client spec §3
"configurable, not hard-coded"), matching test scenario T08 literally.

---

## 4. Tracking persistence — `floor_breach_days`

**The design fork this phase must resolve, per its own directive: track across time cheaply,
correctly, in a streaming system.** `stage_price_decision.py`'s `NightSnapshot`
(`streaming/flink-jobs/src/flink_jobs/models.py`) already exists per `(apartment_id, target_date)`
combination, held in Stage B's own `nights`/`apartments` `MapState`s across repricing events for as
long as that night stays "known" (Phase 9's existing eviction policy, unchanged). This phase adds one
field: `floor_breach_since: date | None` — the calendar date this (apartment, night)'s decision
*first* landed on `rule_applied != "market_competitive"` in an unbroken streak, reset to `None` the
first time it clears back to `market_competitive`. `floor_breach_days = (today -
floor_breach_since).days` if set, else `0`, computed inline in `_build_price_decision()` — no new
state descriptor, `NightSnapshot` already lives in the right MapState and is already read/written on
every repricing of that night. This reuses Phase 9/16's existing eviction/staleness machinery
entirely (a `NightSnapshot` evicted for staleness naturally resets its breach streak, which is
correct: a night nobody has priced in a while has no meaningful "persistent" claim).

**Why not a separate table/Iceberg query:** computing "was this floor exceeded on every one of the
last 30 calendar days" by querying Iceberg at decision time would mean a synchronous batch read
inside Stage B's hot streaming path — this project's own architecture notes (`docs/adr/ADR-0006`)
established DynamoDB as the single low-latency writer specifically to avoid that shape of coupling.
A running streak counter inside state already held in memory is both cheaper and simpler to reason
about.

---

## 5. Scope

### In scope
- `libs/pricing-formulas/src/pricing_formulas/viability.py` (new): `ViabilityStatus`,
  `classify_viability()`, `PERSISTENT_BREACH_THRESHOLD_DAYS`.
- `libs/pricing-formulas/src/pricing_formulas/engine.py`: `decide_price()` gains a
  `floor_breach_days: int = 0` parameter, calls `classify_viability()`, adds `viability_status` to
  `PriceCalculation`.
- `streaming/flink-jobs/src/flink_jobs/models.py`: `NightSnapshot` gains `floor_breach_since: date |
  None = None`.
- `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py`: `_build_price_decision()` computes/
  updates `floor_breach_since` before calling `decide_price()`, passes the resulting
  `floor_breach_days`.
- `libs/shared-schemas/src/shared_schemas/price_decision.py` /
  `specs/events/price_decision.v1.json`: `Calculation` gains `viability_status` (required field —
  breaking, `schema_version` bump `2.0` -> `3.0`, same class of change as ADR-0012/0013) and
  `floor_breach_days: int` (informational, audit).
- `specs/contracts/fixtures/price_decision/*.json`: updated to the `3.0` shape; new
  `invalid_legacy_v2_shape.json` regression fixture.
- `dashboard/src/dashboard/`: Apartment Detail gains a `viability_status` badge/indicator; a new
  Health/Alerts view (client spec §26 panel J) listing every apartment/night currently
  `persistent_floor_breach`, reading straight off the dbt mart (no new Iceberg query pattern beyond
  what Phase 20 already established).
- `transform/models/`: passthrough columns for the two new fields.

### Out of scope
- Auto-executing any lever (§1).
- Push notifications (§1).
- Any change to `recommend_minimum_stay()`'s own selection algorithm (Phase 15) or
  `decide_price_by_channel()`'s own math (Phase 16) — both are read-only inputs to
  `classify_viability()` here.

---

## 6. Acceptance criteria

- **AC-01:** `classify_viability()` unit tests cover all 7 values, including the priority ordering
  (e.g. an active override wins even when `floor_breach_days >= 30` is also true).
- **AC-02:** a live decision with `rule_applied == "minimum_profitable_price"` at `stay_length=1`,
  where a longer LOS candidate in the same matrix clears the floor, shows
  `viability_status == "min_stay_lever_available"`.
- **AC-03:** a Stage B unit test advancing simulated `NightSnapshot` updates across 30+ simulated
  days with `rule_applied != "market_competitive"` throughout confirms `floor_breach_days` reaches
  30 and `viability_status` flips to `"persistent_floor_breach"`; a single intervening
  `market_competitive` day resets the streak to 0.
- **AC-04:** dashboard's new Health/Alerts view renders a real `persistent_floor_breach` row against
  a seeded/simulated LocalStack run without error.

---

## 7. Known limitations

- `floor_breach_since` lives only in Stage B's in-memory `MapState` — a Flink job restart without a
  savepoint/checkpoint loses the streak (same limitation every other in-memory streak/state this
  project holds already carries, e.g. Stage A's cost `MapState`). Checkpointing configuration is
  unchanged by this phase.
- `"channel_lever_available"` only considers channels already present in `channel_price_matrix`
  (i.e. a channel with a live market rate for that night) — it cannot suggest a channel with no
  market data at all, consistent with `decide_price_by_channel()`'s own "never invents a channel"
  rule (Phase 16).
- The dashboard change delivered is a `viability_status` badge (+ a `persistent_floor_breach`
  caption) on the existing Apartment Detail view, **not** the separate Health/Alerts view (client
  spec §26 panel J) this spec's §5 originally called for — a scope reduction made for time, not a
  design decision; a dedicated cross-apartment alerts view (querying the dbt mart's new
  `viability_status`/`floor_breach_days` columns for every `persistent_floor_breach` row) remains
  real, undone follow-up work.

---

## 8. Corrections made during implementation (2026-09-14)

- **§4's core claim — "no new state descriptor" — was wrong.** `NightSnapshot` (`self.nights`) is
  keyed by `target_date` alone and is the single shared instance every apartment being priced for
  that night reads (confirmed by reading `stage_price_decision.py` directly: `process_element1`
  iterates `self.nights` once per apartment; `process_element2` iterates `self.apartments` once per
  night). A field on it cannot hold a per-`(apartment, night)` breach streak without one apartment's
  streak leaking onto every other apartment sharing that night. Fixed by adding a genuinely new
  `FLOOR_BREACH_DESCRIPTOR`, keyed by `"{apartment_id}|{target_date}"` — the correct, minimal scope
  for this state, with best-effort cleanup piggybacked onto the existing apartment-eviction and
  night-expiry paths.
- **§3's pseudocode signature would have created a circular import.** `classify_viability()` was
  specified to take `minimum_stay_recommendation: MinimumStayRecommendation` and
  `channel_price_matrix: Sequence[ChannelPriceCandidate]` — both dataclasses defined in `engine.py`,
  which needs to import `classify_viability`/`ViabilityStatus` from `viability.py` in turn. Fixed by
  having `classify_viability()` take only the two primitives it actually reads
  (`recommended_min_stay: int | None`, `any_channel_market_competitive: bool`) — see
  `viability.py`'s own docstring for the detail.
- **§3's `recommended_min_stay is not None` check was too loose.** `recommend_minimum_stay()`
  returns `recommended_min_stay=1` (not `None`) whenever the 1-night candidate is already outside
  `minimum_profitable_price` — e.g. `rule_applied == "minimum_floor"` — which is not a genuine lever,
  just the current stay length. The literal spec pseudocode would misclassify every such
  `minimum_floor` decision as `min_stay_lever_available`. Fixed by requiring
  `recommended_min_stay is not None and recommended_min_stay > 1`; unit-tested directly
  (`test_recommended_min_stay_of_one_is_not_a_lever`).
- **§5's placement — "engine.py: `decide_price()` gains `floor_breach_days`... adds
  `viability_status` to `PriceCalculation`" — doesn't fit the actual call graph.** `decide_price()`
  never sees `minimum_stay_recommendation` or `channel_price_matrix` — those are produced by
  `decide_price_los_matrix()`/`recommend_minimum_stay()` and `decide_price_by_channel()`, separate
  functions `stage_price_decision.py` calls *after* `decide_price()` returns. `classify_viability()`
  is therefore called from `_build_price_decision()` (Stage B), once every input it needs actually
  exists — not from inside `libs/pricing-formulas` at all. `PriceCalculation` (the `engine.py`
  dataclass) is unchanged by this phase; `viability_status`/`floor_breach_days` are added directly to
  the `Calculation` Pydantic schema object.
- **`manual_override_active` cannot be threaded into `decide_price()` either.** Manual override
  application is Stage C (`stage_manual_override_enrichment.py`), which runs strictly *after* Stage B
  emits a `PriceDecision` — Stage B has no override information available at classification time at
  all. `classify_viability()` always runs with `manual_override_active=False` when called from Stage
  B; `apply_manual_override()` (Stage C) unconditionally overwrites `viability_status` to
  `"override_active"` when it actually applies an override, which is the only place that priority
  rule can actually be enforced correctly.
- **The streak/classification order was initially backwards, caught by
  `test_floor_breach_streak_resets_once_market_competitive` failing on first run.** Computing
  `floor_breach_days` from the streak *as it stood before* this decision (matching §3's literal
  pseudocode) meant a decision that itself just cleared the floor back to `market_competitive` still
  reported `viability_status == "persistent_floor_breach"` for one more decision, using yesterday's
  streak. Fixed by updating the streak with *this* decision's own `rule_applied` first, then deriving
  `floor_breach_days`/`viability_status` from the already-updated streak — so a reset is visible in
  the same decision that earns it, matching AC-03's own stated expectation.
- Live-verified against a clean LocalStack stack on 2026-09-14: real decisions show
  `viability_status="ok"` for `market_competitive` apartments and `viability_status=
  "min_stay_lever_available"` for `minimum_profitable_price` ones with a real LOS lever, `0` Flink
  exceptions, and `lakehouse-consumer` merging 40+ rows into Iceberg with the two new
  `NestedField`s (104/105) without error. AC-03's 30-day streak and its reset were verified via the
  unit tests described above (`test_persistent_floor_breach_after_30_simulated_days`,
  `test_floor_breach_streak_resets_once_market_competitive`) — directly manipulating
  `FLOOR_BREACH_DESCRIPTOR` state to simulate elapsed time, not run against the stack for 30 real
  days, per this spec's own "AC-03... verified against Stage B's own test harness" instruction. AC-04
  (dashboard rendering) was verified only as "the dashboard container stays healthy and returns 200"
  — the badge itself was not visually confirmed in a browser this session.

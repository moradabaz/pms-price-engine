# Post-PoC roadmap — deferred from ADR-0009

**Date:** 2026-08-03 (extended 2026-08-30, ADR-0011 — target architecture; corrected/extended 2026-09-07)

Two items from the stakeholders' profitability model are **explicitly deferred until after the PoC ships**, to keep Phase 4's reform (ADR-0009) scoped to what's cheap: fixing the floor formula, adding commissions, splitting cost by category, and the antelación-tiered dual floor. Not silently dropped — tracked here so they get picked up as real follow-up work.

**2026-08-30 update:** a much richer external spec ("Profitable Dynamic Pricing Engine") confirmed items 1 and 2 below independently, and surfaced several more concepts with no equivalent in this repo today. ADR-0011 adopts that spec as the target design; §3 below is the full backlog it produced, §4 is a learning note to read before starting on it, and §5 sketches the LOS-aware floor increment (now **Phase 9** — Property Bonus/Malus was picked up first as **Phase 8**, [`specs/phases/08-property-bonus-malus/spec.md`](../specs/phases/08-property-bonus-malus/spec.md), since this repo numbers phases by actual implementation order, not by backlog tier).

**Progress:** backlog #6 (Property Bonus/Malus, Phase 8), #1 (LOS floor matrix, Phase 9), #4 (Decision Components, [Phase 10](../specs/phases/10-decision-components/spec.md)), #5 (Owner Contract, [Phase 11](../specs/phases/11-owner-contract/spec.md)), #10 (Floor Policy, [Phase 12](../specs/phases/12-floor-policy/spec.md)), #7 (Layered RM engine, [Phase 13](../specs/phases/13-layered-rm-engine/spec.md)), #9 (Manual Override, [Phase 14](../specs/phases/14-manual-override/spec.md)), #12 (min-stay lever, [Phase 15](../specs/phases/15-minimum-stay-recommendation/spec.md)), and #2 (per-channel pricing, [Phase 16](../specs/phases/16-per-channel-pricing/spec.md)) are implemented, unit-tested, and live-verified against LocalStack.

**2026-09-07 correction/extension** (see `docs/profitable-pricing-glossary.md` for the plain-language version of every concept named below): a re-read of the external spec against the current code corrected two backlog rows (#3, #11) and surfaced one new item (#12, min-stay lever) with real client value and no new-data cost. Priority for the next tranche is driven by client value (a real BiLemon Property Manager's trust and margin), not by cheapest-first: **#12 (min-stay lever) → #9 (Manual Override, spec drafted as [Phase 14](../specs/phases/14-manual-override/spec.md)) → #2 (per-channel) → #8 (channel gross-up) → #11 (real market data) → rest of #3.** #9 is promoted from "Later" to "Now" — its spec is drafted, not yet implemented — because it is the first change that shifts the system from "the machine decides" to "the machine assists, a human can veto," which matters more to client trust than any precision improvement to the algorithm itself.

**2026-09-12 update:** the last item in that list, #3, is now Done — see item 3 below and [Phase 17](../specs/phases/17-cost-concept-breakdown/spec.md).

**2026-09-13 update:** #13 (`CostDefinition`/`CostAllocationRule`) and #14 (Company Costs) are now
Done — see items 13/14 below, [Phase 18](../specs/phases/18-bookings-occupancy/spec.md), and
[Phase 19](../specs/phases/19-cost-definition-model/spec.md) (ADR-0012). This was a much bigger
schema-breaking change than #3's own additive `cost_breakdown` — `payment_line.v1` bumped to
schema_version 2.0.

**2026-09-14 update — prioritized push toward ~80% client-spec coverage:** a gap analysis against
the full external spec (`docs/client-spec-gap-analysis.md`) put overall MVP coverage at ~65-70%.
Five phases are now spec'd (Draft) as the highest-value, lowest-risk items to close that gap without
touching the two most expensive remaining gaps (real market/comp-set data, external §17; a
configurable data-driven rule engine for `PricingRule`, §14.2) — both stay deferred. In dependency
order: [Phase 21](../specs/phases/21-explicit-domain-entities/spec.md) (`StayCandidate`/
`PropertyPricingProfile`/`PricingStrategy` as named entities, ADR-0014) →
[Phase 22](../specs/phases/22-booking-window-layer/spec.md) (the missing RM layer D, ADR-0015) →
[Phase 23](../specs/phases/23-market-floor-conflict-resolution/spec.md) (external spec §13's
situation/action table as a `viability_status` classification, ADR-0016) →
[Phase 24](../specs/phases/24-owner-contract-revenue-bases/spec.md) (the remaining 2 of 5 owner
contract revenue bases, ADR-0017) → [Phase 25](../specs/phases/25-pricing-strategy-versioning/spec.md)
(simple, future-only `PricingStrategy` versioning, ADR-0018, depends on Phase 21).

**2026-09-14 update (same day) — Phase 21 and Phase 22 implemented and live-verified.** Both
corrected two factual errors discovered only once code/tests actually ran (see each spec's own §7):
Phase 21's AC-03 assumed a per-LOS/channel `StayCandidate` construction that doesn't exist at that
layer; Phase 22's own AC-01 tier-value table was arithmetically inconsistent with its own
`BOOKING_WINDOW_TIERS`, and `days_to_arrival`'s original default (`0`) silently discounted every
caller that omitted it, breaking 11 existing tests. Both fixed at implementation time, full
`pytest`/`mypy`/`ruff` clean, live-verified end to end against a clean LocalStack stack (real
`price_decision` rows show `rule_last_minute` appearing/disappearing exactly at the correct
`days_to_arrival` tier boundary, with `break_even_revenue_eur`/`profitable_floor_eur` unchanged
across tiers). Phases 23-25 remain Draft, not yet implemented.

**2026-09-14 update (same day) — Phase 23 implemented and live-verified.** Corrected several
design-time errors surfaced only once code/tests ran (full account in
[Phase 23's own §8](../specs/phases/23-market-floor-conflict-resolution/spec.md#8-corrections-made-during-implementation-2026-09-14)):
`NightSnapshot` cannot hold the per-`(apartment, night)` breach streak the spec assumed (it is one
shared instance per night, across every apartment) — a genuinely new state, keyed by
`"{apartment_id}|{target_date}"`, was added instead; `classify_viability()` takes primitives, not
the `MinimumStayRecommendation`/`ChannelPriceCandidate` dataclasses the spec's pseudocode used
(avoiding a circular import between `engine.py` and the new `viability.py`); it is called from
`stage_price_decision.py`, not from inside `decide_price()` (which never sees
`minimum_stay_recommendation`/`channel_price_matrix` — those come from separate function calls
Stage B makes afterward); `manual_override_active` is always `False` at classification time (Stage
C, which applies overrides, runs strictly after Stage B) — Stage C itself now overwrites
`viability_status` to `"override_active"` directly. Live-verified against a clean LocalStack stack:
real decisions carry the correct `viability_status`, zero Flink exceptions, Iceberg accepting the
two new fields without error. Scope reduction: the dashboard change shipped is a badge on the
existing Apartment Detail view, not the separate Health/Alerts view (spec §26 panel J) originally
scoped — a real, undone follow-up. Phases 24-25 remain Draft.

**2026-09-14 update (same day) — Phase 24 implemented and live-verified.** Extended
`RevenueBase`/`netted_revenue_base_amount()` to the remaining 2 of 5 external-spec bases (§11.1),
added `laundry` as a new `CostDefinition.concept`, and a new `booking_scope_cost_eur` aggregate (the
first cost aggregate in this project keyed by `scope` rather than `behavior`). Corrections found
during implementation (full account in
[Phase 24's own §7](../specs/phases/24-owner-contract-revenue-bases/spec.md#7-corrections-made-during-implementation-2026-09-14)):
the two new `netted_revenue_base_amount()` parameters needed defaults to avoid breaking every
existing caller (same class of fix Phase 22 needed for `days_to_arrival`); `decide_price_by_channel()`
also needed both new parameters threaded through, not just the top-level call site; `payment_line.v1`
needed no enum change at all (Phase 19 already removed `concept` from it); no self-healing `ALTER`
previously existed for `cost_definitions.revenue_base`'s CHECK constraint, so this phase's migration
is the first one, not a widening of an existing one. Live-verified and hand-checked against real
seeded data: `revenue_minus_all_booking_costs` nets exactly an apartment's own booking-scope cost
lines; a pre-existing `revenue_minus_ota` apartment correctly excludes its own cleaning/laundry lines
(regression confirmed). Zero Flink exceptions.

**2026-09-14 update (same day) — Phase 25 implemented and live-verified. All 5 prioritized phases
now done.** `PricingStrategy` extracted into its own insert-only `pricing_strategies` table/CDC
stream — every decision now records `pricing_strategy_version`. Corrections found during
implementation (full account in
[Phase 25's own §6](../specs/phases/25-pricing-strategy-versioning/spec.md#6-corrections-made-during-implementation-2026-09-14)):
`apartment_id` is `TEXT` not `UUID` (matching every other table in this schema); no separate
`pricing_strategy.v1` event contract was created — `pricing_strategies` is a broadcast
*configuration* CDC stream like `owner_contracts`/`apartment_market_segments`, which have none
either (only real domain events like `price_decision.v1` get the formal contract treatment);
PyFlink's `KeyedStream.connect()` accepting exactly one broadcast stream per stage meant
`PricingStrategy` resolution required unioning the new stream with `segment_stream` under two
descriptors (the same pattern `stage_owner_contract_enrichment.py` already established), not a new
chained stage. Live-verified: editing apartment `BCN-006`'s strategy via `new_strategy_version()`
produced version 2 without touching version 1's row, and the next live decisions for that apartment
correctly carried the new `target_margin` and `pricing_strategy_version=2`. Zero Flink exceptions.

price_decision.v1 is now at schema_version 4.0 (2.0 → 3.0 Phase 23 → 4.0 Phase 25). All 5 phases
from the 2026-09-14 prioritized push (21-25) are implemented and live-verified; see
`docs/client-spec-gap-analysis.md` for the resulting coverage re-assessment against the external
spec (not yet re-run since Phase 25 landed).

## 1. Real stay-length pricing (`n` beyond the fixed `1`)

Price a whole candidate stay (several consecutive nights), not one isolated night — so a one-time cost (`Cr`, e.g. cleaning) amortizes correctly across the stay.

**Why deferred:** no phase of this project models an actual reservation with a length. Doing this for real means:
- `price_decision` keyed by `(apartment_id, start_date, stay_length)` instead of `(apartment_id, target_date)` — a primary-key change in DynamoDB (spec §10) and in what Phase 6's dashboard reads.
- Multiple candidate stay lengths (1, 2, 3, 7, 14 nights) evaluated per apartment/night → multiplies Stage B's emission volume.
- No real occupancy/availability calendar exists anywhere (already a known limitation, spec §14) — "possible stay" can't be validated against real availability.

## 2. Per-channel pricing (direct / Airbnb / Booking / Vrbo) — **Done ([Phase 16](../specs/phases/16-per-channel-pricing/spec.md))**

Separate floor and commission per sales channel, instead of one blended `Cp`.

**Why originally deferred (now resolved by Phase 16):**
- `market-ingestor` (Phase 3) produces one blended average rate, no channel dimension — needs a Phase 3 change to ingest/derive per-channel rates. **Resolved:** `market-ingestor` now also publishes 3 channel-specific `MarketPrice` events per segment per tick (`CHANNEL_PRICE_MULTIPLIERS`), alongside the unchanged blended event.
- Apartment config would need per-channel commission, not a single `commission_pct` — a Phase 1 schema change. **Resolved cheaply:** a fixed code constant (`CHANNEL_COMMISSION_PCT`, one value per platform) rather than a new owner/channel contract table — a real `owner_channel_commissions` table is deferred further (see Phase 16 spec §8).
- Multiplies Stage B's fan-out again, this time by number of channels. **Resolved:** Stage B still emits exactly one `PriceDecision` per (apartment, night) per triggering event — `channel_price_matrix` is an embedded list on the existing decision (the Phase 9 LOS-matrix precedent), not new rows.

Backlog #8 (channel gross-up economics) is now unblocked.

## When to pick these up

After the PoC's current reform (ADR-0009) ships and is verified live. Whoever starts this should read ADR-0009's "Alternatives considered and rejected" section first — the reasoning for deferring is there, not just the fact of it.

## 3. Full backlog (ADR-0011 — target architecture)

Every concept from the external spec, mapped to what it would extend or replace in this repo, a priority tier, and whether it's a cheap additive change (new column, same shape) or a structural one (new PK, new top-level model, new entity, new fan-out cardinality). Tiers: **Now** = already a named, reasoned deferral with code that anticipates it. **Next** = little-to-no new data needed, unlocks later tiers. **Later** = needs new data the mock services don't generate, a net-new entity with no existing anchor, or is blocked on a Next-tier item.

| # | Concept | Current-repo anchor | Tier | Change type |
|---|---|---|---|---|
| 1 | LOS / Stay Candidate + floor matrix | `STAY_LENGTH_NIGHTS = 1` in `pricing.py` — this file's item 1 | **Done** — [Phase 9](../specs/phases/09-los-floor-matrix/spec.md) | Structural (embedded matrix inside the existing decision — no DynamoDB PK change needed, see Phase 9 pre-spec §A) |
| 2 | Per-channel pricing/commission | Flat `commission_pct`; `market_price.v1.market_context.platform` (always `null`) — this file's item 2 | **Done** — [Phase 16](../specs/phases/16-per-channel-pricing/spec.md), unit-tested and live-verified against LocalStack on 2026-09-08 | Additive (commission constant, `platform` wiring) + structural (embedded `channel_price_matrix`, no new fan-out at the emission level) |
| 3 | Read `concept` (13 values, incl. `ota_fee`) in cost aggregation | **Done** — [Phase 17](../specs/phases/17-cost-concept-breakdown/spec.md): all 13 values now surface via `cost_inputs.cost_breakdown`, a per-concept/per-day reporting breakdown (display-only — doesn't change `decide_price()`'s fixed/variable/one_time inputs; that remains backlog #13's job) | **Done** | Additive — zero upstream schema change, as predicted |
| 4 | Decision Components / structured reason codes | `rule_applied`/`floor_type` (closed enums), no list field anywhere in `PriceDecision` | **Done** — [Phase 10](../specs/phases/10-decision-components/spec.md) | Structural — new `decision_components` list on `Calculation`/`LosFloorCandidate`, following Phase 9's list-field precedent |
| 5 | Owner contract / configurable commission base (Total Revenue vs. Revenue−OTA vs. …) | No anchor — no owner/contract entity exists | **Done** — [Phase 11](../specs/phases/11-owner-contract/spec.md) | Structural (net-new entity) |
| 6 | Property Bonus/Malus + Property Reference Price | `market-ingestor/segments.py`'s fixed per-segment multiplier (0.7/1.0/1.45) — not per apartment | **Done** — [Phase 8](../specs/phases/08-property-bonus-malus/spec.md) | Structural at the source (mock-app needs new attribute columns), additive in Flink once the data exists |
| 7 | Layered Revenue Management engine (structural/market/performance/booking-window/inventory/commercial/guardrails) | ADR-0009 D4's antelación tier table — the only existing analog, and only for one layer | **Done** — [Phase 13](../specs/phases/13-layered-rm-engine/spec.md) | Structural — rewrite of `pricing.py`'s single if/elif chain into a composable pipeline; the point at which the formulas move into `libs/pricing-formulas` (§6) if that extraction hasn't happened already |
| 8 | Channel gross-up economics | No anchor; depended on #2 | **Now/Next** — unblocked now that #2 ([Phase 16](../specs/phases/16-per-channel-pricing/spec.md)) is done | Additive on top of `channel_price_matrix` |
| 9 | Manual Override + audit trail | No anchor — the pipeline is read-only downstream of Flink | **Done** — [Phase 14](../specs/phases/14-manual-override/spec.md), unit-tested and live-verified against LocalStack on 2026-09-07 (insert, apply, dbt passthrough, and expiry-based revert, all without a Flink restart) | Structural (new write path: new Postgres table + CDC + a third Flink stage, broadcast-joined onto `PriceDecision` after Stage B) |
| 10 | Explicit Hard/Soft floor policy | Mostly already covered by `floor_type`/`rule_applied` | **Done** — [Phase 12](../specs/phases/12-floor-policy/spec.md) | Additive (relabeling) |
| 11 | Real market data / comp-set (real occupancy, competitor rates) | 18 static segments, `occupancy_rate = rng.uniform(0.45, 0.85)`. **Corrected 2026-09-07:** cheaper than "Later" suggests — `MarketPrice.pricing` already carries `p25/p50/p75` (comp-set percentiles) and `MarketContext.data_source` already anticipates real values (`scraped_airbnb`, `scraped_booking`); the README's own architecture diagram already names Inside Airbnb as `market-ingestor`'s intended real source, never wired up. The gap is the data source only, not the model | **Later** (external integration), but re-scope as **Next** if Inside Airbnb (a free, local, already-downloadable dataset) is chosen over a live scraper | Additive at the edge (`market-ingestor` only) once a real source is chosen |
| 12 | Minimum Stay as a profitability lever (spec §10.2, test scenario T11) | No anchor (`grep min_stay` → 0 results) — Phase 9's `los_floor_matrix` is read passively; nothing recommends or applies a `min_stay` change when a single night is uneconomical | **Done** — [Phase 15](../specs/phases/15-minimum-stay-recommendation/spec.md), unit-tested and live-verified against LocalStack on 2026-09-07 | Additive — pure logic over `los_floor_matrix`, which Phase 9 already computes; no new data source, natural extension of Phase 9/13 |
| 13 | Multi-dimensional `CostDefinition`/`CostAllocationRule` (scope/behavior/trigger/calculation-base/recurrence/allocation-rule/validity, spec §8) | `PaymentLine.cost_type: fixed\|variable\|one_time` is the only cost dimension modeled today; temporalization of e.g. an annual cost is inferred from `billing_period_start/end`, not from an explicit `recurrence`/`allocation rule` | **Done** — [Phase 19](../specs/phases/19-cost-definition-model/spec.md) (ADR-0012), plus [Phase 18](../specs/phases/18-bookings-occupancy/spec.md) as its bookings/occupancy prerequisite. Live-verified end to end on 2026-09-13. Still additive at the `libs/pricing-formulas` boundary — `decide_price()` itself isn't rewired onto this richer model yet, tracked as a follow-on ("Phase 20" in-session, not yet a numbered repo phase) | Structural (new entities, breaking `payment_line.v1` schema change — see ADR-0012) |
| 14 | Company Costs imputables (spec §8.3) | No anchor (`grep company_cost` → 0 results); no `scope: company` dimension exists | **Done** — folded into [Phase 19](../specs/phases/19-cost-definition-model/spec.md): `company_cost_occurrences` + `scope=company` + an explicit `weight_config` (never `Total / N` implicitly), broadcast-applied per apartment (Stage A4) | Structural (new entity + explicit allocation-rule config, not `Total / N` by default) |
| 15 | Versioned `PricingStrategy`/`PricingRule` + reproducible historical decisions (spec §28) | [Phase 13](../specs/phases/13-layered-rm-engine/spec.md)'s 7 RM layers are composable Python functions, not versioned/stored rules; only event `schema_version` exists today, no rule/strategy version | **Now** — `PricingStrategy` half spec'd as [Phase 25](../specs/phases/25-pricing-strategy-versioning/spec.md) (ADR-0018, simple version field, future-only, depends on [Phase 21](../specs/phases/21-explicit-domain-entities/spec.md)); `PricingRule` versioning stays **Later** — still needs a configurable rule engine (rules as data) before "versioning the rules" is meaningful, which Phase 13 deliberately is not (ADR-0014 §3) | Structural (new `pricing_strategies` table, insert-only) for the `PricingStrategy` half; `PricingRule` half unchanged/deferred |
| 16 | Currency/channel-aware rounding rules (spec §28) | Mono-currency (`EUR` `Literal`) today; rounding is `round(x, 2)` scattered inline, no configurable policy | **Later** — surfaced 2026-09-07; low priority while the PoC is EUR-only | Additive, whenever a second currency/channel is introduced |
| 17 | Explicit domain entities: `StayCandidate`, `PropertyPricingProfile`, `PricingStrategy` (spec §9, §6-7, §15, §21) | Logic exists but scattered — an implicit tuple in `stage_price_decision.py`'s loops, and `SegmentAssignment` conflating property attributes with strategy parameters | **Now** — [Phase 21](../specs/phases/21-explicit-domain-entities/spec.md) (ADR-0014) | Structural (new `StayCandidate` type; `SegmentAssignment` split) but explicitly no DB schema change, no `libs/pricing-formulas` signature change |
| 18 | Booking Window Revenue Management layer (spec §14.1 row D) | No anchor — `grep booking_window` only finds the antelación-floor file ADR-0013 deleted, an unrelated concept | **Now** — [Phase 22](../specs/phases/22-booking-window-layer/spec.md) (ADR-0015), the gap ADR-0013's own Consequences section named directly | Structural (new layer module) — additive to `decide_price()`'s signature (`days_to_arrival` re-added as an RM-only default-0 argument, floor math untouched) |
| 19 | Market-vs-floor conflict resolution & viability alerting (spec §13, test T08) | No anchor — `rule_applied` exists but nothing classifies the situation or tracks persistence over time | **Now** — [Phase 23](../specs/phases/23-market-floor-conflict-resolution/spec.md) (ADR-0016) | Structural (new `viability_status` field, breaking `price_decision.v1` bump; new in-memory streak on `NightSnapshot`, no batch query added to the hot path) |
| 20 | Remaining owner-contract revenue bases (spec §11.1, test T04/T05) | `RevenueBase` supports 3 of 5 (`total_revenue`, `revenue_minus_ota`, `revenue_minus_ota_minus_cleaning`) | **Now** — [Phase 24](../specs/phases/24-owner-contract-revenue-bases/spec.md) (ADR-0017); a general symbolic solver (spec §29) stays explicitly deferred | Additive (2 new enum values + 2 new cost aggregates: `laundry_cost_eur`, `booking_scope_cost_eur`) |

## 4. Learning note: Flink concepts worth studying before Phase 9 (LOS)

This project's learning focus (README) is CDC and stateful stream processing, not just shipping the pricing formula — so before starting item #1, three streaming-systems concepts are worth deliberately studying, not just implementing around:

1. **State TTL (`StateTtlConfig`) instead of hand-rolled eviction.** Stage A's `MapState<event_id, PaymentLine>` (retaining only the current + prior billing period) and Stage B's `apartments`/`nights` `MapState`s plus their deadline maps for the freshness watchdog are all evicted by hand today — exactly what Flink's native State TTL (per-entry incremental cleanup) is built to do. Worth learning now because LOS is about to multiply the state these structures hold (~5 candidate stay lengths per night), and every hand-written eviction path is one more place for a bug like `error-handling/stage-b-cost-side-fanout-can-emit-for-already-past-nights.md` to recur.
2. **Native join operators (interval join, temporal table join) vs. the hand-rolled cross-join in Stage B.** `stage_price_decision.py` is a `KeyedCoProcessFunction` that manually cross-joins two `MapState`s (`process_element1`/`process_element2`). That's the right call for what it does today, but it's also exactly the shape of the fan-out explosion problem LOS is about to hit. Understanding Flink's built-in two-stream join primitives — even without necessarily switching to them — sharpens the judgment needed for the LOS design fork in §5 below.
3. **Precompute-at-write vs. compute-at-read.** The LOS design fork itself (§5, options A vs. B) is a specific instance of a general streaming-architecture tradeoff: push computation into the stream processor (more state, more fan-out, cheap reads) vs. push it to query time (less state, cheaper writes, logic moves out of Flink). Worth treating as a concept to learn deliberately, since it's currently unresolved in this project's own roadmap.

Lower priority, worth flagging for when item #7 (layered RM engine) is picked up rather than now: **Flink CEP** (pattern matching over streams) as a natural fit for compound rule conditions like "occupancy < 30% AND booking window < 3 days" — not urgent, since #7 is tier "Later."

## 5. LOS-aware floor — outline for Phase 9

Not a full spec — a sketch to work from once this is picked up (see "When to pick these up" above; same rule applies).

- **Stay Candidate, scoped down:** `(apartment_id, arrival_date, stay_length)`. No reservation entity needs to exist — a candidate is hypothetical, not an actual booking (this repo still has no occupancy/availability calendar, per §14 of the Phase 4 spec).
- **Candidate LOS set:** `{1, 2, 3, 7, 14}` nights — needs to be an explicit, documented config choice, not left implicit.
- **The open design fork (the load-bearing decision this phase must resolve — see §4's join/precompute-vs-read learning note above):** Stage B (`stage_price_decision.py`) already cross-join fans out — a cost update reprices every known night for that apartment, a market update reprices every known apartment for that night. `error-handling/anticipated-risks-flink-processing.md` (Risk 4/5) already measured this for a single LOS (~60 emissions per cost update, ~6 per market tick) and flagged it as "expected, but worth load-testing again after any load-profile change." Evaluating 5 LOS candidates per (apartment, night) multiplies both numbers by 5 (~300 and ~30 respectively). Two ways to resolve it:
  - **A — one `price_decision` row per LOS candidate.** Requires changing DynamoDB's key to `(apartment_id, target_date, stay_length)`, with ripple into Phase 5's Iceberg mirror and Phase 6's dashboard queries.
  - **B — compute the LOS floor matrix at read time** (dashboard/API), not precomputed/emitted per candidate by Flink. Avoids multiplying the fan-out, at the cost of moving the calculation out of Flink.
- **Sketch acceptance criteria:** `decide_price()` accepts `stay_length: int >= 1`, defaulting to `1` (must reproduce ADR-0009's existing worked examples unchanged); fan-out volume is re-measured against Risk 4/5's numbers before enabling more than one LOS candidate; the PK-vs-read-time decision (A vs. B above) is made explicit and justified in the spec, not implied by whichever gets coded first.
- **Sequencing note:** ship and measure LOS on its own before adding per-channel pricing (#2) on top — combining both at once makes the fan-out math from this section impossible to attribute to either change.

## 6. Architecture note: centralize pricing formulas in `libs/` — **Done** (via Phase 13)

**2026-09-12 correction:** this section described a plan, not a still-open gap — it's already been carried out. When [Phase 13](../specs/phases/13-layered-rm-engine/spec.md) (backlog #7) rewrote `pricing.py`'s if/elif chain into a composable pipeline, it also performed exactly the extraction this section proposed: `decide_price()`/`decide_price_los_matrix()`/`decide_price_by_channel()` and the 7 RM-layer modules all live in `libs/pricing-formulas/src/pricing_formulas/`, and `streaming/flink-jobs/src/flink_jobs/pricing.py` no longer exists — `flink-jobs` depends on `pricing-formulas` as a workspace package. `cost_aggregation.py` correctly stayed in `flink-jobs`, per this section's own reasoning below: it's stateful `MapState` orchestration, not pure math.

**Original note (kept for context):** the pure math used to be scattered by service — the floor formula in `flink_jobs/pricing.py`, market-side math (seasonal adjustment, synthetic sampling) in `services/market-ingestor/src/market_ingestor/pricing.py`/`seasonality.py` — three services, three copies of "where the formulas live," no shared home. The decision was to extract the Flink-side pure math into a shared package once item #7 rewrote it anyway, rather than restructure the same file twice.

**Still open, not part of what shipped:** `market-ingestor`'s synthetic market-data sampling (`pricing.py`/`seasonality.py`) was never migrated into `libs/pricing-formulas` — it's synthetic *input* generation for the mock ingestor, not pricing-decision math, so it doesn't duplicate anything `pricing_formulas` contains. Whether it belongs there too is an open scope question, not a bug.

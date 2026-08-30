# Post-PoC roadmap — deferred from ADR-0009

**Date:** 2026-08-03 (extended 2026-08-30, ADR-0011 — target architecture)

Two items from the stakeholders' profitability model are **explicitly deferred until after the PoC ships**, to keep Phase 4's reform (ADR-0009) scoped to what's cheap: fixing the floor formula, adding commissions, splitting cost by category, and the antelación-tiered dual floor. Not silently dropped — tracked here so they get picked up as real follow-up work.

**2026-08-30 update:** a much richer external spec ("Profitable Dynamic Pricing Engine") confirmed items 1 and 2 below independently, and surfaced several more concepts with no equivalent in this repo today. ADR-0011 adopts that spec as the target design; §3 below is the full backlog it produced, §4 is a learning note to read before starting on it, and §5 sketches the next concrete increment (LOS-aware floor).

## 1. Real stay-length pricing (`n` beyond the fixed `1`)

Price a whole candidate stay (several consecutive nights), not one isolated night — so a one-time cost (`Cr`, e.g. cleaning) amortizes correctly across the stay.

**Why deferred:** no phase of this project models an actual reservation with a length. Doing this for real means:
- `price_decision` keyed by `(apartment_id, start_date, stay_length)` instead of `(apartment_id, target_date)` — a primary-key change in DynamoDB (spec §10) and in what Phase 6's dashboard reads.
- Multiple candidate stay lengths (1, 2, 3, 7, 14 nights) evaluated per apartment/night → multiplies Stage B's emission volume.
- No real occupancy/availability calendar exists anywhere (already a known limitation, spec §14) — "possible stay" can't be validated against real availability.

## 2. Per-channel pricing (direct / Airbnb / Booking / Vrbo)

Separate floor and commission per sales channel, instead of one blended `Cp`.

**Why deferred:**
- `market-ingestor` (Phase 3) produces one blended average rate, no channel dimension — needs a Phase 3 change to ingest/derive per-channel rates.
- Apartment config would need per-channel commission, not a single `commission_pct` — a Phase 1 schema change.
- Multiplies Stage B's fan-out again, this time by number of channels.

## When to pick these up

After the PoC's current reform (ADR-0009) ships and is verified live. Whoever starts this should read ADR-0009's "Alternatives considered and rejected" section first — the reasoning for deferring is there, not just the fact of it.

## 3. Full backlog (ADR-0011 — target architecture)

Every concept from the external spec, mapped to what it would extend or replace in this repo, a priority tier, and whether it's a cheap additive change (new column, same shape) or a structural one (new PK, new top-level model, new entity, new fan-out cardinality). Tiers: **Now** = already a named, reasoned deferral with code that anticipates it. **Next** = little-to-no new data needed, unlocks later tiers. **Later** = needs new data the mock services don't generate, a net-new entity with no existing anchor, or is blocked on a Next-tier item.

| # | Concept | Current-repo anchor | Tier | Change type |
|---|---|---|---|---|
| 1 | LOS / Stay Candidate + floor matrix | `STAY_LENGTH_NIGHTS = 1` in `pricing.py` — this file's item 1 | **Now** | Structural (DynamoDB PK) |
| 2 | Per-channel pricing/commission | Flat `commission_pct`; `market_price.v1.market_context.platform` (always `null`) — this file's item 2 | **Now/Next** | Additive (commission column, `platform` wiring) + structural (fan-out) |
| 3 | Read `concept` (13 values, incl. `ota_fee`) in cost aggregation | `PaymentLine.concept`, ignored by `cost_aggregation.py` today | **Next** | Additive — zero upstream schema change |
| 4 | Decision Components / structured reason codes | `rule_applied`/`floor_type` (closed enums), no list field anywhere in `PriceDecision` | **Next** (prerequisite for #5, #7, #9) | Structural — every `PriceDecision` sub-model is `extra="forbid"`, no existing array to extend |
| 5 | Owner contract / configurable commission base (Total Revenue vs. Revenue−OTA vs. …) | No anchor — no owner/contract entity exists | **Later** | Structural (net-new entity) |
| 6 | Property Bonus/Malus + Property Reference Price | `market-ingestor/segments.py`'s fixed per-segment multiplier (0.7/1.0/1.45) — not per apartment | **Later** | Structural at the source (mock-app needs new attribute columns), additive in Flink once the data exists |
| 7 | Layered Revenue Management engine (structural/market/performance/booking-window/inventory/commercial/guardrails) | ADR-0009 D4's antelación tier table — the only existing analog, and only for one layer | **Later** | Structural — rewrite of `pricing.py`'s single if/elif chain into a composable pipeline; the point at which the formulas move into `libs/pricing-formulas` (§6) if that extraction hasn't happened already |
| 8 | Channel gross-up economics | No anchor; depends on #2 | **Later** | Additive once #2 exists |
| 9 | Manual Override + audit trail | No anchor — the pipeline is read-only downstream of Flink | **Later** | Structural (new write path) |
| 10 | Explicit Hard/Soft floor policy | Mostly already covered by `floor_type`/`rule_applied` | **Later** | Additive (relabeling) |
| 11 | Real market data / comp-set (real occupancy, competitor rates) | 18 static segments, `occupancy_rate = rng.uniform(0.45, 0.85)` | **Later** | Structural (external integration) |

## 4. Learning note: Flink concepts worth studying before Phase 8 (LOS)

This project's learning focus (README) is CDC and stateful stream processing, not just shipping the pricing formula — so before starting item #1, three streaming-systems concepts are worth deliberately studying, not just implementing around:

1. **State TTL (`StateTtlConfig`) instead of hand-rolled eviction.** Stage A's `MapState<event_id, PaymentLine>` (retaining only the current + prior billing period) and Stage B's `apartments`/`nights` `MapState`s plus their deadline maps for the freshness watchdog are all evicted by hand today — exactly what Flink's native State TTL (per-entry incremental cleanup) is built to do. Worth learning now because LOS is about to multiply the state these structures hold (~5 candidate stay lengths per night), and every hand-written eviction path is one more place for a bug like `error-handling/stage-b-cost-side-fanout-can-emit-for-already-past-nights.md` to recur.
2. **Native join operators (interval join, temporal table join) vs. the hand-rolled cross-join in Stage B.** `stage_price_decision.py` is a `KeyedCoProcessFunction` that manually cross-joins two `MapState`s (`process_element1`/`process_element2`). That's the right call for what it does today, but it's also exactly the shape of the fan-out explosion problem LOS is about to hit. Understanding Flink's built-in two-stream join primitives — even without necessarily switching to them — sharpens the judgment needed for the LOS design fork in §5 below.
3. **Precompute-at-write vs. compute-at-read.** The LOS design fork itself (§5, options A vs. B) is a specific instance of a general streaming-architecture tradeoff: push computation into the stream processor (more state, more fan-out, cheap reads) vs. push it to query time (less state, cheaper writes, logic moves out of Flink). Worth treating as a concept to learn deliberately, since it's currently unresolved in this project's own roadmap.

Lower priority, worth flagging for when item #7 (layered RM engine) is picked up rather than now: **Flink CEP** (pattern matching over streams) as a natural fit for compound rule conditions like "occupancy < 30% AND booking window < 3 days" — not urgent, since #7 is tier "Later."

## 5. LOS-aware floor — outline for a future Phase 8

Not a full spec — a sketch to work from once this is picked up (see "When to pick these up" above; same rule applies).

- **Stay Candidate, scoped down:** `(apartment_id, arrival_date, stay_length)`. No reservation entity needs to exist — a candidate is hypothetical, not an actual booking (this repo still has no occupancy/availability calendar, per §14 of the Phase 4 spec).
- **Candidate LOS set:** `{1, 2, 3, 7, 14}` nights — needs to be an explicit, documented config choice, not left implicit.
- **The open design fork (the load-bearing decision this phase must resolve — see §4's join/precompute-vs-read learning note above):** Stage B (`stage_price_decision.py`) already cross-join fans out — a cost update reprices every known night for that apartment, a market update reprices every known apartment for that night. `error-handling/anticipated-risks-flink-processing.md` (Risk 4/5) already measured this for a single LOS (~60 emissions per cost update, ~6 per market tick) and flagged it as "expected, but worth load-testing again after any load-profile change." Evaluating 5 LOS candidates per (apartment, night) multiplies both numbers by 5 (~300 and ~30 respectively). Two ways to resolve it:
  - **A — one `price_decision` row per LOS candidate.** Requires changing DynamoDB's key to `(apartment_id, target_date, stay_length)`, with ripple into Phase 5's Iceberg mirror and Phase 6's dashboard queries.
  - **B — compute the LOS floor matrix at read time** (dashboard/API), not precomputed/emitted per candidate by Flink. Avoids multiplying the fan-out, at the cost of moving the calculation out of Flink.
- **Sketch acceptance criteria:** `decide_price()` accepts `stay_length: int >= 1`, defaulting to `1` (must reproduce ADR-0009's existing worked examples unchanged); fan-out volume is re-measured against Risk 4/5's numbers before enabling more than one LOS candidate; the PK-vs-read-time decision (A vs. B above) is made explicit and justified in the spec, not implied by whichever gets coded first.
- **Sequencing note:** ship and measure LOS on its own before adding per-channel pricing (#2) on top — combining both at once makes the fan-out math from this section impossible to attribute to either change.

## 6. Architecture note: centralize pricing formulas in `libs/`

Today the pure math is scattered by service, not by concern: the floor formula lives in `streaming/flink-jobs/src/flink_jobs/pricing.py`, cost aggregation in `streaming/flink-jobs/src/flink_jobs/cost_aggregation.py`, and market-side math (seasonal adjustment, synthetic sampling) in `services/market-ingestor/src/market_ingestor/pricing.py`/`seasonality.py` — three services, three copies of "where the formulas live," no shared home.

**Decision:** when backlog item #7 (layered RM engine) is picked up — the point where `pricing.py` stops being one if/elif chain and becomes a composable pipeline anyway — extract the pure formula functions (break-even/floor calculation, and any RM-layer factor functions that follow) into a new shared package, `libs/pricing-formulas/` (same shape as `libs/lakehouse-shared`: a small installable package other services import rather than reimplement). This makes the formulas independently testable and reusable if a future service (dashboard-side what-if simulation, a batch reprice job) ever needs to evaluate the same math without going through Flink.

**Why not now:** item #1 (LOS) already changes `pricing.py`'s formula shape (adds a real `n`); doing the `libs/` extraction at the same time as item #7's bigger rewrite avoids restructuring the same file twice. Tracked here so it isn't lost between now and then, not because it's blocked on anything technical.

**Scope, when it happens:** floor/break-even math and RM-layer factor functions move to `libs/pricing-formulas`; `flink_jobs/pricing.py` and any future `market_ingestor` consumer become thin callers. Aggregation logic (`cost_aggregation.py`, which touches Flink `MapState`) stays in `flink-jobs` — it isn't pure math, it's stateful orchestration around the math.

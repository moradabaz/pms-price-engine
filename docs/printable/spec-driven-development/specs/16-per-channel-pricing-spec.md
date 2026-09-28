# Phase 16 — Per-Channel Pricing Matrix

**Status:** Implemented — all acceptance criteria (§5) verified, including live verification against a running LocalStack stack on 2026-09-08: `market-ingestor` confirmed publishing 4 `MarketPrice` events per segment per tick (1 blended + 3 channel, 72 records/tick for 18 segments); `lakehouse-consumer`/dbt passthrough confirmed on real accumulated data (1247 `fct_price_decision` rows, 965 with a non-empty `channel_price_matrix`, including nights with all 3 channels present); the dashboard's `marts.channel_pricing()` cross-checked against that same live data (correct per-channel `commission_pct` — 0.12/0.15/0.08 for airbnb/booking/vrbo — and correct empty-DataFrame shape when no channel data has arrived yet for a night).
**Depends on:** Phase 9 (`decide_price_los_matrix()`'s "thin composition over `decide_price()`" precedent), Phase 11 (`commission_base`/`commission_pct`, `netted_commission_amount()`), Phase 13 (`property_reference_price()`/`market_reference_price()`, the layers whose output actually varies per channel here)
**Blocks:** ADR-0011 backlog #8 (channel gross-up economics), which is explicitly stated as depending on this item existing
**Related:** [ADR-0011](../../../docs/adr/ADR-0011-profitable-pricing-target-architecture.md) (target architecture, backlog #2), [`docs/post-poc-roadmap.md`](../../../docs/post-poc-roadmap.md) (§1 item 2, §3 backlog #2/#8), [Phase 9 spec](../09-los-floor-matrix/spec.md) (the embedded-list-field precedent this phase reuses for the opposite reason — see §1)

---

## 1. Executive summary

`market_price.v1.market_context.platform` has existed since Phase 3 and has always been `null` in practice — `market-ingestor` only ever produces one blended average rate per segment/night. Every apartment is priced against that single blended figure, with one commission rate (`owner_contracts.commission_pct`, Phase 11) regardless of which channel a guest actually books through. In reality Airbnb, Booking.com and Vrbo have their own market rates and their own commission structures — a floor computed against a blended average can be wrong in both directions for any single real channel.

The roadmap's own framing of this item (`docs/post-poc-roadmap.md` §3, row #2) warns it "multiplies Stage B's fan-out again, this time by number of channels" — written before Phase 9 resolved the *exact same* fan-out fear for LOS by embedding a matrix inside the existing decision instead of emitting one row per candidate. That precedent mostly applies here too, **with one real difference worth being precise about**: LOS's candidates share an identical market side (only the cost floor varies with `stay_length`), so Phase 9 needed no new state. Here, **the market side itself varies per channel** (each platform has its own real rate) — so Stage B's per-night state has to hold one market snapshot per channel, not one. The fan-out that actually multiplies is `market-ingestor`'s own event volume (one blended event, same as today, plus one per channel); Stage B still emits exactly one `PriceDecision` per (apartment, night) per triggering event, unchanged from today.

**Done when:** every `price_decision.v1` event carries a `calculation.channel_price_matrix` — one entry per channel `market-ingestor` has reported a rate for on that night (`airbnb`, `booking`, `vrbo`; possibly fewer, never more) — each with its own `market_reference_price_eur`, floor, suggested price and effective margin, computed with that channel's own commission rate. The existing top-level `calculation` (today's blended/"direct" figure) is completely unaffected — zero regression risk, same discipline Phase 9/15 held to for their own additions.

**Not in this phase:**
- **Combining channel with LOS** (a 5×3 candidate cross-product). `decide_price_by_channel()` fixes `stay_length=1`, same way `decide_price_los_matrix()` fixes the market side constant — combining both axes in one phase would make a regression impossible to attribute to either change, the same sequencing warning the roadmap already gave for LOS-then-channel (§5 of the roadmap doc). A natural Phase 17, not required here.
- **A real per-apartment, per-channel commission contract.** `CHANNEL_COMMISSION_PCT` (§3) is a fixed code constant, one value per platform, applied to every apartment alike — the same "cheap first step, real table later" stance Phase 11 itself once took before `owner_contracts` existed. A future `owner_channel_commissions` table (structural: new entity, CDC, connector) is backlog #2's own "Later" refinement, not blocking here.
- **Channel gross-up economics** (backlog #8) — explicitly deferred until #2 exists, per ADR-0011 §2's own text; this phase is the prerequisite, not the gross-up itself.
- **Picking a winning/recommended channel, or a decision component explaining one.** Unlike Phase 15 (which recommends an action), this phase is pure audit/comparison data — the same posture Phase 9 took for `los_floor_matrix` before Phase 15 later acted on it. Acting on the channel matrix (e.g. "list this apartment as Airbnb-only") is a future phase, not this one.
- **A new Flink stage, Kafka topic, or Postgres table.** Everything here extends Stage B's existing state and `market-ingestor`'s existing tick loop — no new source, no new join.

---

## 2. Scope

### In scope

**`market-ingestor`:**
- `src/market_ingestor/pricing.py` — new `sample_channel_pricing(segment, platform, target_date, rng)`, parallel to `sample_pricing()`. Applies a per-platform multiplier to the same seasoned reference median `sample_pricing()` uses, drawn from a plausible spread (Airbnb typically prices a little above the blended average, Booking.com close to it, Vrbo a little below — same "plausible synthetic spread, not sourced from a specific report" caveat `_TARGET_COEFFICIENT_OF_VARIATION` already carries) — new constant `_CHANNEL_PRICE_MULTIPLIERS = {"airbnb": 1.08, "booking": 1.00, "vrbo": 0.92}`.
- `src/market_ingestor/events.py` — `build_market_price_event()` keeps producing today's blended event (`platform: None`) completely unchanged, plus a new `build_channel_market_price_events()` producing one additional `MarketPrice` per platform (`market_context.platform` set, `data_source` unchanged — still `"mock"`).
- `src/market_ingestor/main.py` — each tick emits the existing blended event **and** the 3 new per-channel events per segment (4 events/segment/tick instead of 1) — `market-ingestor`'s own Kinesis volume is what actually multiplies by channel count, not Stage B's.

**`libs/pricing-formulas`:**
- `src/pricing_formulas/engine.py`:
  - New `CHANNEL_COMMISSION_PCT: dict[str, float] = {"airbnb": 0.12, "booking": 0.15, "vrbo": 0.08}` constant (§1's "cheap first step" — plausible OTA commission figures, not sourced from a specific contract).
  - New `ChannelPriceCandidate` frozen dataclass: `platform`, `avg_nightly_rate_eur`, `commission_pct`, `market_reference_price_eur`, `minimum_price_eur`, `floor_type`, `floor_policy`, `rule_applied`, `suggested_price_eur`, `effective_margin`, `decision_components` — same shape discipline `LosFloorCandidate` established.
  - New `decide_price_by_channel(channel_rates_eur: dict[str, float], fixed_cost_eur, variable_cost_eur, one_time_cost_eur, target_margin, competitiveness_discount, days_to_arrival, property_attribute_factor=1.0, commission_base="total_revenue", ota_related_cost_eur=0.0, cleaning_cost_eur=0.0, channel_commission_pct=CHANNEL_COMMISSION_PCT) -> list[ChannelPriceCandidate]`. **Not** a pure post-processing pass the way `decide_price_los_matrix()` is (§4 explains why) — still a thin per-channel composition over `decide_price()`, no formula duplicated. Produces exactly one candidate per key present in `channel_rates_eur` — never invents a channel with no observed rate.

**Flink (`streaming/flink-jobs`):**
- `src/flink_jobs/models.py` — new `NightSnapshot` frozen dataclass wrapping the existing `MarketSnapshot`: `blended: MarketSnapshot | None` (today's single value, renamed in place — `None` only transiently, before this night's blended event has arrived) plus `channels: dict[str, MarketSnapshot]` (populated incrementally, independent of whether every channel has reported yet).
- `src/flink_jobs/stage_price_decision.py`:
  - `self.nights` MapState value type becomes `NightSnapshot` instead of bare `MarketSnapshot`. `process_element2` reads `value.market_context.platform`: `None` updates `.blended` (unchanged path/staleness check); a real platform value updates `.channels[platform]` (its own independent staleness check against that channel's own prior `collected_at`, not the blended one's).
  - The fan-out loops in `process_element1`/`process_element2` are **unchanged in shape** — still one `_build_price_decision()` per known (apartment, night) pair per triggering event; only what a `NightSnapshot` carries changed, not how many are iterated.
  - A **brand-new** night whose first-ever event is channel-only (blended not yet arrived) is stored but does not trigger a fan-out — same "known night" definition Stage B already uses (a night isn't "known" until its blended snapshot exists). In practice this window is one Kafka round-trip: `market-ingestor` emits all 4 events for a segment/tick together (§2 above).
  - `_build_price_decision()` calls `decide_price_by_channel()` once, alongside the existing `decide_price()`/`decide_price_los_matrix()` calls, passing `{platform: snapshot.avg_nightly_rate_eur for platform, snapshot in night.channels.items()}`.

**Shared schema / event contract:**
- `libs/shared-schemas/src/shared_schemas/price_decision.py` — new `ChannelPriceCandidate` Pydantic model (`extra="forbid"`, mirrors `LosFloorCandidate`'s shape plus `platform: Literal["airbnb","booking","vrbo"]`, `avg_nightly_rate_eur`, `commission_pct`); `Calculation` gains `channel_price_matrix: list[ChannelPriceCandidate]` (**required**, like `los_floor_matrix` — always computable, never null — but unlike it, legitimately allowed to be **empty** when no channel data has reached this night yet; no `minItems` floor).
- `specs/events/price_decision.v1.json` — `calculation.channel_price_matrix`: required array (`minItems: 0`), element shape matching the new Pydantic model; `platform` enum restricted to the 3 modeled channels (not `"mixed"` — that value belongs to `market_price.v1`'s blended case, which this matrix never represents).

**Lakehouse / dbt:**
- `services/lakehouse-consumer/src/lakehouse_consumer/schema.py` — new **required** `NestedField` list/struct group for `channel_price_matrix`, fresh IDs continuing from **73** (Phase 15's last-used ID was 72): struct id 73, element id 74, leaves 75–83 (`platform`, `avg_nightly_rate_eur`, `commission_pct`, `market_reference_price_eur`, `minimum_price_eur`, `floor_type`, `floor_policy`, `rule_applied`, `suggested_price_eur`, `effective_margin` — 10 leaves, so 75–84), nested `decision_components` list continuing after (85 struct/element, 86–88 leaves) — exact numbering finalized at implementation time following the module's own "append at the end, never renumber" rule.
- `services/lakehouse-consumer/src/lakehouse_consumer/transform.py` — new `_channel_price_matrix(calculation: dict) -> list[dict]` helper, same shape as `_los_floor_matrix()` (list comprehension over whatever the DynamoDB item's `channel_price_matrix` holds, `[]` when absent on a pre-Phase-16 record).
- `transform/models/staging/stg_price_decision.sql`, `transform/models/marts/fct_price_decision.sql` — pass through `calculation.channel_price_matrix` unchanged (same nested-list mechanics as `los_floor_matrix`).

**Dashboard (`dashboard`):**
- New `render_channel_pricing()` view/tab: for a selected apartment/night, a small table exploding `channel_price_matrix` (platform, market rate, commission, suggested price, margin) read from `fct_price_decision` (cold path, same `marts.py` pattern `render_price_evolution()`/`render_margin_alerts()` already use) — **not** added as columns to the existing "Current price" table, which is already dense after Phase 15.
- `src/dashboard/marts.py` — new `channel_pricing(settings, apartment_id, target_date)` query function.

**Contracts / tests:**
- Existing `specs/contracts/fixtures/price_decision/*.json` gain `calculation.channel_price_matrix` — `[]` for fixtures with no channel data (minimally changed, same convention every prior phase followed for fixtures it doesn't specifically exercise), one new fixture (`channel_price_matrix_populated.json`) with 2–3 populated candidates.
- Unit tests: `sample_channel_pricing()`/`build_channel_market_price_events()` (market-ingestor); `decide_price_by_channel()` — empty input returns `[]`, one channel returns one candidate matching a standalone `decide_price()` call with that channel's own rate/commission, multiple channels are independent of each other and of the top-level blended calculation; `NightSnapshot`'s blended/channel update paths in `stage_price_decision.py` (a channel-only update before the blended snapshot exists doesn't fan out; a blended update after channel data already exists includes it); `stage_price_decision.py`'s emitted `channel_price_matrix` shape; contract tests (both empty and populated shapes); lakehouse-consumer schema/transform tests; dashboard's new view.

### Out of scope (explicitly deferred)

- Combining channel with LOS, a real per-channel commission contract entity, channel gross-up, and picking/recommending a channel — all listed in §1's "Not in this phase," not repeated here.
- **Any change to the top-level `calculation`** (`rule_applied`, `suggested_price_eur`, `minimum_price_eur`, etc.) — it keeps reading the blended snapshot exactly as it does today. Zero regression risk is the point.

---

## 3. Data model

### `Calculation` — one new required field

| Field | Type | Description |
|---|---|---|
| `channel_price_matrix` | `list[ChannelPriceCandidate]` | One entry per channel with an observed market rate for this night (0–3 entries). Computed from data Stage B's `NightSnapshot.channels` already holds — no new upstream source beyond `market-ingestor`'s new per-channel events. |

### `ChannelPriceCandidate` (new)

| Field | Type | Description |
|---|---|---|
| `platform` | `"airbnb" \| "booking" \| "vrbo"` | Which channel this candidate prices for. |
| `avg_nightly_rate_eur` | `number >= 0` | That channel's own raw market rate, as reported by `market-ingestor` — the input `market_reference_price_eur` below is derived from. |
| `commission_pct` | `number, 0-1` | That channel's commission rate (`CHANNEL_COMMISSION_PCT`, §2) — independent of `owner_contracts.commission_pct`, which only applies to the top-level/direct calculation. |
| `market_reference_price_eur` | `number >= 0` | **Genuinely varies per candidate**, unlike LOS's identical-across-candidates version (§1) — this channel's own rate run through the same Bonus/Malus (`property_attribute_factor`) and competitiveness-discount layers the top-level calculation uses. |
| `minimum_price_eur` | `number >= 0` | The cost floor for this channel at `stay_length=1`, using this channel's own `commission_pct` (a higher commission raises the floor, since it's netted into the same denominator `booking_window_floor()` already uses). |
| `floor_type` | same enum as top-level | Unaffected by channel — selected by `days_to_arrival`, identical across every candidate in one decision (same as LOS). |
| `floor_policy` | same enum as top-level | Derived from `floor_type`, same reasoning. |
| `rule_applied` | same enum as top-level | **Can genuinely differ per candidate** — a channel with a higher commission and/or a lower own market rate can tip into `cost_protected` while another channel in the same decision stays `market_competitive`. |
| `suggested_price_eur` | `number >= 0` | What this channel would be priced at. |
| `effective_margin` | `number` | This channel's own margin at its own suggested price. |
| `decision_components` | `list[DecisionComponent]` | Just this candidate's own rule component (same "property/commission components aren't forwarded to matrix candidates" convention `LosFloorCandidate` already established). |

Not touched: `los_floor_matrix`, `minimum_stay_recommendation`, `output.*`, and every top-level `calculation.*` field — all still computed exclusively from the blended snapshot, exactly as before this phase.

---

## 4. Logic

**Why this can't be pure post-processing the way `decide_price_los_matrix()` is:** Phase 9's matrix works as a cheap wrapper because everything except the cost floor is provably constant across LOS candidates (`docs/phase-9-los-floor-matrix-design-decisions.md` §A) — so it never needed a second market input. Here, `avg_nightly_rate_eur` itself is the thing that differs per candidate, which flows through `property_reference_price()` and `market_reference_price()` (Phase 13's Structural/Market layers) before it even reaches the floor comparison — so `market_reference_price_eur` is **not** constant across `channel_price_matrix` entries the way it is across `los_floor_matrix` entries. `decide_price_by_channel()` is therefore a genuine (if thin) per-channel composition, not a pure derivation of already-computed numbers:

```python
CHANNEL_COMMISSION_PCT: dict[str, float] = {
    "airbnb": 0.12,
    "booking": 0.15,
    "vrbo": 0.08,
}


def decide_price_by_channel(
    channel_rates_eur: dict[str, float],
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
    target_margin: float,
    competitiveness_discount: float,
    days_to_arrival: int,
    property_attribute_factor: float = 1.0,
    commission_base: CommissionBase = "total_revenue",
    ota_related_cost_eur: float = 0.0,
    cleaning_cost_eur: float = 0.0,
    channel_commission_pct: dict[str, float] = CHANNEL_COMMISSION_PCT,
) -> list[ChannelPriceCandidate]:
    net = netted_commission_amount(commission_base, ota_related_cost_eur, cleaning_cost_eur)
    candidates = []
    for platform, avg_nightly_rate_eur in channel_rates_eur.items():
        commission_pct = channel_commission_pct[platform]
        commission_netting_eur = round(commission_pct * net, 2)
        calc = decide_price(
            fixed_cost_eur=fixed_cost_eur,
            variable_cost_eur=variable_cost_eur,
            one_time_cost_eur=one_time_cost_eur,
            target_margin=target_margin,
            commission_pct=commission_pct,
            avg_nightly_rate_eur=avg_nightly_rate_eur,
            competitiveness_discount=competitiveness_discount,
            days_to_arrival=days_to_arrival,
            property_attribute_factor=property_attribute_factor,
            commission_netting_eur=commission_netting_eur,
        )
        candidates.append(
            ChannelPriceCandidate(
                platform=platform,
                avg_nightly_rate_eur=avg_nightly_rate_eur,
                commission_pct=commission_pct,
                market_reference_price_eur=calc.market_reference_price_eur,
                minimum_price_eur=calc.minimum_price_eur,
                floor_type=calc.floor_type,
                floor_policy=calc.floor_policy,
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
                decision_components=calc.decision_components,
            )
        )
    return candidates
```

Still DRY: `decide_price()`'s formula is not duplicated, just invoked once more per channel — the same discipline `decide_price_los_matrix()` follows, applied to a case where the composition is over a real second input dimension instead of a single already-computed one.

### Worked example

Reusing Phase 9's own worked-example inputs where they don't concern channel (`Cf=0`, `Cv=0`, `Cr=110.0`, `target_margin=0.05`, `property_attribute_factor=1.0`, `days_to_arrival` in the 7-14d contribution-floor tier, `commission_base="total_revenue"` so `net=0`), with three channel rates:

| `platform` | `avg_nightly_rate_eur` | `commission_pct` | `market_reference_price_eur`\* | `minimum_price_eur` | `rule_applied` |
|---|---|---|---|---|---|
| `airbnb` | 97.20 (90 × 1.08) | 0.12 | 92.34 | 125.00 | `cost_protected` |
| `booking` | 90.00 | 0.15 | 85.50 | 129.41 | `cost_protected` |
| `vrbo` | 82.80 (90 × 0.92) | 0.08 | 78.66 | 119.57 | `cost_protected` |

\*`market_reference_price_eur = avg_nightly_rate_eur × property_attribute_factor × (1 - competitiveness_discount)`, `competitiveness_discount=0.05`.

At `stay_length=1` every channel is still `cost_protected` here (same underlying issue Phase 15's own worked example diagnoses) — but the three floors and reference prices are all genuinely different, which is exactly the point: a PM comparing channels sees Airbnb's lower commission partially offsets Booking's higher own market rate, while Vrbo's lower rate and mid commission land it in between. None of this is visible today, where only one blended figure exists.

---

## 5. Acceptance criteria

- **AC-01 — Empty matrix when no channel data exists.** A night with only a blended snapshot (pre-Phase-16 behavior, or a night whose channel events haven't arrived yet) gets `channel_price_matrix: []`; the top-level `calculation` is identical to what it would be without this phase.
- **AC-02 — One candidate per known channel, independently correct.** For a night with 1–3 channel snapshots, `decide_price_by_channel()` returns exactly that many candidates, each matching a standalone `decide_price()` call with that channel's own `avg_nightly_rate_eur`/`commission_pct` and the decision's shared cost/margin/property inputs.
- **AC-03 — `rule_applied` can differ per channel.** A constructed scenario where one channel's floor clears `cost_protected` and another's doesn't produces two candidates with different `rule_applied` values, proving §4's claim that (unlike LOS) this isn't a monotonic single-axis comparison.
- **AC-04 — Top-level calculation is byte-for-byte unaffected.** Before/after comparison on an existing fixture: adding channel snapshots to a night changes nothing about `calculation.minimum_price_eur`/`rule_applied`/`suggested_price_eur`/`los_floor_matrix`/`minimum_stay_recommendation`.
- **AC-05 — `NightSnapshot` state transitions are correct.** A channel-only event for a brand-new night (no blended snapshot yet) is stored but doesn't fan out; once the blended event for that night arrives, the resulting `PriceDecision` includes the already-stored channel candidate(s); a blended update for an already-known night preserves existing `channels` entries untouched.
- **AC-06 — `market-ingestor` emits 4 events per segment per tick**, unchanged blended event plus 3 new channel events, each schema-valid against `market_price.v1.json` with a real `platform` value.
- **AC-07 — `price_decision.v1` validates both the empty and populated `channel_price_matrix` shapes.**
- **AC-08 — lakehouse-consumer carries the new field without error**; `dbt run`/`dbt test` pass with `channel_price_matrix` present (possibly empty) on `fct_price_decision`.
- **AC-09 — Dashboard's new channel view renders both shapes** (no channel data yet vs. 1–3 populated candidates) without error.
- **AC-10 — Live verification.** Bring up the stack; confirm real apartments show a non-empty `channel_price_matrix` in DynamoDB once `market-ingestor`'s channel events have flowed for a few ticks, that the top-level `calculation` for those same decisions is unchanged from before this phase's deployment, that the field reaches `fct_price_decision` via `dbt run`, and that the dashboard's new view renders it against live data.

---

## 6. Test strategy

- **Pure logic, no infrastructure:** `sample_channel_pricing()` (market-ingestor), `decide_price_by_channel()` (AC-01–AC-03) — same tier `decide_price()`/`decide_price_los_matrix()` are tested at.
- **Integration, no infrastructure:** `stage_price_decision.py`'s `NightSnapshot` state-transition tests (AC-05) and its `channel_price_matrix` wiring (AC-02, AC-04) — extends the existing fakes-based `test_stage_price_decision.py` suite.
- **Static/schema review:** `price_decision.v1.json` contract test (AC-07), lakehouse-consumer schema/transform unit tests (AC-08).
- **Dashboard unit test:** new `test_marts.py`/`test_app.py` coverage for the channel view (AC-09).
- **Live verification against LocalStack (required, AC-10):** heavier than Phase 15's (a real `market-ingestor` code change affecting its emission volume, unlike Phase 15's zero-new-data-source scope) — needs a fresh `market-ingestor` image and a few ticks observed before channel data appears.

---

## 7. Known limitations

- **Synthetic channel spread, not real per-platform data.** `_CHANNEL_PRICE_MULTIPLIERS` and `CHANNEL_COMMISSION_PCT` are both plausible constants, not sourced from a specific market report or contract — same caveat every other synthetic constant in this project already carries (`segments.py`, `_TARGET_COEFFICIENT_OF_VARIATION`).
- **One commission rate per channel, shared by every apartment.** A real PM's actual negotiated commission with Booking.com can vary by property/volume — this phase intentionally doesn't model that (§1); it establishes the mechanism a future `owner_channel_commissions` table would plug into.
- **Not combined with LOS.** A PM cannot yet see "what would a 3-night stay on Airbnb cost" — only 1-night-per-channel and 1-channel(blended)-per-LOS exist independently after this phase.
- **`"direct"` isn't a modeled channel.** The existing top-level/blended calculation already represents the direct-booking case (no OTA commission netting beyond `owner_contracts`'s own rate) — adding a literal `"direct"` entry to `channel_price_matrix` would just duplicate the top-level `calculation` under a different name, so it's deliberately left out.

---

## 8. Follow-ups

- **`docs/post-poc-roadmap.md`** — backlog #2 moves from "Now/Next" to "Done" once this phase is implemented, unit-tested, and live-verified; backlog #8 (channel gross-up) becomes unblocked.
- **Phase 17 candidate: combine channel × LOS.** Once both axes are independently proven correct, a `decide_price_by_channel_and_stay()` (or a 2D matrix) is a natural, low-risk extension — deliberately not attempted here (§1).
- **A real `owner_channel_commissions` table**, replacing the fixed `CHANNEL_COMMISSION_PCT` constant with actual per-apartment, per-channel contract data — the same evolution `owner_contracts` (Phase 11) was for the old flat `commission_pct` column.
- **Picking/recommending a channel**, once enough of backlog #2/#8 exists to make that a well-defined question rather than a raw data comparison — mirrors how Phase 15 later acted on Phase 9's passive matrix.

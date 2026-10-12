# Phase 13 — Layered Revenue Management engine: design decisions

Pre-spec for ADR-0011 backlog #7. Captures the decisions made before writing
`specs/phases/13-layered-rm-engine/spec.md`.

## A. The source document doesn't name what each layer computes

The external spec's Context paragraph in ADR-0011 names the seven layers
(structural/market/performance/booking-window/inventory/commercial/guardrails)
as a single parenthetical list — the document itself isn't checked into this
repo, and no file anywhere defines what each layer individually computes or
how they combine (multiplicative factors? sequential clamps? something
else). This phase is designed from the repo's own accumulated hints plus
standard revenue-management judgment, not from the original document.

Hints found and honored:

| Layer | Anchor | Status |
|---|---|---|
| Structural | Property Bonus/Malus (Phase 8, `property_attributes.py`) | Already built, substantial |
| Booking-Window | ADR-0009 D4's antelación tier table | Already built, substantial |
| Guardrails | `docs/AUDIT_DIARY.md`'s own words: "a financial guardrail... the system publishes max(market, floor)" | Already built, substantial (see §C for which of two anchors was chosen) |
| Commercial | `specs/phases/11-owner-contract/spec.md`'s own follow-up note: "a future Commercial or Guardrails layer could reuse this phase's netted-amount shape" | Already built, substantial |
| Market | The existing `market_reference_price_eur = property_reference_price_eur * (1 - competitiveness_discount)` line | Already built, trivial today (see §D) |
| Performance | None | No anchor — stub |
| Inventory | None | No anchor — stub |

## B. Scope: one phase, zero event-schema change

Confirmed with the user: a single phase (not split), and — because every
one of the seven computations above already exists somewhere in this repo's
pricing logic (this phase reorganizes, it doesn't invent new pricing
behavior for 5 of the 7 layers) — there is no new field anywhere in
`price_decision.v1`, `shared_schemas`, the Iceberg schema, or any dbt model.
The entire diff is: a new `libs/pricing-formulas` package, and import-path
updates in `streaming/flink-jobs`. This also means the live-verification
acceptance criterion (§7 spec) is framed as a **regression proof** (a real
decision's output is byte-for-byte identical before/after) rather than a
new-field proof, unlike every prior phase in this backlog.

## C. Guardrails: two plausible anchors, one chosen

Two things in this repo could plausibly be called "the Guardrails layer":

1. `property_attributes.py`'s `[0.5, 2.0]` clamp on the combined attribute
   factor — `docs/phase-8-property-bonus-malus-design-decisions.md` already
   calls it "a guardrail against an absurd combined factor, matching the
   source spec's own emphasis on caps (§14.2, Guardrails layer)".
2. `decide_price()`'s final `rule_applied`/`suggested_price_eur` selection —
   `docs/AUDIT_DIARY.md:119` independently describes this exact logic as "a
   financial guardrail... the system publishes max(market, floor)".

**Decision: Guardrails-as-a-formal-layer maps to (2), the final floor-vs-market
arbitration.** Reasoning: (1) is a bounds-check internal to one layer's own
arithmetic (Structural's `MIN_FACTOR`/`MAX_FACTOR` are Structural-domain
constants) — it doesn't arbitrate between two different layers' outputs. (2)
is a genuine cross-layer arbitration between what Booking-Window's floor
demands and what Market proposes, which is the textbook definition of a
guardrail in an RM pipeline. The clamp stays inside `layers/structural.py`
exactly where it is today — not relocated, not duplicated.

## D. Market is genuinely thin today — that's intentional, not a gap

`market_layer()`'s entire body is one multiplication, with no
`DecisionComponent` (a discount setting isn't a decision to explain, same
principle as commission netting's "don't emit a component that explains
nothing" from Phase 11). Its value in this phase isn't new logic — it's
being the pre-cut seam where backlog #11's real market data
(`occupancy_rate`/`sample_size`, confirmed dead pass-through data today,
never read by `decide_price()`) will eventually land, without that future
phase having to carve a new module out of `engine.py`.

## E. Performance and Inventory: real stubs, no speculative parameters

Both are wired into the actual pipeline (invoked, their factor genuinely
multiplied into `property_reference_price_eur`) — not declared-but-uncalled
dead code. Both are permanently zero-argument functions returning `1.0`
today. Two decisions worth stating explicitly:

- **No `DecisionComponent` is ever emitted for either.** A component that's
  coincidentally `0.0` this period (like `commission_base_netting` when
  `net <= 0`) is "evaluated, no effect" — informative. A stub that can
  *never* be anything but neutral, for this entire phase's lifetime, isn't
  evaluating anything — emitting a component for it would be the purest
  form of the "don't emit a component that explains nothing" anti-pattern
  Phase 11 already named.
- **No speculative parameters added to `decide_price()` to feed them.**
  `occupancy_rate`/`sample_size` already flow into `MarketSnapshot` but stay
  unwired here — when backlog #11 gives Performance a real signal to act
  on, that phase adds the parameter together with the logic, the same
  pattern Phase 11 itself followed for `commission_netting_eur` (never
  added ahead of having something real to compute with it).

## F. Rounding-order coupling — the real risk in this extraction

`decide_price()` today compares `minimum_price_eur`/`market_reference_price_eur`/
`property_reference_price_eur` **unrounded** to decide `rule_applied`, then
passes **rounded** copies of those same three values into
`rule_decision_component()` purely for its label/impact text. A refactor
that rounds once and reuses that value for both the comparison and the
label would pass almost every existing test yet could silently shift
`rule_applied` at boundary cases by fractions of a cent — none of the
existing ~32 tests specifically probe this seam. `guardrails.py`'s
`apply_guardrails()` preserves the exact original order; two new boundary
tests close the gap (spec AC-04).

## G. No backward-compatibility shims

`flink_jobs/pricing.py`, `property_attributes.py`, `decision_components.py`
are deleted outright, not kept as re-export shims. Confirmed by repo-wide
grep: zero consumers outside `streaming/flink-jobs`, entirely under this
project's own control, no precedent anywhere in this repo for leaving a
compatibility shim after an internal move. Every import site is updated
directly.

## H. Honesty about what this phase actually delivers

Structural, Booking-Window, Commercial, and Guardrails are structurally
*and* functionally real today (this phase reorganizes already-working
logic into named, independently-testable modules). Market is structurally
real but functionally trivial. Performance and Inventory are structurally
real (invoked, composed, unit-tested) but functionally inert until backlog
#11. This phase's value for the last three is the seam and the pipeline
shape, not new pricing behavior — stated plainly in the spec's executive
summary rather than implied otherwise.

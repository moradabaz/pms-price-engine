# ADR-0014 — Formalize StayCandidate/PropertyPricingProfile/PricingStrategy, without changing `libs/pricing-formulas`'s signatures

**Status:** Proposed
**Related:** ADR-0011 (target architecture), ADR-0013 (BER/MPR rewire, the last phase to touch these
call sites), `specs/phases/21-explicit-domain-entities/spec.md`,
`/Users/morad/Downloads/dynamic_price_engine.md` §6-7, §9, §15, §21

## Context

The external spec names `StayCandidate` (§9), `PropertyPricingProfile` (§6-7), and `PricingStrategy`
(§15) as first-class entities, and lists all three in its own entities table (§21). This project
implements the *logic* behind all three today, but not the *shape*: a stay candidate is an implicit
`(apartment_id, target_date, stay_length, platform)` tuple threaded through loop variables in
`stage_price_decision.py`; property attributes, segment identity, and strategy parameters are all
flattened into one `SegmentAssignment` dataclass that conflates two conceptually different things —
"what the property IS" (attributes/positioning) and "what business risk the operator wants to take"
(target margin, discount tolerance).

Two questions had to be resolved before writing code: (1) does formalizing these entities require
`libs/pricing-formulas`'s pure functions to change shape, and (2) does `SegmentAssignment` need to
become one bigger entity or split into two.

## Decision

1. **`StayCandidate` lives in `streaming/flink-jobs`, not `libs/pricing-formulas`.**
   `libs/pricing-formulas` has been deliberately kept as a package of pure, flat-argument functions
   since Phase 13, reaffirmed as recently as ADR-0013 §6 ("no formula duplicated... a thin
   composition over `decide_price()`"). `decide_price()` never needs `channel`/`guests`/
   `candidate_revenue` — only `stay_length`. Forcing an entity parameter onto it would couple a pure
   math package to a domain object it doesn't use most of. `StayCandidate` is therefore the object
   Stage B constructs once per (apartment, night, stay_length, channel) combination it evaluates,
   living in `flink_jobs/models.py` next to `CostAggregate`/`NightSnapshot` — `decide_price()`'s
   signature is unchanged by this ADR.
2. **`SegmentAssignment` splits into `PropertyPricingProfile` and `PricingStrategy`.** The client's
   own document treats "what the property is" and "what strategy the operator wants" as different
   panels (§26: Panel A "Property Pricing" vs. Panel C "Pricing Strategy") — this project's single
   `SegmentAssignment` conflated them for convenience during earlier phases (they were always
   resolved from the same broadcast row, so one dataclass was the path of least resistance at the
   time). This ADR undoes that conflation at the code level only — no database schema change; both
   entities are still built from the same `apartment_market_segments` CDC row via two `to_*` methods
   on `ApartmentSegmentRow`, replacing the current single `to_assignment()`.
3. **`PricingRule` is explicitly not created as an entity.** Making Revenue Management rules
   data-driven (trigger/scope/adjustment/priority/compatibility/cap/validity, external spec §14.2)
   would be a materially larger effort — Phase 13's RM layers (`structural.py` through
   `guardrails.py`, plus ADR-0015's new `booking_window.py`) stay plain, composed Python functions.
   This is a scope boundary, not an oversight — it keeps this phase a pure, low-risk refactor rather
   than a rule-engine rewrite.
4. **No behavioral change is acceptable output from this phase.** Both entities are constructed from
   data this project already resolves identically today; the acceptance bar (spec §5, AC-02) is
   numerically identical decisions before and after the refactor.

## Consequences

- `stage_cost_enrichment.py`'s broadcast state shape changes (two objects or a tuple instead of one
  `SegmentAssignment`) — every read site inside that stage and its tests updates accordingly, a
  mechanical rename/split, not new logic.
- Future strategy-related work (ADR-0018's versioning) has a real entity to attach a `version` field
  to, without having also dragged property attributes along for the ride.
- `apartment_market_segments` remains a single table carrying both property-attribute and
  strategy-parameter columns until ADR-0018 finally splits `pricing_strategies` out at the database
  level — this ADR's split is code-only, a deliberate two-step sequencing (name the concepts first,
  move the data later) rather than doing both in one larger, riskier change.

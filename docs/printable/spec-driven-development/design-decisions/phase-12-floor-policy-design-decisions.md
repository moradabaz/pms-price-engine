# Phase 12 — Explicit Hard/Soft floor policy: design decisions

Pre-spec for ADR-0011 backlog #10. Captures the decisions made before writing
`specs/phases/12-floor-policy/spec.md`.

## A. What "Hard/Soft floor policy" maps to in this repo

The external spec's Hard floor (an absolute minimum the engine will never
cross) and Soft floor (a target-margin floor the engine tries to respect but
can relax as booking urgency changes) already exist here as two of the three
`floor_type` tiers ADR-0009 defined by days-to-arrival:

- `contribution` — no margin term at all, only variable + amortized one-time
  cost survive commission. This is the Hard floor: the absolute point below
  which a booking becomes a loss.
- `structural_full_margin` / `structural_reduced_margin` — both carry a
  margin term (full or reduced), the thing that gets relaxed as arrival
  approaches. Both are Soft floor: negotiable by design, `contribution` is
  what the reduction is bounded by.

So `floor_policy` is a pure 2-valued classification *of* `floor_type`, not a
new pricing input. No formula changes.

## B. Field shape and placement

`floor_policy: Literal["hard", "soft"]`, derived by one pure function
(`floor_policy_for(floor_type) -> FloorPolicy`) and placed next to
`floor_type` at both nesting levels that already carry a `floor_type`:
`Calculation.floor_policy` and `LosFloorCandidate.floor_policy` — mirrors
exactly how `floor_type` itself is duplicated at both levels (a LOS
candidate's own tier can differ from the top-level decision's, since each
candidate's `days_to_arrival` is shared but nothing else about which
tier fires is tied to stay length... actually `days_to_arrival` is the only
input, so in practice every candidate in one `los_floor_matrix` shares the
same `floor_type`/`floor_policy` today — still placed at both levels for
symmetry with `floor_type`, and to stay correct if that ever changes).

## C. No new decision component

`floor_policy` doesn't explain anything `decision_components` doesn't
already cover more precisely (the `rule_*` component already states the
EUR gap; `floor_type` already states which tier). It's a relabeling for
external-vocabulary alignment, not a new "why" — so no `ReasonCode` addition.

## D. Backward compatibility for records predating this phase

Unlike Phase 11's `commission_base` (a genuinely new input with a fixed
historical default), `floor_policy` is 100% derived from `floor_type`, which
every prior-phase record already has. So the lakehouse-consumer fallback for
a missing `floor_policy` key isn't a static default — it re-derives it from
that record's own `floor_type` via the same `hard`-iff-`contribution` rule,
duplicated as a small local helper in `transform.py` (lakehouse-consumer
doesn't depend on `flink_jobs`).

## E. Live verification scope

This phase adds one more plain string field next to two others
(`floor_type`, `commission_base`) whose Iceberg/DynamoDB/dbt round-trip is
already proven (Phase 8, Phase 11) — no new shape, no new pipeline stage, no
new topic. Live verification is still required per this project's
convention, but it's a confirmation pass (values correct, present at both
nesting levels, correct for both a `hard` and a `soft` example), not new
ground to prove.

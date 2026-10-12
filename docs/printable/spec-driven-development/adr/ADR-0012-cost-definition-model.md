# ADR-0012 — `payment_line.v1` breaking change: `CostDefinition`/`CostOccurrence`/`CostAllocationRule`

**Date:** 2026-09-12
**Status:** Accepted

## Context

`payment_line.v1`'s `concept`/`cost_type` pair is the only cost classification this project has
ever had, and it has never changed since ADR-0003 defined the contract. ADR-0011 backlog item #13
(the external target spec, `/Users/morad/Downloads/dynamic_price_engine.md` section 8) replaces
this with a richer model: every cost is described along 6 independent dimensions (Scope, Behavior,
Trigger, Calculation Base, Recurrence, Allocation Rule) plus a validity window, via three entities:
`CostDefinition` (the rule), `CostOccurrence` (an actual amount for a period — what `PaymentLine`
already is), and `CostAllocationRule` (how a temporal cost becomes an imputable per-unit cost).

Unlike every prior change to `payment_line.v1` (Phase 17's `cost_breakdown`, for example, added a
field without touching any existing one), this one **removes** `concept`, `cost_type`, `is_shared`,
and `allocation_ratio` from `payment_lines`/`payment_line.v1` entirely, replacing them with a
single `cost_definition_id` foreign key. A consumer written against the old shape cannot parse the
new one — this is this project's first genuinely breaking event-contract change, not an additive
one, and is called out here explicitly rather than silently folded into a phase spec.

## Decision

1. **`payment_line.v1`'s `schema_version` const moves from `"1.0"` to `"2.0"`.** This is the
   trigger for a version bump in this project's own convention: every prior phase (8 through 17)
   added fields without removing any, so `schema_version` correctly stayed `"1.0"` throughout. This
   phase removes required fields, which is not backward compatible, so the version number changes
   to say so honestly.
2. **`concept`/`cost_type`/`is_shared`/`allocation_ratio` are physically removed** from
   `payment_lines` and `payment_line.v1` — not kept alongside `cost_definition_id` as a redundant
   copy. `CostDefinition` becomes the single source of truth for what a cost is; Flink resolves
   `concept`/`behavior`/etc. via a new broadcast join, never by reading them off `PaymentLine`
   directly. Same "one source of truth" precedent Phase 11 set when it moved `commission_pct` out
   of `apartment_market_segments` into `owner_contracts`.
3. **Existing `payment_lines` rows are backfilled, not orphaned.** A one-time migration generates
   one `CostDefinition` per distinct `(concept, cost_type)` pair already observed in seeded data,
   and sets `cost_definition_id` on every existing row to match. No historical row loses its
   classification; it becomes queryable under the new model immediately, rather than requiring
   every downstream reader to handle a permanently-possible `NULL cost_definition_id`.
4. **Company-scoped cost fan-out is broadcast-state-read-from-the-keyed-side, not a true push.**
   PyFlink's own installed source (`pyflink/datastream/functions.py`) states
   `applyToKeyedState` is not supported in the Python API — there is no way to push an update from
   a broadcast element to every keyed partition. A `scope=company` `CostDefinition` is applied the
   same way Phase 11's owner-contract broadcast already works: every apartment reads the shared
   broadcast state when its own next event (a payment line, a booking) is processed. A company cost
   change is visible to a quiet apartment only once that apartment has its own next event, not
   instantly — the same trade-off the owner-contract broadcast already accepts for commission
   changes, not a new limitation this decision introduces.

## Rationale

- **Honesty over convenience.** Keeping `schema_version` at `"1.0"` while silently removing
  required fields would make every future reader of this project's own convention ("schema_version
  changes mean something") wrong about what changed and when.
- **A single source of truth, even mid-migration, beats a temporarily-easier dual copy.** Keeping
  `concept`/`cost_type` alongside `cost_definition_id` "for now" is exactly the two-sources-of-truth
  shape this project's own SOLID/DRY practice already rejected once (Phase 11, `commission_pct`).
- **A confirmed technical constraint, not a design preference, drove the fan-out design.** The
  original plan for this backlog item assumed a genuine 1-to-N broadcast push existed in PyFlink;
  it does not, confirmed by reading the installed library's own docstrings before writing any code
  against the assumption.

## Consequences

- `specs/events/payment_line.v1.json`'s required fields change; any fixture or code written against
  the pre-ADR-0012 shape must be updated (specs/contracts/fixtures/payment_line/*.json).
- `streaming/flink-jobs/src/flink_jobs/cost_aggregation.py`'s `OTA_RELATED_CONCEPTS`/
  `CLEANING_CONCEPTS` filters, which read `line.concept` directly, must resolve concept via the new
  `CostDefinition` broadcast join instead.
- A `scope=company` cost is only as fresh as each apartment's own event cadence — an apartment with
  no activity for a long window will not see a company cost change until it has one. Acceptable for
  this PoC's scale; would need revisiting for a production deployment with genuinely idle
  properties.
- `libs/pricing-formulas`'s consumption of the new richer cost representation is deliberately out
  of scope here (Phase 20) — this ADR covers the data model and its ingestion into Flink's
  `CostAggregate` only, not any change to `decide_price()` itself.

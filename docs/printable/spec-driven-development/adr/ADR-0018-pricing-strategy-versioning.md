# ADR-0018 — Version `PricingStrategy` with a simple integer field, future-only, no historical replay

**Status:** Proposed
**Related:** ADR-0014 (`PricingStrategy` as an entity — this ADR's prerequisite), ADR-0011/ADR-0013
(the "future-only" precedent this ADR follows), `specs/phases/25-pricing-strategy-versioning/spec.md`,
`/Users/morad/Downloads/dynamic_price_engine.md` §28, `docs/post-poc-roadmap.md` backlog #15

## Context

The external spec (§28) asks for versioned `PricingStrategy`/`PricingRule` and reproducible
historical decisions. `docs/post-poc-roadmap.md` backlog #15 has carried this as tier "Later" since
2026-08-30 specifically because versioning `PricingRule` presupposes a configurable rule engine
(rules as data) that Phase 13 deliberately never became (ADR-0014 §3 keeps that decision). But
ADR-0014 did make `PricingStrategy` a real, named entity — independent of whether RM rules are ever
data-driven, a strategy's own parameters (`target_margin`, `competitiveness_discount`) can be
versioned on their own.

The user was offered two depths for this: a simple `version` integer with no replay, or full
historical replay (storing every version and recomputing past decisions under a chosen one). The
user chose the simple option, given the PoC has no current use case that needs replaying a past
decision under a different, hypothetical strategy.

## Decision

1. **`pricing_strategies` becomes a new, insert-only table**, extracted out of
   `apartment_market_segments` (which currently holds `target_margin`/`competitiveness_discount`
   inline). A real substitution, not an additive duplicate column — the same precedent ADR-0012
   established for `payment_lines`/`concept`/`cost_type` and ADR-0013 established for
   `owner_contracts`/`commission_pct`/`commission_base`: the old columns are dropped after a
   guarded, one-time backfill, not kept alongside the new table as a second source of truth.
2. **Editing a strategy inserts a new `version` row; nothing is ever `UPDATE`d.** This is what makes
   "future-only" concrete at the data layer, not just a documented intention: Debezium can only ever
   emit an `INSERT` CDC event for a strategy change, mirroring the append-only shape
   `manual_overrides` already uses for its own audit trail (Phase 14).
3. **No historical replay.** A `PriceDecision` already emitted keeps referencing whatever
   `pricing_strategy_version` was live at the time it was computed — nothing recomputes it when a
   later version is introduced. This is the same "future-only, same as every other broadcast-config
   change in this repo" rule ADR-0011/ADR-0013 both already state explicitly for cost/contract
   changes; this ADR applies it to strategy changes too, for consistency, not as a new precedent.
4. **Out-of-order CDC delivery is resolved by version comparison, not arrival order.** The Flink
   broadcast stage keeps the highest `version` seen so far per apartment
   (`if incoming.version > current.version: replace`) rather than assuming Kafka delivers strategy
   changes in strict order across partitions — a correctness guard, not a performance optimization.
5. **`Calculation.pricing_strategy_version` becomes a required field** — a breaking `price_decision.v1`
   bump, the mechanism that actually satisfies "reproducibility" (§28): given any decision, its exact
   strategy version is always recoverable, even without replay.

## Consequences

- Every decision now carries one more piece of required provenance, at the cost of one more required
  field to keep in sync across `libs/shared-schemas`, `specs/events/price_decision.v1.json`, Iceberg,
  dbt, and the dashboard — the same "ripple through every layer" cost every prior breaking
  `price_decision.v1` bump (ADR-0012, ADR-0013, and potentially ADR-0016 if it lands first) has
  already paid; this phase's numbering (`2.0`/`3.0`/`4.0`) depends on merge order with ADR-0016, not
  fixed in advance (documented in the phase spec, §3).
- `PricingStrategy.effective_from` is set at insertion time, not schedulable ahead of time the way
  `CostDefinition.validity_start`/`.validity_end` already are (Phase 19) — a deliberate, documented
  scope cut, not parity with every other config entity in this project.
- If `PricingRule` versioning is ever picked up later, it inherits this ADR's "insert-only, no
  replay" pattern by default, rather than needing its own separate design discussion — one
  versioning philosophy for this project's config entities, not several.

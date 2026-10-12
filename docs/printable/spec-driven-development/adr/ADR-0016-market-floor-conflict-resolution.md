# ADR-0016 — Classify market-vs-floor conflicts as a `viability_status` field, tracked via in-memory streaks, not batch queries

**Status:** Proposed
**Related:** ADR-0006 (DynamoDB single-writer / Iceberg CDC — the architecture this ADR avoids
recoupling to a synchronous read path), Phase 9/15/16 (LOS matrix, min-stay recommendation, channel
matrix — all reused, none changed), `specs/phases/23-market-floor-conflict-resolution/spec.md`,
`/Users/morad/Downloads/dynamic_price_engine.md` §13, test scenario T08

## Context

The external spec's §13 describes a situation/action table (RM price vs. floor, by LOS, by channel,
by persistence over time) that this project currently only implements narratively, via the
`rule_applied` value (`market_competitive`/`minimum_floor`/`minimum_profitable_price`) and whatever a
human reading the dashboard infers from it. Nothing classifies *what to do about it* — evaluate
min-stay, favor a channel, raise a structural alert, or simply note demand upside — and nothing
tracks whether a floor breach is a one-off or a 30-day structural problem (test scenario T08).

Two decisions had to be made: what shape the classification takes, and — the harder one — how to
track "persistent for 30 days" inside a streaming system without adding a synchronous batch query to
the hot decision path.

## Decision

1. **A new, pure, priority-ordered classification function**, `classify_viability()`, producing one
   of seven `ViabilityStatus` values from data this project already computes per decision
   (`rule_applied`, `minimum_stay_recommendation`, `channel_price_matrix`, `manual_override`
   presence, and the new `floor_breach_days` counter). It classifies; it never acts — every lever the
   external spec's table names (raise min-stay, favor a channel) stays a human/dashboard decision,
   consistent with this project's existing Manual Override precedent that a human always authorizes
   an exception.
2. **Persistence is tracked as an in-memory streak on `NightSnapshot`, not a batch Iceberg query.**
   `stage_price_decision.py`'s Stage B already holds one `NightSnapshot` per `(apartment_id,
   target_date)` in its own `MapState` for as long as that night stays "known." Adding one field,
   `floor_breach_since: date | None`, set the first time a night's `rule_applied` moves off
   `market_competitive` and cleared the first time it moves back, makes `floor_breach_days` a pure
   arithmetic derivation (`today - floor_breach_since`) computed inline, with zero new I/O. The
   alternative — querying Iceberg at decision time for "was this floor exceeded on each of the last
   30 days" — would mean a synchronous batch read inside Stage B's hot streaming path, directly
   against the reasoning ADR-0006 already established for why DynamoDB, not Iceberg, is the
   single low-latency writer this pipeline's hot path depends on.
3. **`PERSISTENT_BREACH_THRESHOLD_DAYS = 30`** is a named, configurable constant matching the
   client's own test scenario T08 literally, not a coincidence of implementation convenience.
4. **`viability_status` becomes a required field on `Calculation`** — a breaking `price_decision.v1`
   bump, same class of change ADR-0012/ADR-0013 already established for their own required-field
   additions under this project's strict (`extra="forbid"`) schema convention.

## Consequences

- A Flink job restart without a checkpoint/savepoint loses in-progress breach streaks — an accepted,
  pre-existing limitation class (Stage A's cost `MapState` already carries the same risk); this ADR
  does not change checkpointing configuration.
- The dashboard gains a genuinely new surface (a Health/Alerts view, external spec panel J) rather
  than just another column on an existing table — `persistent_floor_breach` is the first alerting
  concept this project has needed to expose as its own view.
- `"channel_lever_available"` can only ever suggest a channel already present in
  `channel_price_matrix` — consistent with `decide_price_by_channel()`'s existing "never invents a
  channel with no observed market rate" rule (Phase 16); this ADR does not relax that.

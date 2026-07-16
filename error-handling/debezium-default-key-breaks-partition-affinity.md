# Incident: Messages weren't partitioned by apartment_id at all — Debezium's default key is the primary key

**Phase:** 2 (CDC pipeline)
**Component:** `infra/debezium/postgres-connector.json`
**Date:** 2026-07-16
**Discovered while:** verifying AC-05 (partitioning)

---

## What happened

ADR-0001 states plainly: *"Partition key for both streams is `apartment_id` to guarantee per-apartment ordering and avoid state race conditions in Flink."* Before trusting that this held, we captured live messages with their Kafka record key and partition number:

```
Partition:3 | {"event_id":"c2b89c6f-9bbd-416b-81f8-a9d40a06d5dd"} | {...,"apartment_id":"BCN-001",...}
```

The Kafka **key** was `{"event_id": "..."}` — not `apartment_id`. Grouping every captured message by its `apartment_id` field and checking how many distinct partitions each one touched: **10 of 12 apartment_ids were scattered across all 6 partitions.** The partitioning requirement wasn't slightly off — it wasn't in effect at all. Every row was, in practice, independently hash-partitioned by its own primary key.

## Root cause

Debezium's PostgreSQL connector has a well-defined default for the Kafka record key: **the table's primary key column(s)** — here, `event_id` (`payment_lines`' `UUID PRIMARY KEY`). This is a deliberate, sensible default in general (it's what makes log compaction and keyed upserts work correctly out of the box), but it has nothing to do with which *business* column you want Kafka's partitioner to group by. Kafka's default partitioner hashes whatever the record key is — `hash(key) % partition_count` — so with `event_id` as the key, every row (a fresh UUID) landed on an effectively random partition, completely independent of which apartment it belonged to.

Nothing in `infra/debezium/postgres-connector.json` ever told Debezium to use a different key. The config had a full transform chain (`unwrap`, `route`), decimal handling, and a custom date converter — but no `message.key.columns`, the one setting that actually controls this.

This is a case where **the architecture decision (ADR-0001) and the connector config silently diverged** — same underlying shape of problem as the topic-naming incident, just for a different setting. The spec's data contract discussion (§3, ADR-0003) covers `event_id`'s role in the *payload* in detail, but nothing in Phase 2's spec or connector config ever explicitly addressed the Kafka *key*.

## How we found it

We didn't wait for a subtle symptom — AC-05 in the Phase 2 spec exists specifically to check this, so we checked it directly: consumed the topic with `--property print.key=true --property print.partition=true`, grouped every message by its `apartment_id` value (from the *payload*, not the key), and counted distinct partitions per apartment. Anything with more than one partition is a violation, by construction. 10 out of 12 lit up immediately — no ambiguity, no waiting for a downstream symptom to appear.

## How we fixed it

Added Debezium's own setting for exactly this: `message.key.columns`, which lets you override the default (primary key) with an arbitrary column list, scoped per table:

```json
"message.key.columns": "public.payment_lines:apartment_id"
```

This only changes the Kafka record **key** (and therefore partition assignment) — the JSON **payload** still contains `event_id` on every message exactly as before, so ADR-0003's "consumers upsert keyed state by `event_id`" requirement for Phase 4 (Flink) is completely unaffected. The two concerns — "what partitions messages for ordering" and "what field consumers use to upsert state" — are independent, and Debezium lets you set them independently too.

As with the two earlier incidents, changing this setting doesn't retroactively fix already-committed messages (see the postscript in [`debezium-date-decimal-wire-encoding-mismatch.md`](./debezium-date-decimal-wire-encoding-mismatch.md)) — every old message was still keyed by `event_id` and would stay scattered across partitions forever. We repeated the same full-reset procedure: stop the connector, apply the new config, reset its Connect-managed offsets, delete and recreate the topic, then resume — producing a completely fresh, correctly-keyed snapshot. Re-verified: all 12 apartment_ids now map to exactly one partition each, and AC-01/AC-03 still hold.

## Situations where you can hit this

- **Any CDC connector capturing a table whose primary key isn't the column you actually want Kafka ordering/partitioning guarantees on.** This is the norm, not the exception — a surrogate UUID primary key (for stable row identity, as `event_id` is here) is very often *not* the column downstream consumers care about grouping/ordering by.
- **Multi-table connectors, where each table might need a different key override.** `message.key.columns` takes a semicolon-separated list of `<table>:<column(s)>` pairs — easy to add the setting for the table you're actively testing and forget it for others captured by the same connector.
- **Any architecture decision (an ADR, a design doc) that specifies a partitioning/ordering guarantee "by X"** without that requirement being traced all the way down into the actual connector config that produces the messages. A decision written down is not a decision enforced — only a config setting (or a test that checks for it, like AC-05 here) enforces it.
- **Anywhere a "reasonable-sounding default" quietly satisfies a *different* requirement than the one you have.** Keying by primary key is completely correct and desirable for compaction/upsert semantics — it's just orthogonal to per-business-key ordering, and it's easy to assume a sensible-sounding default is *the* sensible default for every purpose at once.

## What to learn from this

- **An architecture decision only holds once it's traceable to a specific, checkable setting.** ADR-0001 said "partition key is `apartment_id`" for weeks before this was ever verified against a running connector — the gap between "documented" and "configured" only became visible because AC-05 exists specifically to check it, and we ran that check rather than assuming a well-written ADR implies a correctly-configured system.
- **The Kafka record key and the event payload are independent concerns, and CDC connectors let you tune them independently.** Don't reach for reshaping the payload (or worse, a downstream re-partitioning step) when the fix is a one-line key-column override at the source.
- **When verifying a partitioning/ordering guarantee, check it structurally (group and count), not anecdotally.** A quick manual spot-check of two or three messages could easily have looked fine by chance (12 apartments over 6 partitions means roughly a 1-in-6 chance any two random messages for the same apartment collide anyway) — the violation only became undeniable once every apartment's *full* partition set was computed and checked for `len() > 1`.

# Incident: adding a table to an already-running connector's `table.include.list` never snapshots its existing rows

**Phase:** 4 (Flink processing) — discovered while wiring `apartment_market_segments` into Debezium
**Component:** `infra/debezium/postgres-connector.json`
**Date:** 2026-07-29
**Discovered while:** deploying Phase 4 live, verifying `apartment-market-segments.v1` actually received the 10 seeded rows

---

## What happened

`pms-payment-lines-connector` was already `RUNNING` (registered in an earlier session, streaming `payment_lines` for hours). Added `public.apartment_market_segments` to `table.include.list`, added a second `RegexRouter` transform to route it to `apartment-market-segments.v1`, and `PUT` the updated config to the running connector. It came back `RUNNING` immediately, no errors. `apartment-market-segments.v1`'s offset stayed at **0** — none of the table's 10 already-seeded rows ever arrived.

## Root cause

```
A previous offset indicating a completed snapshot has been found.
According to the connector configuration no snapshot will be executed
Snapshot ended with SnapshotResult [status=SKIPPED, ...]
```

`snapshot.mode: initial` only snapshots on a connector's **first-ever start**, when no committed offset exists yet. This connector already had a committed offset (from streaming `payment_lines` for hours) — so from Debezium's point of view, "the snapshot" was already done, full stop. Adding a new table to `table.include.list` on an already-initialized connector does not mark that table as needing its own snapshot; the connector goes straight to streaming, which only captures **future** inserts/updates. The 10 rows that existed in `apartment_market_segments` before the config update are gone from the topic's perspective, permanently, unless something writes to them again.

## How we found it

1. `GetOffsetShell` on `apartment-market-segments.v1` showed `0` — the data-plane check this project always reaches for first (same instinct as Phase 2's four incidents), not trusting the connector's `RUNNING` status.
2. `docker logs pms_kafka_connect` showed the exact SKIPPED-snapshot message above — confirmed the mechanism instead of guessing.

## How we fixed it

`UPDATE apartment_market_segments SET updated_at = now();` — touched all 10 rows, forcing Debezium to emit them as streaming `UPDATE` events (which for a dimension table with a stable primary key are practically indistinguishable from what a snapshot would have produced — same current row state). Verified: offset moved to `10`, and message shape matched exactly what `flink_jobs.job._parse_apartment_segment_row` expects.

This is a workaround for a small, static seed table (C.1 confirmed no ongoing reassignment) — not a general solution. It would not work cleanly for a large table (touching every row generates real WAL/replication traffic) or a table whose rows genuinely shouldn't change.

## What to learn from this

- **This is the same underlying lesson as `debezium-heartbeat-topic-stalls-entire-connector.md` and the decimal/date incident's postscript, in a new shape: Kafka Connect's offset/snapshot state is sticky and independent of what the config file currently says.** Editing `table.include.list` changes what the connector *will* watch going forward; it does not retroactively reconcile history for anything newly added.
- **The real fix, for a table where "touch every row" isn't acceptable**, is Debezium's **incremental (ad-hoc) snapshot** feature — a signal table the connector watches for snapshot requests, letting you snapshot a specific table's current rows without disturbing the rest of the connector's streaming position. Not set up here (out of scope for this PoC), but the correct tool if this table ever needs a real backfill again.
- **A full offset reset** (`stop` → `DELETE .../offsets` → `resume`) would also have worked, but at the cost of re-snapshotting `payment_lines` too — its entire multi-hour history, wastefully, just to pick up 10 rows in a different table. Worth knowing this trade-off exists, and that the touch-update workaround avoided it here.

## Situations where you can hit this

Any time a table is added to an **already-initialized** CDC connector's scope, on any CDC system that snapshots-then-streams (Debezium generally, but the same shape of gotcha applies to other CDC tools with a similar model):

- Growing a connector's scope incrementally as a project's schema grows (exactly this case) — a very likely recurring pattern in this project as later phases add more tables.
- Restoring a connector from a backed-up config after schema changes, assuming the restored connector will "just pick up" new tables the same way a brand-new connector would.
- Any runbook that says "just update the config and reload" for a CDC connector — always ask "does this system treat mid-life scope additions the same as a first-time start?" before assuming yes.

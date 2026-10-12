# Incident: Stage A's cost totals were far lower than Postgres's real totals — Kafka retention, not a bug

**Phase:** 4 | **Date:** 2026-07-30 | **Component:** `payment-events.v1` (Kafka), Stage A cost aggregation

## What happened

Live pipeline showed `BCN-001` at `daily_cost_eur=20.51` (`11` cost lines, `635.85€` total) and `rule_applied=market_competitive`. The real total in Postgres for the same apartment/billing period: `147` lines, `13,946.37€` — over 20× higher, which should have produced `cost_protected`. All 190 live decisions came out `market_competitive`, none `cost_protected`/`minimum_floor`, contradicting a manual sample computed minutes earlier from the same Postgres data.

## Root cause

`kafka-consumer-groups --describe` showed the job's consumer group had almost no lag (near the end of the topic) — so it wasn't under-consuming. Checked the topic's actual earliest offset instead:

```
kafka-run-class kafka.tools.GetOffsetShell --topic payment-events.v1 --time -2   # earliest
payment-events.v1:0:209   (latest: 667)
payment-events.v1:3:151   (latest: 474)
payment-events.v1:4:66    (latest: 322)
```

Kafka's own retention had already deleted the oldest ~200+ messages per partition **before this consumer group ever started reading**. Postgres never expires rows; Kafka's log does. Debezium published each `payment_line` event exactly once (ADR-0003) — once its Kafka segment is deleted by retention, that event is gone from Kafka forever, even though the row still exists in Postgres.

## Not a bug in Stage A

`aggregate_cost`'s upsert-by-`event_id` logic is correct — it aggregated everything it was actually given. It was given a partial history because this demo's `payment-events.v1` had already been accumulating for many hours/days across several sessions before this Flink consumer group's very first read.

## What to learn from this

Same shape as Phase 3's already-documented Kinesis 24h retention limitation, one layer deeper: **a downstream consumer that starts late relative to a topic's retention window inherits a partial history, permanently** — not recoverable by re-reading, since the data is actually gone from the broker, not just unread. In a real deployment where Flink starts consuming from day one alongside Debezium, this never happens — the gap only exists in this demo because the CDC pipeline ran for a long time before Stage A ever attached to it.

## Situations where you can hit this

Any time a new consumer (a new consumer group, a rebuilt Flink job, a new analytics pipeline) attaches to a Kafka topic that already has a production history longer than the topic's retention — always check the topic's earliest offset against consumer lag, not just lag against the latest offset, before trusting an aggregation built from "reading everything."

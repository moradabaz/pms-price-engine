# ADR-0008 — A small bridge service republishes Kinesis onto Kafka for Stage B

**Date:** 2026-07-30
**Status:** Accepted

## Context

No Kinesis connector runs on Flink 2.x — confirmed live (`error-handling/flink-2x-removes-legacy-sourcefunction-breaking-flinkkinesisconsumer.md`): `FlinkKinesisConsumer` depends on `RichParallelSourceFunction`, part of the legacy `SourceFunction` API Flink 2.0 removed, and no connector release targets the new Source API for Kinesis yet. Stage B cannot consume `market-price-events` directly.

## Decision

`services/kinesis-kafka-bridge` — a small standalone Python process (same shape as `mock-pm-app`/`market-ingestor`) polls Kinesis via `boto3` and republishes each record unmodified (same bytes, same `event_id`) onto a new Kafka topic, `market-price-bridge.v1`. Stage B reads that topic via `KafkaSource` (confirmed working) instead of `FlinkKinesisConsumer`.

Two options considered and rejected:
- **Downgrade the cluster to Flink 1.20** — `FlinkKinesisConsumer` works there, but loses Flink 2.x.
- **Wait for an official connector** — no released timeline; blocks Phase 4 indefinitely.

## Consequences

- Republishing is at-least-once, never regenerated — consistent with `market_price.v1`'s existing idempotency model (Phase 3 spec §3: consumers dedupe by `collected_at`, not `event_id` uniqueness), so bridge restarts or duplicate republishes are harmless by design, not a new risk.
- One more service, one more topic, one more thing that can go down — the bridge itself has no state to lose (starts from `LATEST` on restart) and no dependents besides Stage B.
- Revisit if a Flink-2.x-compatible Kinesis connector ever ships — this is a workaround for a point-in-time ecosystem gap, not a permanent architecture preference.

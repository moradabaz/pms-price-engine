# Incident: no Kinesis connector can run on Flink 2.x

**Phase:** 4 | **Date:** 2026-07-29 | **Component:** Stage B's Kinesis source, `streaming/flink-jobs/Dockerfile`

## What happened

Submitting the real job to a live Flink 2.3.0 cluster failed constructing the Kinesis source:

```
py4j.protocol.Py4JError: org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer does not exist in the JVM
```

Everything else (both Kafka sources, broadcast state) built fine.

## Root cause

The error message is misleading. The class **is** in the jar (verified with Python's `zipfile` module — no `unzip`/`jar` CLI in this image). The real error, one line earlier in the log:

```
NoClassDefFoundError: org/apache/flink/streaming/api/functions/source/RichParallelSourceFunction
```

`FlinkKinesisConsumer` extends `RichParallelSourceFunction` — part of Flink's legacy `SourceFunction` API, which **Flink 2.0 removed** in favor of the new `Source` API (FLIP-27). No released Kinesis connector targets the new API yet (checked Maven Central: `flink-connector-kinesis` tops out at `5.1.0-1.20`, i.e. Flink 1.20). **No Kinesis connector can run on Flink 2.x today — this is an ecosystem gap, not a config mistake.**

## How we verified it live anyway

Ran a temporary demo (not committed) with just Stage A (Kafka sources + broadcast state, `.print()` instead of the DynamoDB sink) on the same cluster. Confirmed real `payment_line`/`apartment_market_segments` data flows through `CostEnrichmentFunction` correctly, arithmetic verified against the seeded values.

## Lesson

A `Py4JError: "X does not exist in the JVM"` can mean "class missing" **or** "class failed to load because a dependency is missing" — check the Java stack trace *above* the py4j error, not just the last line. Before wiring any connector to a new-major-version engine, check Maven Central for a matching release first (`search.maven.org`) — a cluster version bump does not imply its connectors kept up.

## Resolved 2026-07-30

Built the bridge option: `services/kinesis-kafka-bridge` (boto3 poller, republishes unmodified onto Kafka topic `market-price-bridge.v1`). Stage B now reads that topic via `KafkaSource` instead of `FlinkKinesisConsumer`. See ADR-0008. Verified live: full pipeline (Stage A + Stage B + DynamoDB sink) running, writing real `price_decision` items.

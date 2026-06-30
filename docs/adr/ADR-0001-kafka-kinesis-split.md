# ADR-0001 — Kafka for cost events, Kinesis for market-price events

**Date:** 2026-06-30
**Status:** Accepted

## Context

The pipeline has two event streams with fundamentally different origins:

1. **Payment lines (cost data)** — originate from changes in the PostgreSQL `payment_lines` table. The established tool for capturing these is Debezium (Change Data Capture). Debezium's Kafka Connect integration is mature and battle-tested; its native output target is a Kafka topic.

2. **Market prices** — originate from an external scraper or ETL that polls Airbnb/Booking datasets. These are not database-driven; the producer can publish to any stream endpoint directly.

The question was: should we unify on a single streaming system (all Kafka, or all Kinesis), or use the best tool per use case?

## Decision

Use **Kafka** for `payment-events` and **Kinesis (LocalStack locally, AWS Kinesis in demo)** for `market-price-events`.

## Rationale

- Debezium has no first-class Kinesis sink connector. Forcing CDC events into Kinesis would require a Kafka → Kinesis bridge (MSK Connector, or a custom forwarder), adding an extra hop and a new failure mode with no learning benefit.
- The market-price ingestor has no CDC dependency and can publish to any stream. Using Kinesis here allows learning both streaming systems in contexts where they each make natural sense, which is the stated learning goal of this project.
- In production systems, mixed event buses per domain are common. This architecture mirrors that reality.

## Consequences

- The Flink job must consume from two different sources (Kafka connector + Kinesis connector). Both are supported by PyFlink 2.x.
- Local development requires LocalStack to emulate Kinesis (single endpoint `http://localhost:4566`). The market-ingestor boto3 client must be configured with `endpoint_url` pointing to LocalStack in non-production environments.
- Partition key for both streams is `apartment_id` to guarantee per-apartment ordering and avoid state race conditions in Flink.

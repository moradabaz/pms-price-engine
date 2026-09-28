# ADR-0005 — `market-price-events` partition key: market segment, not `apartment_id`

**Date:** 2026-07-25
**Status:** Accepted — supersedes ADR-0001 §Consequences, third bullet, for the `market-price-events` stream only

## Context

ADR-0001 states: "Partition key for both streams is `apartment_id` to guarantee per-apartment ordering." That was written before `market_price.v1.json` existed in its current form. The schema, as actually specified, has **no `apartment_id` field at all** — a market price snapshot is scoped by `market_area` (`city`, `neighborhood`) and `property_profile` (`type`, `bedrooms`), a segment shared by every apartment matching that profile in that area, not a single apartment. `apartment_id` cannot be the Kinesis partition key for a payload that doesn't contain one.

This surfaced while planning Phase 3 (market ingestion), before any producer code was written — the same category of gap Phase 2 hit four times with Debezium's connector defaults, except caught here at spec time instead of after a "healthy but wrong" pipeline was already running.

The real question this ADR resolves: what should the `market-price-events` Kinesis partition key be, given the payload's actual identity is a market segment?

## Decision

The `market-price-events` Kinesis `PartitionKey` is a deterministic **plain string** built from the market segment: `f"{city}|{neighborhood or ''}|{property_type}|{bedrooms}"`. The producer does **not** apply its own hash — Kinesis's `PutRecord`/`PutRecords` already MD5-hashes whatever string it's given to place it into a shard's hash-key range; hashing again before sending would add a failure mode (an extra function to keep consistent across producer and verification tooling) for zero benefit. `neighborhood` is normalized to an empty string, not Python's `None`/`"None"`, so two records for the same segment with no neighborhood always produce an identical key. All snapshots for the same segment land on the same shard, in shard-sequence order.

`payment-events.v1` (Kafka) is unaffected — ADR-0001's `apartment_id` partitioning stands as-is there, since `payment_line.v1` genuinely is scoped per apartment.

## Rationale

- **The partition key should reflect the payload's actual identity.** `market_price.v1`'s natural key is the market segment it describes, not an apartment — using segment identity keeps partition affinity meaningful (all history for one segment is co-located, same debugging value Phase 2 got from `apartment_id` affinity on `payment-events.v1`) instead of assigning a key with no relationship to the data.
- **This is a deliberate, disclosed trade-off, not a free win.** Segment-hash partitioning does not, by itself, give Flink (Phase 4) anything it strictly requires for correctness — `collected_at` already exists for event-time watermarking, and per ADR-0003's precedent, consumers must upsert by identity + timestamp rather than depend on delivery order. The benefit here is operational/debugging locality and a genuine opportunity to observe real Kinesis shard behavior, not a correctness guarantee.
- **Hot-shard risk is accepted intentionally, as a learning target.** With a small number of distinct segments (a handful of cities × a handful of property profiles) spread across the stream's 4 shards (`infra/localstack/init-aws.sh`), an uneven segment distribution will concentrate traffic on fewer shards than an even/random key would. This project's explicit purpose is to practice and observe real streaming behavior rather than engineer it away by default — the same reasoning that kept Phase 2's connector-default bugs as documented lessons instead of pre-empting them with defensive config nobody would have learned from.
- **Random/round-robin keying was rejected for this stream**, even though it would eliminate hot-shard risk, because it discards the one piece of operational value a deterministic key gives for free (segment-level locality for manual verification and debugging) in exchange for a benefit (even shard load) this PoC's traffic volume doesn't need.
- **A coarser key (city only) was considered and rejected.** Shard count is fixed at stream-creation time (`--shard-count 4`, `infra/localstack/init-aws.sh`) — it does not grow with the number of distinct partition-key values used; every value, however many, still hashes into one of the existing shards. A coarser key (fewer distinct values — e.g. 3-4 cities) generally *worsens* concentration rather than improving it: it collapses every neighborhood and property profile within a city onto whatever single shard that city's string happens to hash to, while providing no locality benefit `market_price.v1`'s consumer actually needs (Flink's join key is the full segment, not the city alone).

## Consequences

- Phase 3's spec (`specs/phases/03-market-ingestion/spec.md`) must document this partition key explicitly in its own configuration section, and its acceptance criteria must include a segment-affinity check (the Phase 3 equivalent of Phase 2's AC-05), verified on the data plane (`aws kinesis get-records` grouped by shard), not just connector/producer health.
- If synthetic segment generation later turns out heavily skewed toward one or two segments (e.g. most mock data concentrated in one city), a hot shard is an expected, not surprising, outcome — worth writing up in `error-handling/` if it's actually observed and causes a concrete effect (e.g. `ProvisionedThroughputExceededException`), per this project's practice of documenting real incidents rather than hypothetical ones.
- If a future phase needs even shard load instead (e.g. much higher synthetic volume, or a real Inside Airbnb backfill with a skewed city distribution), this ADR must be revisited — the fix would likely mean adding a shard-count increase or switching to random keying, at the cost of losing segment-level locality.

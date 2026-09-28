# ADR-0006 — Flink writes only to DynamoDB; Iceberg is populated via DynamoDB Streams CDC, not a second Flink sink

**Date:** 2026-07-28
**Status:** Accepted

## Context

The Phase 4 pre-spec (`docs/phase-4-streaming-design-decisions.md`, Decision G) established that the DynamoDB sink is safely idempotent by `decision_id` — no transactional sink needed *for that sink alone*. The open question this ADR resolves is different: `price_decision.v1`'s own description says it is "written to both DynamoDB (hot path) and Iceberg (audit trail)" — but *what writes it to Iceberg*, and *when*, was left open (diary: "¿la Fase 4 escribe directo a S3+Iceberg además de DynamoDB, o solo DynamoDB y la Fase 5 se encarga del resto?").

The candidate that looks obvious at first — have the same Flink job write to both DynamoDB **and** Iceberg, as two sinks off the same stream — is exactly the **dual-write problem** Kleppmann describes in *Designing Data-Intensive Applications* (ch. 11, "Keeping Systems in Sync"): when the same application logic writes to two independent systems, there is no atomicity across them. A crash (or a checkpoint restore) between the two writes leaves them permanently inconsistent, with **no way to detect it, let alone repair it**, because neither system has any record that the other was supposed to receive the same update. Under Flink's `EXACTLY_ONCE` checkpointing (Decision F), this gets subtler, not safer: a checkpoint can guarantee exactly-once semantics *per sink*, but does not — and cannot — guarantee that two independently-committing sinks land in the same consistent state relative to each other without a real distributed transaction coordinator, which nothing in this stack provides across DynamoDB and Iceberg/S3.

This project already has the correct pattern for exactly this class of problem, used since Phase 2: **don't write the same fact to two systems independently — pick one system as the leader, and derive the other via its own change log.** Debezium doesn't have the payment-service write to both Postgres and Kafka; it reads Postgres's own WAL and turns it into Kafka events after the fact. The same idea applies here, with DynamoDB Streams playing the WAL's role.

## Decision

**Flink (Phase 4) writes `price_decision.v1` to DynamoDB only** — a single `put_item` per decision, keyed by `decision_id` (already idempotent per Decision G). Flink has no Iceberg sink and no second commit path to reason about.

**Iceberg is populated by a separate, dedicated CDC consumer** (owned by Phase 5, not Phase 4) that reads **DynamoDB Streams** — the change-data-capture log DynamoDB itself exposes for every table with streams enabled — and writes each captured change into the Iceberg table(s). DynamoDB is the leader; Iceberg is a derived, read-optimized, historically-accumulating copy — exactly the same relationship Kafka's `payment-events.v1` has to `payment_lines` (ADR-0001, Phase 2), just with DynamoDB Streams standing in for Debezium/WAL and a to-be-built consumer standing in for Kafka Connect.

Concretely, for this PoC: a small Python service (same shape as `mock-pm-app`/`market-ingestor` — a standalone long-running process, not a Lambda or a second Flink job) polling DynamoDB Streams via `boto3`'s `dynamodbstreams` client, writing records to Iceberg tables via **PyIceberg** (kept Python-native and consistent with this project's existing preference for hand-rolled, explicit consumers over managed glue). This is a Phase 5 deliverable — Phase 4's own spec only needs to state that its sink is single-writer and that Iceberg population is out of its scope, owned downstream.

## Rationale

- **Eliminates the dual-write problem structurally**, rather than mitigating it with retries or reconciliation jobs — there is exactly one system Flink commits to, so there is exactly one thing Flink's checkpointing needs to make exactly-once. This is a straightforward instance of Kleppmann's own resolution to the problem he names, not a novel idea being introduced here for the first time.
- **Reuses a pattern this project has already validated in production-shaped form** (Phase 2's Debezium pipeline, including its four hard-won `error-handling/` lessons about CDC's sharp edges) instead of inventing a new consistency mechanism from scratch. The specific gotchas will differ (DynamoDB Streams is not Postgres logical decoding), but the *shape* of the problem — "read a leader's own change log, don't have application code write to the follower directly" — is one this project already understands.
- **Keeps Phase 4's spec simpler**, not more complex: a single sink with a single idempotency key (Decision G) is easier to reason about, test, and operate than two sinks with independent failure modes under one shared checkpoint.
- **Matches how Phase 5 was already scoped** (`docs/AUDIT_DIARY.md`, Phase 5: "Will consume Phase 4's `price_decision.v1` output into S3 + Iceberg") — this ADR clarifies *how* that consumption happens (CDC via DynamoDB Streams, not a batch export or a second live sink), it does not invent new phase scope.

### Alternatives considered and rejected

| Alternative | Why rejected |
|---|---|
| **Dual-write from the same Flink job** (the option this ADR replaces) | The dual-write problem itself, per Kleppmann — no cross-system atomicity, silent divergence on partial failure, and no way to detect it after the fact. |
| **Two-phase commit across DynamoDB + Iceberg/S3** | No native 2PC coordinator exists between DynamoDB and an S3/Iceberg table in this stack (or in AWS generally, for this pairing) — building one would be significant, unjustified infrastructure for a PoC. |
| **Periodic batch export** (DynamoDB point-in-time export to S3, read by a scheduled job) | Works, but is batch, not streaming — defeats this project's explicit learning goal, and trades near-real-time Iceberg freshness for simplicity that CDC already provides without that trade-off. |
| **DynamoDB Streams via the Kinesis Client Library / DynamoDB Streams Kinesis Adapter** (the AWS-recommended production pattern for custom consumers) | Legitimate and more production-hardened (proper shard lease management, checkpointing via DynamoDB), but Java/KCL-centric — inconsistent with this project's Python-first services (`mock-pm-app`, `market-ingestor`) and adds a JVM dependency to a layer that doesn't otherwise need one. Worth revisiting if this ever needs to be production-grade rather than a PoC. |

## Consequences

- `specs/phases/04-flink-processing/spec.md` (not yet written) must specify a **single** sink (DynamoDB), not two — simplifying the "sink failure handling" acceptance criteria the design doc already called for.
- Phase 5's spec (not yet written) must define: DynamoDB Streams shard iteration and checkpoint tracking for the new consumer (an open design question of its own — likely a small checkpoint record, either in DynamoDB itself or a local/S3 marker, tracking the last processed shard sequence number per shard), and the Iceberg write path (PyIceberg, target table schema mirroring `price_decision.v1`).
- **Known limitation carried forward, worth flagging before it's discovered live:** DynamoDB Streams retains data for only 24 hours, the same shape of limitation Phase 3 already documented for Kinesis's default retention (`specs/phases/03-market-ingestion/spec.md` §7). If the Phase 5 consumer is down for longer than that, those DynamoDB changes are permanently unrecoverable from the stream itself — candidate for its own `error-handling/` write-up if actually observed, not a hypothetical to solve now.
- **Needs live verification, not assumed:** whether LocalStack's DynamoDB implementation supports DynamoDB Streams (`dynamodbstreams` API) to the fidelity this consumer needs. Flagged here so Phase 5's implementation doesn't discover this as a surprise the way Phase 2 discovered Debezium's binary logical-decoding output — check early, against the actual running LocalStack container, before designing the consumer around an assumption.
- This ADR does **not** change Decision G (DynamoDB sink idempotency by `decision_id`) — it composes with it: the same `decision_id` that makes Flink's `put_item` safely retryable is also what the Iceberg-side consumer should upsert/merge by, so a replayed DynamoDB Streams record (DynamoDB Streams itself is at-least-once) doesn't create a duplicate Iceberg row either.

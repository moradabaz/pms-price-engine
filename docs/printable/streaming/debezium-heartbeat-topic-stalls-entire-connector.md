# Incident: A missing heartbeat topic silently stalled the *entire* connector, not just heartbeats

**Phase:** 2 (CDC pipeline)
**Component:** `infra/debezium/postgres-connector.json`
**Date:** 2026-07-16
**Discovered while:** verifying AC-02 (live insert propagates within 5s), immediately after fixing [the topic-naming incident](./debezium-default-topic-naming-mismatch.md)

---

## What happened

Right after fixing the topic-naming bug, the connector reported `RUNNING` and the initial snapshot delivered messages correctly (confirmed AC-01). But a manually inserted row never showed up on `payment-events.v1` — not within 5 seconds, not within 15 minutes. `SELECT count(*) FROM payment_lines` kept climbing (the mock app's generator never stopped), while the topic's total message count sat frozen.

## Root cause

The connector config carried `"heartbeat.interval.ms": "30000"` — a setting nobody had actually decided on; it wasn't in the Phase 2 spec's connector-behavior table (§4) at all, just present in the committed JSON. Debezium heartbeats are published to their own auto-named topic: **`__debezium-heartbeat.<topic.prefix>`** — here, `__debezium-heartbeat.pms`. That topic was never created (same reason as the first incident: broker auto-create is off), so every 30 seconds the connector's producer tried and failed to resolve its metadata:

```
WARN [Producer clientId=connector-producer-pms-payment-lines-connector-0]
  Error while fetching metadata with correlation id ... : {__debezium-heartbeat.pms=UNKNOWN_TOPIC_OR_PARTITION}
```

The surprising part: **this didn't just break heartbeats — it froze the whole connector.** A `KafkaProducer` is shared across every topic a connector writes to, and its single background `Sender` thread gates readiness on having valid cluster metadata for every topic it's been asked to produce to. One topic that can never resolve (because it doesn't exist and never will, absent auto-create) leaves that producer's metadata perpetually "needs update" — which stalled sends to `payment-events.v1` too, even though that topic existed, had a healthy leader, and was working moments earlier.

Confirmed via the replication slot directly — the clearest signal that this was gone, not the topic:

```sql
SELECT confirmed_flush_lsn, pg_current_wal_lsn(),
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots WHERE slot_name='debezium_payment_lines';
```

`confirmed_flush_lsn` was frozen at the exact LSN logged at task startup, while `pg_current_wal_lsn()` kept advancing as the mock app wrote — a growing, unmistakable lag that a `RUNNING` connector status never surfaced.

## How we found it

1. **Data-plane check first, again:** `GetOffsetShell` showed the topic's total message count unchanged since the last check, despite ~20 new rows having been written in between.
2. **Went straight to the replication slot** (`pg_replication_slots.confirmed_flush_lsn` vs `pg_current_wal_lsn()`) rather than trusting connector status — this is the one number that can't lie about whether Debezium is actually consuming WAL.
3. **Connect worker logs** showed the same shape of error as the first incident (`UNKNOWN_TOPIC_OR_PARTITION`), but for a *different* topic name (`__debezium-heartbeat.pms`) — the giveaway that this was a second, distinct config gap, not a recurrence of the first.

## How we fixed it

Removed `heartbeat.interval.ms` entirely rather than creating a third topic to support it. Heartbeats exist to advance a replication slot's position when the *watched table itself* is idle for long stretches (preventing unbounded WAL retention) — not needed here, since `payment_lines` receives real writes every 10–30 seconds from the Phase 1 mock app, which is enough on its own to keep the slot moving.

Applied via a live config update (`PUT /connectors/pms-payment-lines-connector/config`) rather than delete + re-register — no need to touch the replication slot this time, since it was correctly positioned and Postgres had been safely retaining all WAL since the previous fix (that's the entire point of a replication slot: it prevents the WAL segments a subscriber hasn't confirmed yet from being recycled).

Kafka Connect had never persisted an offset for this connector (`"No previous offsets found"` on every restart, because nothing had ever been successfully delivered end-to-end until now), so the config update triggered a fresh `snapshot.mode: initial` snapshot — re-emitting the current table state, then catching up on everything the slot had been retaining. Confirmed recovery two ways: the topic's message count jumped immediately, and the slot's `lag_bytes` dropped to (and stayed near) zero as new rows kept arriving.

## What to learn from this

- **A shared Kafka producer is only as healthy as its least-resolvable topic.** Any topic a connector might write to — business topic, heartbeat topic, dead-letter topic, whatever — needs to exist (or auto-create needs to be on) for the *whole* producer to make progress, not just for that specific record type. This is a sharp edge of the Kafka producer client's metadata model, not a Debezium-specific quirk.
- **`RUNNING` really is silent on producer-level backpressure.** Between this incident and the previous one, two completely different failure mechanisms (wrong topic name; unresolvable second topic) both left the connector reporting healthy indefinitely. A real health check for a CDC pipeline has to include the replication slot's own lag (`pg_wal_lsn_diff`) — it's the one number that directly measures "is this connector actually keeping up," independent of anything Kafka Connect's REST API reports.
- **Every setting in a connector config should be a decision, not an artifact.** `heartbeat.interval.ms` was in the file with no corresponding line in the spec's connector-behavior table — nobody had actually decided the project needed heartbeats. A config value that isn't backed by a documented "why" is exactly where these silent gaps hide. Worth going back and adding a line to `specs/phases/02-cdc-pipeline/spec.md` §4 for whatever the config ends up being once this is fully verified.
- **Don't drop a replication slot reflexively when recovering from a stuck connector.** The first incident's recovery correctly dropped the slot (nothing had been captured yet, so there was nothing to lose). This time, the slot was already correctly positioned past a working snapshot, and Postgres had been retaining the WAL exactly so it *could* be replayed — dropping it would have thrown away real backlog for no reason. Whether to drop a slot during recovery depends on whether it's actually behind something worth keeping, not habit.

## Situations where you can hit this

The specific trigger here was `heartbeat.interval.ms`, but the underlying mechanism — **one unresolvable topic on a shared producer stalls every topic that producer writes to** — is much more general. Watch for it whenever a connector (or any single Kafka producer instance) has more than one logical output:

- **Dead-letter queues.** `errors.deadletterqueue.topic.name` on a sink connector, or any DLQ pattern, only helps if that topic exists *before* the first error occurs. If it doesn't, the DLQ write itself can stall the connector at exactly the moment you needed error handling to work.
- **Multi-table / multi-topic connectors.** A connector capturing several tables (or routing to several destination topics) can have some topics pre-created and others not — added a table to `table.include.list` without pre-creating its topic, and the whole connector can stall, not just delivery for the new table.
- **Kafka ACL / authorization gaps** — not just missing topics. Kafka deliberately reports `UNKNOWN_TOPIC_OR_PARTITION` for a topic the caller isn't authorized to see (`Describe`/`Read`/`Write` denied), rather than an authorization-specific error — a security-through-obscurity choice that means a permissions misconfiguration on a topic that genuinely exists produces the *exact same symptom* as this incident. Don't assume "topic missing" without checking ACLs too.
- **A topic that existed and stops existing** — deleted by mistake (`kafka-topics --delete` against the wrong name), or its only partition's leader broker goes down and doesn't fail over (no replicas, as we deliberately run here with `replication-factor 1`). Same producer-wide stall, whether the cause is permanent or a transient broker blip.
- **Turning on "nice to have" connector features without provisioning for them** — heartbeats (this incident), exactly-once transaction markers, or audit/side-output topics enabled because a tutorial or default config included them, without the matching topic being part of the environment's initial bring-up.
- **Whenever topic creation and connector registration are separate manual steps** (as we deliberately chose here — see spec §6). Nothing ties the two together at config-write time; every new setting added to a connector config later needs to be re-checked against "does every topic this could possibly write to actually exist," because the only thing that will ever tell you otherwise is a runtime metadata-fetch failure, discovered exactly the way this one was.

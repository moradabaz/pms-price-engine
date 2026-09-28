# Incident: Debezium connector reported `RUNNING`, published zero messages

**Phase:** 2 (CDC pipeline)
**Component:** `infra/debezium/postgres-connector.json`
**Date:** 2026-07-16

---

## What happened

After registering the Debezium PostgreSQL connector (`pms-payment-lines-connector`) and confirming both the connector and its task reported `"state": "RUNNING"` via the Kafka Connect REST API, the acceptance check for AC-01 (snapshot backfill) found **zero messages** on `payment-events.v1`:

```bash
$ docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic payment-events.v1
payment-events.v1:0:0
payment-events.v1:1:0
payment-events.v1:2:0
payment-events.v1:3:0
payment-events.v1:4:0
payment-events.v1:5:0
```

All six partitions sat at offset 0, despite `payment_lines` already holding 200+ seeded rows and a healthy `RUNNING` connector.

## Root cause

Debezium names the Kafka topic for a captured table using its **default topic-naming convention**: `<topic.prefix>.<schema>.<table>`. With `topic.prefix: "pms"` and `table.include.list: "public.payment_lines"`, every change event was actually being published to **`pms.public.payment_lines`** — not `payment-events.v1`, the topic name the spec (and the Kafka topic we manually created with 6 partitions) actually uses.

`infra/debezium/postgres-connector.json` had a transform to flatten the CDC envelope (`transforms.unwrap`), but **no transform to rename the output topic**. Nobody had wired the two together — the config and the spec's chosen topic name had silently diverged.

Two things conspired to hide this rather than fail loudly:

1. **`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`** meant `pms.public.payment_lines` was never silently created either — so the connector's producer sat retrying `Metadata` requests forever (`UNKNOWN_TOPIC_OR_PARTITION`), rather than either succeeding (to the wrong topic) or failing outright.
2. **Kafka Connect's `RUNNING` state reflects the control plane, not the data plane.** The REST status only means "the task's poll loop is alive," not "records are being delivered." A producer stuck retrying metadata fetches is invisible from `/status` — it only becomes a `FAILED` task once `delivery.timeout.ms` (default 120s) elapses with no successful send, which is much later than an operator instinctively checks. The Kafka Connect worker logs (`docker compose logs kafka-connect`) were the only place the problem was actually visible, as a wall of `WARN` (not `ERROR`) lines.

## How we found it

1. `GetOffsetShell` showed all target-topic partitions at offset 0 — data-plane check, contradicting the healthy `RUNNING` status.
2. `docker compose logs kafka-connect --since 5m` surfaced the real signal: repeated
   `Error while fetching metadata ... {pms.public.payment_lines=UNKNOWN_TOPIC_OR_PARTITION}` —
   the topic name in the log was the giveaway that Debezium was targeting a topic nobody had created or expected.

## How we fixed it

Added a second Single Message Transform (SMT), `RegexRouter`, chained after the existing `unwrap` transform, to rename the topic at publish time:

```json
"transforms": "unwrap,route",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "pms\\.public\\.payment_lines",
"transforms.route.replacement": "payment-events.v1"
```

Transform order doesn't matter here — `RegexRouter` only rewrites the topic the record is produced to, and `unwrap` only rewrites the record's key/value shape — but conventionally we chain routing after unwrapping.

Recovery steps taken (safe because nothing had ever been successfully delivered — Kafka Connect had logged `"No previous offsets found"`, so there was no committed state to reconcile):

1. `DELETE /connectors/pms-payment-lines-connector` — stop the broken task.
2. `pg_drop_replication_slot('debezium_payment_lines')` — the slot was still marked `active` for a few seconds after task deletion (client disconnects asynchronously); retried once it went inactive.
3. Fixed the connector JSON (above).
4. Re-registered via `POST /connectors`. Confirmed both `connector.state` and `tasks[0].state` were `RUNNING`, **and** independently confirmed via `GetOffsetShell` that partition offsets were now non-zero and roughly tracking `SELECT count(*) FROM payment_lines`.

## What to learn from this

- **Debezium's default topic name is never your business topic name.** Unless you explicitly route it, every table lands on `<topic.prefix>.<schema>.<table>`. If your data contract (spec, downstream consumers, ADRs) names a specific topic, that name must be enforced by an explicit `RegexRouter` (or equivalent) — it is never the default, and nothing will warn you at config-validation time that your intended topic and the connector's actual output topic disagree.
- **A connector `status` of `RUNNING` is a control-plane signal only.** It tells you the task hasn't crashed; it tells you nothing about whether records are actually landing where you expect. Always verify with a data-plane check (consumer offset count, `kcat`, or an actual consumed message) before trusting a green connector status — especially right after registration.
- **Disabling topic auto-create is what made this debuggable at all.** Had auto-create been on, Debezium would have silently created `pms.public.payment_lines` with cluster-default partitions/replication and messages would have flowed — just to the wrong place, with the wrong partition count, invisibly diverging from the spec until some downstream consumer came up empty. A production-realistic, auto-create-off posture turns silent architecture drift into a loud (if slow to notice) failure. This is a concrete payoff of a decision made purely "to learn/practice production habits" in an earlier session.
- **Kafka Connect worker logs are the actual source of truth during connector setup**, not the REST status endpoint. `WARN`-level retry spam that looks like background noise can be the entire story.

## Situations where you can hit this

The specific mismatch here was our chosen topic name (`payment-events.v1`) versus Debezium's default (`pms.public.payment_lines`), but the general trap — *the topic a connector actually writes to and the topic name written down anywhere else (a spec, a downstream consumer, a dashboard query) silently diverge* — shows up in several recurring situations:

- **Any time a target topic name doesn't match `<topic.prefix>.<schema>.<table>`** (or, on older Debezium versions, `<database.server.name>.<schema>.<table>`). If a naming convention, a shared team standard, or an existing consumer's expectation requires a different shape, a `RegexRouter` (or equivalent SMT) is mandatory — there is no config flag that changes the default naming scheme itself.
- **Multi-table connectors where only some tables get an explicit route.** It's easy to add a regex covering the table you're actively working on and forget that every *other* table captured by the same connector still falls through to the default name.
- **Renaming a table in Postgres** (`ALTER TABLE ... RENAME`) changes Debezium's default topic name out from under you, since it's derived from the table's current name — any consumer still pointed at the old topic silently stops receiving data, with no error on either side.
- **Copy-pasting a working connector config for a new table or schema** without updating `table.include.list` *and* the route's regex/replacement pair together. The regex can silently stop matching the new source topic name, so records flow to Debezium's default for the new table instead of the intended one.
- **Per-environment `topic.prefix` values** (e.g., `dev`, `staging`, `prod`) changed without updating a route regex that was hardcoded against one specific prefix — works in the environment it was written for, silently breaks in every other one.
- **A schema/data contract (like `specs/events/*.json` here) written before the connector config, or by a different person/team than whoever wires up the connector** — exactly what happened in this repo. The two artifacts have no automatic link to each other; only a live data-plane check (as opposed to reading the spec and assuming it's implemented) catches the drift.

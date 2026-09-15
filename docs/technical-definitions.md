# Technical definitions: Debezium CDC configuration

**Who this is for:** anyone who needs to understand not just *what* the Debezium connector
(`infra/debezium/postgres-connector.json`) is configured to do, but *why* each setting has the
value it has, what the alternatives were, and what was traded away by picking it.

**What this is not:** it is not a Debezium reference manual. Only the settings this project
actually uses are covered, each grounded in a concrete trade-off made for this pipeline, several
of them discovered the hard way (see [`error-handling/`](../error-handling/) for the full
incident write-ups referenced below).

---

## 1. `snapshot.mode: initial`

**Definition:** controls whether and when Debezium performs a full snapshot of the captured
tables before switching to pure streaming from the WAL.

**Possible values:** `initial` (snapshot only on the connector's first-ever start, when no
committed offset exists yet) · `initial_only` (snapshot, then stop, never streams) ·
`never`/`no_data` (streaming only, assumes the consumer does not need history) · `when_needed`
(re-snapshots if the existing offset or slot is no longer valid) · `schema_only` (structure only,
no row data).

**Trade-off of `initial`:** gives a complete backfill on day one with no manual work, but it
**only ever runs once**. Adding a new table to `table.include.list` on an already-initialized
connector (a committed offset already exists) does not trigger a snapshot for that table — only
future changes are captured, the existing rows are gone from the topic's perspective (see
[`error-handling/debezium-adding-a-table-to-a-running-connector-skips-its-snapshot.md`](../error-handling/debezium-adding-a-table-to-a-running-connector-skips-its-snapshot.md)).
`when_needed` avoids this but is operationally more expensive and less predictable about when a
snapshot will fire.

---

## 2. `message.key.columns`

**Definition:** explicit override of which column(s) Debezium uses as the Kafka record key,
replacing the default (the table's primary key column).

**Values:** a `;`-separated list of `table:column[,column2]` pairs. Here:
`public.payment_lines:apartment_id;public.bookings:apartment_id`.

**Trade-off:** the default (primary key) is ideal for log compaction and idempotent upserts, but
it is orthogonal to what a pipeline needs for business-level ordering. Forcing `apartment_id`
guarantees Kafka's hash partitioner groups every event for one apartment onto the same partition,
a requirement for Flink to keep correct per-apartment keyed state without race conditions. The
cost: the "one key = one unique row" semantics are lost (several `payment_lines` rows for the same
apartment share a key), so log compaction, if it were ever enabled, would no longer behave as a
per-row upsert. That cost is only theoretical here since this project does not use compaction. See
[`error-handling/debezium-default-key-breaks-partition-affinity.md`](../error-handling/debezium-default-key-breaks-partition-affinity.md)
for the incident that surfaced this.

---

## 3. `RegexRouter` (`transforms.route*`)

**Definition:** a Kafka Connect Single Message Transform that rewrites a record's destination
topic name via a regex/replacement pair, without touching its key or value.

**Values:** any `regex`/`replacement` pair; one is configured per table here (e.g.
`pms\.public\.payment_lines` → `payment-events.v1`).

**Trade-off:** without it, the topic name is Debezium's default
(`<topic.prefix>.<schema>.<table>`), coupled to the database's internal structure, ugly but
functional. With the router, topic names stay stable and readable even if the source schema/table
changes, but it **adds one piece of manual configuration per table** that can drift: adding a new
table and forgetting its router silently falls back to the default naming, exactly what happened
in
[`error-handling/debezium-default-topic-naming-mismatch.md`](../error-handling/debezium-default-topic-naming-mismatch.md).

---

## 4. `decimal.handling.mode: double`

**Definition:** controls how Debezium serializes Postgres `NUMERIC`/`DECIMAL` columns.

**Values:** `precise` (default; Kafka Connect's `Decimal` logical type, an exact `BigDecimal`'s
raw bytes, but only decodable by a schema-aware consumer) · `double` (a plain JSON/JS number, with
floating-point rounding risk) · `string` (an exact decimal string, human-readable but requires the
consumer to parse it).

**Trade-off of `double` (the value chosen):** with `schemas.enable: false` (plain JSON, no Schema
Registry, per ADR-0003), `precise` is unusable, it comes out as an opaque base64 string. `double`
produces native JSON numbers, easy to consume, but **introduces floating-point rounding error on
monetary values**. Acceptable for this learning-focused PoC; the incident write-up
([`error-handling/debezium-date-decimal-wire-encoding-mismatch.md`](../error-handling/debezium-date-decimal-wire-encoding-mismatch.md))
explicitly notes that a production pricing engine handling real money should instead use `string`
paired with a schema change to `type: string`, trading numeric convenience for exact precision.

---

## 5. Custom `DateStringConverter` (Java, `infra/debezium/custom-converters/`)

**Definition:** a Debezium extension using the `CustomConverter` SPI (not a Kafka Connect SMT)
that intercepts `DATE` columns and serializes them as ISO date strings instead of Debezium's own
logical type (`io.debezium.time.Date`, an integer count of days since epoch).

**Values:** there is no built-in configurable "mode" for this, that is precisely why the converter
had to be written: **no** standard Kafka Connect SMT (including `TimestampConverter`) recognizes
Debezium's own logical types.

**Trade-off:** there was no free option here. The alternative to writing this converter would have
been Avro + Schema Registry, which does preserve the logical type with real semantics, but that
contradicts the decision to keep plain, schema-registry-free JSON (ADR-0003). The cost paid is
maintaining a small piece of custom Java code, compiled separately and mounted as a connector
plugin, in exchange for a simpler event contract. Full investigation trail in
[`error-handling/debezium-date-decimal-wire-encoding-mismatch.md`](../error-handling/debezium-date-decimal-wire-encoding-mismatch.md).

---

## 6. `transforms.unwrap` (`ExtractNewRecordState`)

**Definition:** a Debezium SMT that flattens the full CDC envelope
(`{before, after, source, op, ts_ms}`) down to just the "after" row state, the plain business
event.

**Relevant sub-settings:** `drop.tombstones: true/false` (whether a `null`-value tombstone is
emitted for deletes, relevant for compaction) · `delete.handling.mode: drop/rewrite/none` (what to
do with `DELETE` operations). Here: `drop.tombstones=true`, `delete.handling.mode=drop`.

**Trade-off:** flattening massively simplifies consumption, Flink receives the business object
directly instead of a CDC envelope, but the operation type (`op`) and the previous row state
(`before`) are lost, there is no way downstream to tell an `INSERT` from an `UPDATE`, or to see
what actually changed. With `delete.handling.mode=drop`, a Postgres `DELETE` disappears from the
topic silently, no tombstone, no event. Acceptable here because every captured table
(`payment_lines`, `bookings`, etc.) is insert/update-heavy in practice, but it would be a problem
for any table whose consumers genuinely need to react to row deletion.

---

## 7. `errors.tolerance: none`

**Definition:** Kafka Connect's policy for handling a conversion/transformation error on an
individual record.

**Values:** `none` (any error stops the task) · `all` (the problematic record is skipped,
optionally routed to a dead letter queue).

**Trade-off of `none`:** guarantees there is never silent data loss, an encoding error, for
example, halts the entire connector rather than silently dropping the row. The cost is
availability: a single malformed record can take down ingestion for all 9 tables until a human
intervenes. In production, at real volume, many teams use `all` + a DLQ to avoid blocking the
whole pipeline over one anomalous row, but that requires having already solved the "DLQ topic that
does not exist yet" trap (the same shared-producer failure mode described in
[`error-handling/debezium-heartbeat-topic-stalls-entire-connector.md`](../error-handling/debezium-heartbeat-topic-stalls-entire-connector.md)).

---

## 8. Postgres replication slot

**Definition:** an internal Postgres structure (`pg_replication_slots`) that retains WAL segments
not yet confirmed by the subscriber (Debezium), guaranteeing it can resume exactly where it left
off.

**Values:** not a multi-valued setting, it is binary: the slot exists or it does not
(`slot.name`, here `debezium_payment_lines`). The `plugin.name` does vary (`pgoutput` vs
`wal2json`, etc.); here it is `pgoutput`, native since Postgres 10, no external plugin required.

**Trade-off:** the slot is the entire reason "no changes are lost" holds while the connector is
down, Postgres will not recycle that WAL until it is confirmed. The real operational risk: **if
the connector dies and is not repaired, retained WAL grows unbounded and can fill Postgres'
disk**, there is no automatic TTL. This requires actively monitoring lag
(`pg_wal_lsn_diff`), which this project currently does manually, not via automated alerting.

---

## 9. `auto.create.topics.enable: false` (Kafka broker setting)

**Definition:** whether the broker automatically creates a topic the first time something tries
to produce or consume against a name that does not exist yet.

**Values:** `true` / `false`.

**Trade-off of `false` (the value chosen):** with `true`, any topic name mismatch "resolves"
itself silently by creating a new topic with cluster-default configuration (partitions,
replication), the naming error stays invisible until a downstream consumer comes up empty, much
later. With `false`, the same mismatch fails loudly and fast (`UNKNOWN_TOPIC_OR_PARTITION` in the
logs), more operational discipline (topics must be created explicitly) in exchange for errors that
surface in seconds instead of silently in production. This is what made the incidents in
[`error-handling/debezium-default-topic-naming-mismatch.md`](../error-handling/debezium-default-topic-naming-mismatch.md)
and
[`error-handling/debezium-heartbeat-topic-stalls-entire-connector.md`](../error-handling/debezium-heartbeat-topic-stalls-entire-connector.md)
debuggable at all.

---

## 10. Health check via `confirmed_flush_lsn` vs `pg_current_wal_lsn()`

**Definition:** a direct comparison between the WAL position Debezium has confirmed processing
(`confirmed_flush_lsn`, a column of `pg_replication_slots`) and Postgres' current write position
(`pg_current_wal_lsn()`). The difference (`pg_wal_lsn_diff`) is the real replication lag, in
bytes.

**Values:** not a configuration setting, a monitoring query. There are no "modes."

**Trade-off:** it is the only signal that cannot lie about whether the connector is actually
keeping up, unlike Kafka Connect's REST `RUNNING` status, which only means the polling thread has
not died (the same misleading symptom appeared in two unrelated incidents,
[`error-handling/debezium-default-topic-naming-mismatch.md`](../error-handling/debezium-default-topic-naming-mismatch.md)
and
[`error-handling/debezium-heartbeat-topic-stalls-entire-connector.md`](../error-handling/debezium-heartbeat-topic-stalls-entire-connector.md)).
The cost: **it is not automated** in this project, it is a manual query, not an alert or a
healthcheck wired into `docker-compose.yml`. For production, this would be the first metric to
expose to Prometheus/Grafana with a lag threshold alert.

---

## How this relates to the rest of the documentation

- [`README.md`](../README.md) `CDC Configuration and Replication Reliability`: the summarized
  version of this document, aimed at a first-time reader of the repo.
- [`error-handling/`](../error-handling/): the full incident write-ups this document draws its
  trade-offs from, each with root cause, how it was found, and how it was fixed.
- [`infra/debezium/postgres-connector.json`](../infra/debezium/postgres-connector.json): the live
  configuration every setting above refers to.

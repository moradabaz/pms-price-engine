# Incident: Debezium's wire encoding for DATE and NUMERIC columns didn't match the data contract

**Phase:** 2 (CDC pipeline)
**Component:** `infra/debezium/postgres-connector.json`, `infra/debezium/custom-converters/`
**Date:** 2026-07-16
**Discovered while:** spot-checking a captured live message ahead of AC-03 (schema conformance)

---

## What happened

A message captured straight from `payment-events.v1` looked structurally right (right fields, right topic) but failed to match `specs/events/payment_line.v1.json` on two field families:

```json
"billing_period_start": 20635,
"billing_period_end": 20665,
"amount_gross": "EGg=",
"vat_rate": "ANI=",
"amount_net": "DY8="
```

The schema requires `billing_period_start`/`billing_period_end`/`due_date`/`payment_date` to be `"format": "date"` (ISO strings like `"2026-07-01"`) and `amount_gross`/`vat_rate`/`amount_net`/`allocation_ratio` to be `"type": "number"`. Instead: DATE columns arrived as raw integers, and NUMERIC columns arrived as base64-encoded byte strings.

## Root cause

Both are consequences of Debezium's **default, precision-preserving temporal/decimal encoding**, designed for schema-aware consumers (Avro + Schema Registry, or a Kafka Connect sink that reads the accompanying schema) — not for plain JSON with `schemas.enable: false` (this project's deliberate choice, see ADR-0003: no envelope, no schema registry, just the flat business event):

- **NUMERIC/DECIMAL columns** (`amount_gross`, `vat_rate`, `amount_net`, `allocation_ratio`) use Debezium's default `decimal.handling.mode: precise`, which represents the value as Kafka Connect's `org.apache.kafka.connect.data.Decimal` logical type — internally a `BigDecimal`'s unscaled value as raw bytes. Serialized through plain `JsonConverter` with no schema attached to interpret it, those bytes come out as an opaque base64 string.
- **DATE columns** (`billing_period_start`, `billing_period_end`, `due_date`, `payment_date`) always use Debezium's own logical type `io.debezium.time.Date` — an `INT32` count of days since the Unix epoch. This is *not* one of Kafka Connect's own logical types (`org.apache.kafka.connect.data.Date`), it's Debezium's own, and — critically — **Kafka Connect's bundled `TimestampConverter` SMT does not recognize it.** Chaining `transforms.dateX.type: org.apache.kafka.connect.transforms.TimestampConverter$Value` against a DATE field fails the task outright:

  ```
  org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
  Caused by: org.apache.kafka.connect.errors.ConnectException:
    Schema Schema{io.debezium.time.Date:INT32} does not correspond to a known timestamp type format
  ```

  This was confirmed empirically (the task failed with exactly this trace the moment the transform was applied) and cross-checked against Debezium's own documentation and community reports — it isn't a version fluke, it's a structural incompatibility: `TimestampConverter`'s schema-name allowlist only contains Kafka Connect's own three logical types, never Debezium's.

Notably, `created_at`/`updated_at` (Postgres `TIMESTAMPTZ`) never had this problem — Debezium's `io.debezium.time.ZonedTimestamp` logical type is *already* a plain ISO-8601 string by default, unlike `Date`. Only the plain-`DATE`-typed columns were affected.

## How we fixed it

**Decimals — a one-line connector config change:**
```json
"decimal.handling.mode": "double"
```
Emits every NUMERIC column as a plain JSON number instead of the `Decimal` logical type. Trade-off, disclosed rather than hidden: `double` can introduce floating-point rounding error on currency values — acceptable for this PoC's learning scope (matches the schema's own `"type": "number"`, not `"string"`), but a production pricing engine handling real money might instead want `decimal.handling.mode: string` paired with a schema change to `"type": "string"`, trading JSON-native numerics for exact precision.

**Dates — Debezium's own documented extension point, not a Kafka Connect SMT.** Since no bundled transform works, we implemented Debezium's `CustomConverter` SPI directly: a ~40-line Java class (`infra/debezium/custom-converters/src/main/java/com/pms/debezium/DateStringConverter.java`) that intercepts any column where `RelationalColumn.typeName()` is `"date"` and registers a `SchemaBuilder.string()` output, formatting the raw value (`LocalDate` in the normal case, with an epoch-day-integer fallback) via `DateTimeFormatter`.

Getting this correctly wired up took genuine investigation, not a guess:
1. **Compiled with a throwaway Maven container** (`docker run --rm -v ".../custom-converters:/app" maven:3.9-eclipse-temurin-17 mvn package`) — no local Maven install needed. `debezium-api`/`connect-api` are `provided` scope, since the running `debezium/connect:2.6` container already has matching versions on its classpath.
2. **Mounted the built JAR into the connector's own existing plugin directory** (`/kafka/connect/debezium-connector-postgres/`) via a `docker-compose.yml` volume — no custom Docker image build required, because Kafka Connect's plugin classloader isolation treats everything in that one directory as a single shared classloader, and our JAR only needs to be visible to that classloader, not globally.
3. **Verified the exact config property names against Debezium's own source**, not blog posts — two different web sources disagreed on whether it's `<name>.type` or `converters.<name>.type`. Rather than guess, we pulled `CustomConverterServiceProvider.java` directly from `debezium/debezium` on GitHub via `gh api`, which settled it precisely: `configuration.getInstance(name + ".type", ...)` — unprefixed, confirming `"dates.type": "..."` was correct all along.
4. **First verification attempt was a false negative** — the initial captured message still showed integers, which looked like the converter wasn't loading. It turned out to be a *stale* message: `payment-events.v1` retains every message from every previous snapshot re-run (harmless, by upsert-semantics design — see the heartbeat incident), so the "newest line in a `--from-beginning` dump" isn't reliably the newest event. Re-testing against a freshly inserted, uniquely-tagged row confirmed the fix immediately (`billing_period_start: "2026-08-01"`, a proper string).

## Situations where you can hit this

- **Any time you skip Schema Registry / Avro and use plain `JsonConverter` with `schemas.enable: false`** (a deliberate simplicity choice here, but a common one for PoCs and lightweight pipelines) — you lose the schema information that would otherwise let a consumer correctly interpret Debezium's logical types. Every `DATE`, `TIME`, `TIMESTAMP` (non-TZ), and `NUMERIC`/`DECIMAL` column is affected; only `TIMESTAMPTZ` (`ZonedTimestamp`) happens to already serialize as a human string by default.
- **Any consumer-facing data contract that specifies "plain JSON number" or "ISO date string"** for a column backed by a CDC pipeline — that shape is never Debezium's default for those types; it always has to be configured for, explicitly, at the source. Assuming "the connector will just emit sensible JSON" is exactly the assumption that broke here.
- **Reaching for `org.apache.kafka.connect.transforms.TimestampConverter` against *any* Debezium-sourced temporal field** — it only recognizes Kafka Connect's own logical type names. This applies to `io.debezium.time.Date`, `io.debezium.time.Timestamp`, `io.debezium.time.MicroTimestamp`, and others — none of Debezium's own logical types are recognized by that stock SMT, so the same failure mode awaits anyone who reaches for it against `TIMESTAMP`/`TIME` columns too, not just `DATE`.
- **Trusting a single captured message as proof a fix worked (or didn't).** Because Debezium re-snapshots on every offset-less restart, and nothing is ever deleted from a Kafka topic by default, "the wrong shape" and "the right shape" for the same logical row can both be sitting in the same topic simultaneously. Any verification against a live topic needs to target a message you can uniquely identify as freshly produced (a just-inserted row with a distinguishing marker), not just "the last line in a consumer dump."
- **Any custom Kafka Connect plugin (SMT, converter, or connector) added after initial infra bring-up** — needs a real place to be compiled and a real classloader to be visible in. Here that meant a disposable Maven container (no permanent build tooling added to the repo or host) and a volume mount into the *existing* plugin directory rather than a bespoke Docker image — the lower-effort option that was still correct, worth defaulting to before reaching for a custom image build.

## What to learn from this

- **Schema-less JSON and CDC precision-preserving encodings are in tension by design**, not by bug — Debezium's defaults exist specifically to preserve full fidelity for schema-aware consumers; a "just give me plain JSON" contract has to actively opt out of that fidelity (`decimal.handling.mode`) or route around it (`CustomConverter` for dates), field family by field family.
- **Not every gap has (or needs) an SMT-shaped fix.** `decimal.handling.mode` was a one-line connector config. Dates needed Debezium's actual extension mechanism — recognizing which category a problem falls into (a connector *setting* vs. something that needs *code*) upfront avoids blindly retrying variations of the wrong tool.
- **When two sources disagree on an exact config key, go to the source code.** A blog post and a Q&A thread gave contradictory property-naming conventions for the same feature; `gh api` pulling the actual parsing logic from `debezium/debezium` settled it in one read, with zero ambiguity left.
- **A disposable build container (`docker run --rm maven:... mvn package`) is enough to extend a system with real code**, without adding permanent build tooling to the host or the repo — the JAR it produces is a plain artifact that just needs to land in the right classloader's directory.

## Postscript: fixing the connector didn't retroactively fix already-committed messages

Validating *every* live message against the schema (not just a hand-picked one) surfaced one more thing: 365 of 398 messages still failed — all of them rows from the original historical backfill, still carrying the pre-fix encoding, even though freshly-inserted rows were correct.

**Why:** Kafka Connect persists source offsets keyed by Debezium's *logical server name* (`source_info[server='pms']`), in its own internal offsets topic — completely independent of the Connector object's lifecycle. Deleting and re-registering the connector (as done for the two earlier incidents) does **not** clear this. And `snapshot.mode: initial` only performs a snapshot when *no offset is found* — once one exists (which it did, from the healthy streaming window before the decimal/date fix), every later restart skips the snapshot entirely (`SnapshotResult [status=SKIPPED]`) and just resumes streaming. The historical rows that were snapshotted once, under the broken encoding, were never going to be reprocessed by any further connector restart, decimal fix or not.

Confirmed directly in the logs on one restart attempt:
```
Found previous partition offset ... {lsn_proc=..., messageType=UPDATE, ...}
... but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed if you want to recover.
Snapshot ended with SnapshotResult [status=SKIPPED, ...]
```
(The referenced LSN was already gone because we'd also dropped and recreated the replication slot — two independent pieces of leftover state, the slot and the Connect-managed offset, both needed clearing.)

**The actual fix** (safe here only because this is local demo data with no real consumers depending on current offsets):
1. `PUT /connectors/pms-payment-lines-connector/stop` — the connector must be fully stopped (not just paused) before its offsets can be reset.
2. `DELETE /connectors/pms-payment-lines-connector/offsets` — Kafka Connect's own offset-reset API (available since the KIP-875 offsets management endpoints), explicitly wiping the persisted `{server=pms}` position.
3. Deleted and recreated the `payment-events.v1` topic itself — old messages don't disappear just because the connector restarts; without log compaction enabled, every historical (broken) message would still sit in the log forever alongside the new correct ones.
4. `PUT /connectors/.../resume` — this time genuinely logged `No previous offsets found` and `SnapshotResult [status=COMPLETED]`, producing a topic where every message (`428` distinct rows) validated cleanly against the schema.

**What to learn:** fixing a producer's encoding, transform, or converter is not retroactive. Anything already committed downstream keeps its old shape until something explicitly reprocesses it — a fresh CDC snapshot, a backfill job, or (for a compacted topic) waiting for compaction plus republishing corrected tombstone/rewrite records. This generalizes well beyond Debezium: the same is true of a fixed Spark job's already-written Parquet partitions, a corrected dbt model's already-materialized historical rows, or any pipeline stage where "I fixed the code" is silently assumed to mean "the already-produced output is now correct" — it never does, on its own. Verification has to check the *actual current state of the output*, not just "does newly-produced output look right."

## Sources consulted

- [Debezium Custom Converters - TimestampConverter (dev.to)](https://dev.to/oryanmoshe/debezium-custom-converters-timestampconverter-26hh) — the working code shape for a `CustomConverter` implementation (`configure`/`converterFor`/`ConverterRegistration.register`) that our `DateStringConverter` is modeled on.
- [holmofy/debezium-datetime-converter (GitHub)](https://github.com/holmofy/debezium-datetime-converter) — checked as a possible ready-made converter; confirmed third-party, MySQL-specific, and archived since 2023 — ruled out rather than used.
- [`CustomConverterRegistry.java`, debezium/debezium (GitHub, via `gh api`)](https://github.com/debezium/debezium/blob/main/debezium-connector-common/src/main/java/io/debezium/relational/CustomConverterRegistry.java) — confirmed `converterFor(RelationalColumn, ConverterRegistration)` is called per-column with the real `Column.typeName()`, validating the `"date".equalsIgnoreCase(...)` matching approach.
- [`CustomConverterServiceProvider.java`, debezium/debezium (GitHub, via `gh api`)](https://github.com/debezium/debezium/blob/main/debezium-connector-common/src/main/java/io/debezium/converters/custom/CustomConverterServiceProvider.java) — the deciding source: settled the exact config key format (`<name>.type`, unprefixed) after two web sources gave conflicting conventions (one showed `converters.<name>.type`, prefixed).
- [Kafka Postgres Sink Connector Schema Schema{io.debezium.time.Date:INT32}... (Confluent Community forum)](https://forum.confluent.io/t/kafka-postgres-sink-connector-schema-schema-io-debezium-time-date-int32-does-not-correspond-to-a-known-timestamp-type-format/7997) — another reporter hitting the exact same `TimestampConverter` incompatibility error, confirming it isn't specific to our setup/version.
- [Custom Converters — Debezium Documentation](https://debezium.io/documentation/reference/stable/development/converters.html) and the equivalent [Red Hat build of Debezium user guide chapter](https://docs.redhat.com/en/documentation/red_hat_build_of_debezium/2.5.4/html/debezium_user_guide/developing-debezium-custom-data-type-converters) — both returned HTTP 403 to direct fetches and could only be read second-hand via search-result summaries; the GitHub source files above were used to verify anything load-bearing instead of trusting those summaries.

# User Manual — PMS Price Engine

Practical companion to the root [`README.md`](../../README.md): how to bring the whole stack up
from a fresh clone, what each piece does, and how to read the dashboard once it's running. The
README stays the high-level pitch; this manual is the "I just cloned this, now what" document.

1. [Prerequisites](#1-prerequisites)
2. [Bringing up the stack from scratch](#2-bringing-up-the-stack-from-scratch)
3. [Verifying the pipeline is actually flowing](#3-verifying-the-pipeline-is-actually-flowing)
4. [The dashboard, explained](#4-the-dashboard-explained)
5. [Phase 4 — what Flink does, and what data the client provides](#5-phase-4--what-flink-does-and-what-data-the-client-provides)
6. [Authoring a Manual Override (Phase 14)](#6-authoring-a-manual-override-phase-14)
7. [Stopping the stack](#7-stopping-the-stack)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Prerequisites

- Docker + Docker Compose
- Python 3.11+ and [uv](https://docs.astral.sh/uv/) — only needed to run tests/lint locally, not to run the stack itself
- `curl` — used once to register the Debezium connector
- AWS CLI, optional — only for poking at LocalStack manually (`aws --endpoint-url=http://localhost:4566 ...`)

Nothing here needs a real AWS account. LocalStack emulates Kinesis, S3, and DynamoDB; everything
else (Postgres, Kafka, Debezium, Flink) is a plain Docker container.

---

## 2. Bringing up the stack from scratch

These steps match the sequence actually used to live-verify every phase of this project — not a
theoretical runbook. Three steps are manual on purpose (topic creation, connector registration,
Flink job submission): this project deliberately treats them as one-time operator actions instead
of adding an orchestration harness just to automate a `curl`/`kafka-topics` call (see
[spec 02 §6](../../specs/phases/02-cdc-pipeline/spec.md)).

```bash
# 1. Build every custom image and start the whole stack
docker compose -f infra/docker-compose.yml build
docker compose -f infra/docker-compose.yml up -d

# 2. Wait for Postgres to be healthy and mock-pm-app to have seeded some rows
docker compose -f infra/docker-compose.yml logs -f mock-pm-app
# ...Ctrl-C once you see it looping past the initial seed

# 3. Create the Kafka topics (one-time — this broker runs with
# KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, so nothing creates these for you;
# skipping this step surfaces as UNKNOWN_TOPIC_OR_PARTITION warnings and a
# Debezium/bridge producer that never makes progress, per
# error-handling/debezium-heartbeat-topic-stalls-entire-connector.md's own lesson
# that a RUNNING connector says nothing about whether data is actually flowing)
docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic payment-events.v1 --partitions 6 --replication-factor 1
docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic apartment-market-segments.v1 --partitions 1 --replication-factor 1
docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic market-price-bridge.v1 --partitions 4 --replication-factor 1
docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic owner-contracts.v1 --partitions 1 --replication-factor 1
docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic manual-overrides.v1 --partitions 1 --replication-factor 1

# 4. Register the Debezium connector (one-time — reads payment_lines +
# apartment_market_segments + owner_contracts + manual_overrides,
# Phase 14 ADR-0011 backlog #9)
curl -X POST -H "Content-Type: application/json" \
  --data @infra/debezium/postgres-connector.json \
  http://localhost:8083/connectors

# Confirm it's actually running before moving on
curl -s http://localhost:8083/connectors/pms-payment-lines-connector/status | grep state

# 5. Submit the Flink job (one-time — the JobManager doesn't auto-submit on boot)
docker exec -d pms_flink_jobmanager flink run \
  -pyclientexec /app/.venv/bin/python3 -pyexec /app/.venv/bin/python3 \
  -py /app/streaming/flink-jobs/src/flink_jobs/main.py

# Confirm it's RUNNING with no exceptions
curl -s http://localhost:8081/jobs
```

From here, everything downstream is automatic and tick-based:

- `market-ingestor` publishes market snapshots to Kinesis continuously.
- `kinesis-kafka-bridge` republishes them onto Kafka for Flink to consume (no Flink 2.x Kinesis
  connector exists yet — see [`error-handling/`](../../error-handling/)).
- Flink joins costs + market prices and writes every `price_decision` to DynamoDB (hot path) and,
  via its DynamoDB Streams change feed, to `lakehouse-consumer` → Iceberg (cold path).
- `dbt-runner` transforms Iceberg into the marts the dashboard reads, every 15 minutes.
- `dashboard` is already running against both paths.

Open the dashboard: **http://localhost:8501**

### Services this brings up

| Service | Port | Role |
|---|---|---|
| `postgres` | `5432` | Source DB — `payment_lines`, `apartment_market_segments`, `owners`, `owner_contracts`, `manual_overrides` |
| `mock-pm-app` | — | Seeds + continuously writes synthetic cost rows |
| `zookeeper` | `2181` | Kafka coordination |
| `kafka` | `9092` | Event bus for `payment-events.v1` |
| `kafka-connect` | `8083` | Hosts the Debezium PostgreSQL connector |
| `flink-jobmanager` | `8081` | Flink UI + job submission endpoint |
| `flink-taskmanager` | — | Executes the Flink job's tasks |
| `localstack` | `4566` | Emulates Kinesis, S3, DynamoDB (single endpoint) |
| `market-ingestor` | — | Publishes synthetic market prices to Kinesis |
| `kinesis-kafka-bridge` | — | Republishes Kinesis records onto Kafka for Flink |
| `lakehouse-consumer` | — | DynamoDB Streams → Iceberg (`price_decision_raw`) |
| `lakehouse-maintenance` | — | Periodic Iceberg compaction |
| `dbt-runner` | — | Iceberg → Kimball marts (DuckDB file), every 15 min |
| `dashboard` | `8501` | Streamlit — the only service you actually open in a browser |

---

## 3. Verifying the pipeline is actually flowing

```bash
# Cost events reaching Kafka
docker exec pms_kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payment-events.v1 --from-beginning --max-messages 5

# Decisions landing in DynamoDB
aws --endpoint-url=http://localhost:4566 --region eu-west-1 \
  dynamodb scan --table-name price_decision --select COUNT

# Flink job health
curl -s http://localhost:8081/jobs
curl -s "http://localhost:8081/jobs/<job-id>/exceptions?maxExceptions=5"
```

If the DynamoDB item count isn't growing, check the Flink exceptions endpoint first — most
pipeline stalls in this project trace back there, not to the source services. See
[`error-handling/`](../../error-handling/) for the incidents already diagnosed this way.

---

## 4. The dashboard, explained

Three tabs, refreshing themselves every **60 seconds** (no manual reload needed):

### Current price

Hot path — reads DynamoDB directly, so it's as live as the pipeline itself. One row per known
apartment, its **most recently decided** night (not necessarily tonight — see §5's note on
fan-out gaps):

| Column | Meaning |
|---|---|
| Apartment | `apartment_id` |
| Night | The specific check-in date this price applies to |
| Cost (1-night) | Total cost as if this were a 1-night reservation (`fixed_cost_eur + variable_cost_eur + one_time_cost_eur`, the one-time/turnover cost charged in full, undiluted), always shown in red |
| Market avg | The raw average nightly rate for that apartment's market segment, always shown in blue |
| Suggested price | The price Flink recommends |
| Margin | `(suggested_price / cost) - 1` — the margin this price actually achieves |
| Status | **`Price Below Cost`** (red) — loses money outright. **`Price Below Profit`** (orange) — covers cost but not the PM's target margin. **`Price Above Market`** (orange) — clears the target margin but prices above the raw market average. **`Market Competitive`** (green) — clears the target margin and stays at/below the market average. |
| Min. stay reco. | Minimum Stay as a profitability lever (ADR-0011 backlog #12, Phase 15) — the shortest stay length (nights) that would clear `cost_protected` for this apartment/night, or `—` when already fine at 1 night or when no candidate stay length resolves it (see `specs/phases/15-minimum-stay-recommendation/spec.md`) |
| Cost per reservation | Total cost for a whole reservation at the "Min. stay reco." length — fixed/variable cost recur every night, the one-time/turnover cost is paid once per booking. `—` when there is no recommendation. |
| Suggested price (reservation) | What the whole reservation should be priced at in total — the recommended stay length's own market-competitive per-night rate × its nights, i.e. a price already covering cost + target margin while staying at/below the market average. `—` when there is no recommendation. |

An apartment with no decision yet shows up in a "No decision yet for: ..." caption instead of a
crash.

### Price evolution

Cold path — reads the `fct_daily_price` mart (dbt, up to 15 minutes stale, timestamp shown under
the header). Pick an apartment, see its suggested price night by night as a line chart plus the
underlying table.

### Margin alerts

Cold path — reads `fct_margin_alert`, i.e. every decision where `rule_applied = cost_protected`:
the actionable case where an apartment's costs are pricing it above its own market average. Same
15-minute freshness as price evolution.

**Why two different "freshness" behaviors:** the current-price tab needs no timestamp because
DynamoDB is read live; the other two show `max(ingested_at)` from the mart because dbt only
refreshes on its own schedule. Each tab degrades independently — a DynamoDB outage only breaks
"Current price," a stale dbt run only makes the other two tabs stale, never wrong.

---

## 5. Phase 4 — what Flink does, and what data the client provides

Phase 4 is the only place cost and market price actually become one number. It runs in two
stages, kept separate because they key by different things:

**Stage A** (keyed by `apartment_id`) — for every incoming cost row, it keeps a running,
always-current total per apartment (upsert by `event_id`, never summed twice), split by
`cost_type` into `fixed_cost_eur` / `variable_cost_eur` / `one_time_cost_eur`. It also looks up
that apartment's segment and pricing parameters (next section) from a broadcast side-input.

**Stage B** (keyed by market segment) — whenever a new market price snapshot arrives for a
segment, it fans out to every apartment in that segment and recomputes a full `price_decision`
for it, combining Stage A's latest cost aggregate with the new market number.

### Data the client (property owner) provides

Three numbers, set **per apartment** in the `apartment_market_segments` table (seeded once, kept
up to date by the PM as a normal database row — not derived by Flink, not hardcoded):

| Field | Default | Meaning |
|---|---|---|
| `target_margin` | `0.05` (5%) | The minimum profit margin the owner requires over cost. This is a **floor Flink itself never crosses** — lowering it, or delisting the apartment, is always the owner's call, never automatic. |
| `competitiveness_discount` | `0.05` (5%) | How far below the raw market average the owner is willing to price to stay competitive, once the margin floor allows it. |
| `commission_pct` | `0.15` (15%) | Blended OTA + payment-processing commission. Scales with the final price, so it sits in the floor's denominator rather than being a fixed euro cost. |

Everything else in the formula — the cost breakdown, the market average, the floor itself, the
final recommended price, the achieved margin — is **computed by Flink**, never supplied directly.

### The three-way rule

For every fan-out emission, Flink picks the higher of two forces — the owner's cost floor, and
the market — and records which one won as `rule_applied`:

```
minimum_price_eur          = cost floor (formula below, depends on how far out the night is)
market_reference_price_eur = market_avg × (1 - competitiveness_discount)

market_competitive  → minimum_price_eur <= market_reference_price_eur   (floor doesn't bind — price the market rate)
minimum_floor        → market_reference_price_eur < minimum_price_eur <= market_avg   (floor wins, still ≤ raw market avg)
cost_protected        → minimum_price_eur > market_avg   (floor pushes price above the raw market — actionable: costs are pricing the apartment out of its own market)
```

The floor itself gets **stricter the further out the booking is** (an owner can afford to hold
out for full margin on a booking 45 days away; not on one arriving tomorrow):

| Days to arrival | Floor type | Margin applied |
|---|---|---|
| `> 30` | `structural_full_margin` | Full `target_margin` |
| `15–30` | `structural_reduced_margin` | `target_margin × 0.75` |
| `< 15` | `contribution` | None — break-even on variable + one-time cost only; fixed costs are sunk either way |

Full derivation, worked numeric examples, and the exact division-based formula:
[`specs/phases/04-flink-processing/spec.md §8`](../../specs/phases/04-flink-processing/spec.md),
[ADR-0007](../adr/ADR-0007-price-decision-cost-protected-rule.md),
[ADR-0009](../adr/ADR-0009-profitability-floor-reform.md).

---

## 6. Authoring a Manual Override (Phase 14)

Every phase before this one is read-only downstream of Flink. This is the first human write path
into the pipeline (ADR-0011 backlog #9,
[spec 14](../../specs/phases/14-manual-override/spec.md)): a Property Manager can force a specific
apartment/night's published price, overriding the algorithmic suggestion — authorized, reasoned,
time-boxed, and fully audited in the resulting `price_decision`, never a silent patch.

There is no UI or API for this in the PoC (§1 of the spec) — authoring an override means inserting
a row directly:

```bash
# 1. Insert the override — apartment_id and target_date must match a real,
#    already-seeded apartment/night for Stage C to pick it up.
docker exec -i pms_postgres psql -U pms -d pms_db <<'SQL'
INSERT INTO public.manual_overrides
    (apartment_id, target_date, override_price_eur, reason, authorized_by, valid_until)
VALUES
    ('<apartment_id>', '<YYYY-MM-DD>', 130.00,
     'Pre-event availability push', 'ops@bilemon.example',
     now() + interval '2 days');
SQL

# 2. Confirm the CDC message actually reached Kafka (not just the connector's
#    "RUNNING" status — see error-handling/debezium-heartbeat-topic-stalls-...)
docker exec pms_kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic manual-overrides.v1 --from-beginning --max-messages 1

# 3. Trigger a reprice for that apartment/night — Stage C only re-evaluates
#    on the next cost or market event for it, not on a timer. The fastest
#    way is a new payment_lines row for that apartment (mock-pm-app's
#    generator already does this continuously) or waiting for the next
#    market-ingestor tick.

# 4. Confirm the override applied
aws --endpoint-url=http://localhost:4566 --region eu-west-1 \
  dynamodb get-item --table-name price_decision \
  --key '{"apartment_id": {"S": "<apartment_id>"}, "target_date": {"S": "<YYYY-MM-DD>"}}' \
  | grep -A6 manual_override
```

**Cancelling a live override is an `UPDATE`, never a `DELETE`** — this connector's global
`delete.handling.mode: drop` setting silently drops delete events for every captured table, so a
deleted row's cancellation would never reach Flink:

```sql
UPDATE public.manual_overrides
SET valid_until = now()
WHERE apartment_id = '<apartment_id>' AND target_date = '<YYYY-MM-DD>';
```

The next reprice after `valid_until` passes reverts to the algorithmic price automatically — no
Flink restart needed (spec 14 AC-09).

---

## 7. Stopping the stack

```bash
# Stop containers, keep volumes (Postgres data, LocalStack data, dbt warehouse, Iceberg catalog survive)
docker compose -f infra/docker-compose.yml down

# Full reset — wipes everything, including seeded data (Kafka/Zookeeper have no volume anyway,
# so a plain `down` already loses their topics)
docker compose -f infra/docker-compose.yml down -v
```

---

## 8. Troubleshooting

Every non-trivial incident hit while building this project — root cause, fix, and how to
recognize it again — is written up in [`error-handling/`](../../error-handling/) instead of only
living in a commit message. If the pipeline stalls, a dbt run fails, or LocalStack starts
behaving inconsistently after a restart, check there before re-deriving the diagnosis from
scratch.

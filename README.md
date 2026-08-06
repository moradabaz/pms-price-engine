# PMS Price Engine — Dynamic Pricing Streaming PoC

A data engineering learning project focused on **streaming processing, CDC, and stateful stream processing**, built around a real business problem: dynamic repricing of vacation rental apartments based on operational costs and market prices.

> **Learning focus:** Change Data Capture (Debezium), stateful stream processing (PyFlink), multi-stream joins, and Apache Iceberg for auditable pricing history. The business case is real, but technical decisions are guided by maximising learning depth.

> 📖 **New here?** [`docs/manual/MANUAL.md`](docs/manual/MANUAL.md) walks through bringing up the entire stack from a fresh clone, verifying the pipeline is flowing, and reading every view in the dashboard.

---

## Motivation

A property manager running ~100 vacation rentals adjusts prices manually today. Existing dynamic pricing tools (PriceLabs, Beyond) don't have visibility into each property's real cost structure, so they sometimes set prices below break-even. This engine:

1. Ingests operational cost lines (electricity, water, OTA fees, etc.) via CDC from PostgreSQL
2. Ingests market reference prices from a scraper/mock
3. Combines both streams in Flink to compute a **recommended price per apartment** that guarantees a configurable minimum margin
4. Persists every pricing decision with full traceability for audit

---

## Architecture

```
┌──────────────────────────────────────┐
│     PostgreSQL (existing PMS DB)     │  payment_lines table — WAL (wal_level=logical)
│  Rows inserted by the PM application │  Debezium reads changes via replication slot
└────────────────────┬─────────────────┘
                     │ logical replication slot (pgoutput)
                     ▼
┌─────────────────────┐
│      Debezium       │  Kafka Connect connector
└────────┬────────────┘
         │ publishes payment-events
         ▼
┌─────────────────────┐        ┌──────────────────────────┐
│       Kafka         │        │   Market scraper / ETL   │
│  payment-events.v1  │        │  (Inside Airbnb / mock)  │
└────────┬────────────┘        └────────────┬─────────────┘
         │                                  │ publishes
         │                                  ▼
         │                     ┌────────────────────────┐
         │                     │   Kinesis (LocalStack)  │
         │                     │   market-price-events   │
         │                     └────────────┬───────────┘
         │                                  │
         └──────────────────┬───────────────┘
                            ▼
               ┌────────────────────────┐
               │      Apache Flink      │
               │  - aggregate costs     │
               │    per apartment       │
               │  - join with market    │
               │    price stream        │
               │  - pricing engine      │
               │    (deterministic v1)  │
               └────────────┬───────────┘
                            │
             ┌──────────────┴──────────────┐
             ▼                             ▼
  ┌──────────────────┐        ┌─────────────────────────┐
  │     DynamoDB     │        │      S3 + Iceberg        │
  │  (LocalStack)    │        │    (LocalStack)          │
  │  current price,  │        │  full pricing history,   │
  │  hot path        │        │  audit trail             │
  └────────┬─────────┘        └─────────────────────────┘
           ▼
  ┌──────────────────┐
  │    Streamlit     │
  │    dashboard     │
  └──────────────────┘
```

### Pricing formula

Cost plus a minimum margin is a floor the engine never crosses; above that floor, it competes on
market price. Which force won is recorded on every decision as `rule_applied`
(`market_competitive` / `minimum_floor` / `cost_protected`). `target_margin`,
`competitiveness_discount`, and `commission_pct` are configured **per apartment** — set by the
property owner, never derived by the engine. Full formula, worked examples, and which fields come
from where: [`docs/manual/MANUAL.md §5`](docs/manual/MANUAL.md#5-phase-4--what-flink-does-and-what-data-the-client-provides).

---

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Cost data source | **PostgreSQL 16** | `payment_lines` table, WAL enabled (`wal_level=logical`). Rows written by the PM's existing app — no REST API in the pipeline. |
| CDC | **Debezium** (Kafka Connect) | PostgreSQL connector reads WAL via `pgoutput` replication slot and publishes to Kafka. |
| Event bus — costs | **Apache Kafka** | Topic `payment-events.v1` (partitioned by `apartment_id`) |
| Event bus — market | **Amazon Kinesis** | Stream `market-price-events` — published directly by the scraper, no CDC dependency |
| Stream processing | **Apache Flink** (PyFlink) | Stateful joins, cost aggregation per apartment, pricing rule engine |
| Hot path store | **DynamoDB** | Current recommended price per apartment, low-latency reads |
| Cold path / audit | **S3 + Apache Iceberg** | Full pricing decision history with justification (cost, market, rule applied) |
| Analytical models | **dbt** | Models over Iceberg: price evolution, margin alerts, cost vs price comparison |
| Orchestration | **Airflow** | Batch tasks only (e.g. periodic market price refresh) — not used for streaming |
| Dashboard | **Streamlit** | Current prices (DynamoDB), history (dbt/Iceberg) |
| Local AWS emulation | **LocalStack** | Emulates Kinesis, S3, DynamoDB locally — no AWS account needed for development |
| IaC | **Terraform** | AWS resources for demo deployments (S3, Kinesis, DynamoDB). Run `terraform destroy` immediately after demo. |
| Local infra | **Docker Compose** | Full stack runs locally: Postgres, Kafka, Debezium, Flink, LocalStack |
| Package manager | **uv** | Workspace with a single lockfile across all Python services |
| CI | **GitHub Actions** | Lint (ruff), typecheck (mypy), schema contract validation on every push |

---

## Local Infrastructure

Everything runs locally via Docker Compose. **No AWS account required** — LocalStack emulates all AWS services.

### Services

| Service | Port | Purpose |
|---|---|---|
| PostgreSQL | `5432` | Source database — `payment_lines`, `apartment_market_segments` |
| `mock-pm-app` | — | Seeds + continuously writes synthetic cost rows |
| Zookeeper | `2181` | Kafka coordination |
| Kafka | `9092` | Event bus for `payment-events.v1` |
| Kafka Connect | `8083` | Hosts the Debezium PostgreSQL connector |
| Flink JobManager | `8081` | Flink web UI + job submission |
| Flink TaskManager | — | Executes Flink tasks |
| LocalStack | `4566` | Emulates Kinesis, S3, DynamoDB (single endpoint) |
| `market-ingestor` | — | Publishes synthetic market prices to Kinesis |
| `kinesis-kafka-bridge` | — | Republishes Kinesis records onto Kafka for Flink (no Flink 2.x Kinesis connector exists) |
| `lakehouse-consumer` | — | DynamoDB Streams → Iceberg (`price_decision_raw`) |
| `lakehouse-maintenance` | — | Periodic Iceberg compaction |
| `dbt-runner` | — | Iceberg → Kimball marts (DuckDB), every 15 min |
| Streamlit (`dashboard`) | `8501` | The dashboard |

### Start the stack

```bash
# Build every custom image and start all services
docker compose -f infra/docker-compose.yml build
docker compose -f infra/docker-compose.yml up -d

# Tail logs for a specific service
docker compose -f infra/docker-compose.yml logs -f kafka

# Stop and remove volumes
docker compose -f infra/docker-compose.yml down -v
```

Bringing the containers up is not the whole story: the Debezium connector and the Flink job are
**one-time manual steps**, not auto-submitted on boot. See
[`docs/manual/MANUAL.md §2`](docs/manual/MANUAL.md#2-bringing-up-the-stack-from-scratch) for the
exact commands and the order they need to run in.

### LocalStack AWS CLI

```bash
# Configure a local profile (no real credentials needed)
aws configure --profile localstack
# AWS Access Key ID: test
# AWS Secret Access Key: test
# Default region: eu-west-1

# Example: list Kinesis streams
aws --endpoint-url=http://localhost:4566 --profile localstack kinesis list-streams

# Example: list S3 buckets
aws --endpoint-url=http://localhost:4566 --profile localstack s3 ls
```

---

## Getting Started

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- `kcat` (Kafka CLI) — for manual event inspection
- `psql` — for manual Postgres inspection
- AWS CLI — for LocalStack interaction

### Install dependencies

```bash
# Install all workspace packages in development mode
uv sync
```

### Run contract/schema validation tests

```bash
uv run pytest specs/contracts/ -v
```

### Lint and typecheck

```bash
uv run ruff check .
uv run mypy services/ streaming/ libs/ dashboard/
```

---

## Project Structure

```
pms-price-engine/
├── .github/workflows/       # CI: lint, typecheck, schema validation
├── specs/
│   ├── events/              # JSON Schema definitions — source of truth for all events
│   │   ├── payment_line.v1.json
│   │   ├── market_price.v1.json
│   │   └── price_decision.v1.json
│   ├── contracts/           # Tests that validate producers/consumers against schemas
│   └── phases/              # Per-phase specs (spec-driven development) — requirements,
│       ├── 01-mock-app-db/  # acceptance criteria, non-goals, written before implementation
│       ├── 02-cdc-pipeline/
│       ├── 03-market-ingestion/
│       ├── 04-flink-processing/
│       ├── 05-persistence/
│       ├── 06-dashboard/
│       └── 07-demo-docs/
├── services/
│   ├── mock-pm-app/         # Seeds + writes synthetic payment_lines rows
│   ├── market-ingestor/     # ETL — publishes market prices to Kinesis (LocalStack)
│   ├── kinesis-kafka-bridge/# Republishes Kinesis onto Kafka (Flink 2.x has no Kinesis source)
│   ├── lakehouse-consumer/  # DynamoDB Streams CDC → Iceberg (price_decision_raw)
│   ├── lakehouse-maintenance/ # Periodic Iceberg compaction
│   └── dbt-runner/          # Runs the transform/ dbt project on an interval
├── streaming/
│   └── flink-jobs/          # PyFlink — cost aggregation, stream join, pricing engine
│       └── src/flink_jobs/
│           └── pricing/     # Pricing rule interface + v1 deterministic implementation
├── libs/
│   ├── shared-schemas/      # Pydantic models generated from specs/events (DRY)
│   ├── common/              # Shared logging, config (pydantic-settings)
│   └── lakehouse-shared/    # Iceberg catalog/table helpers shared by consumer + maintenance
├── dashboard/               # Streamlit — current prices (DynamoDB) + history (dbt marts)
├── transform/               # dbt project — Kimball marts over price_decision_raw (Iceberg)
├── infra/
│   ├── docker-compose.yml   # Full local stack incl. LocalStack
│   ├── debezium/            # Debezium connector config
│   └── terraform/           # AWS resources for demo (destroy after use)
├── error-handling/          # One write-up per real incident: root cause, fix, how to recognize it
└── docs/
    ├── adr/                 # Architecture Decision Records
    └── manual/              # Full setup + usage manual (this repo's "getting started" doc)
```

---

## Development Phases

Each phase is spec'd in `specs/phases/<NN-name>/spec.md` — requirements and acceptance criteria written before implementation — starting with Phase 1.

| Phase | Scope | Key output | Spec |
|---|---|---|---|
| 0 | Repo setup | This structure, schemas, CI | — |
| 1 | Mock app & `payment_lines` DB | Postgres table + generator app writing live/seeded cost data; hands-on WAL/logical-replication check | [`specs/phases/01-mock-app-db/spec.md`](specs/phases/01-mock-app-db/spec.md) |
| 2 | CDC pipeline | Postgres → Debezium → Kafka, validated with kcat | [`specs/phases/02-cdc-pipeline/spec.md`](specs/phases/02-cdc-pipeline/spec.md) |
| 3 | Market ingestion | Synthetic `market-ingestor` → Kinesis (LocalStack), verified live | [`specs/phases/03-market-ingestion/spec.md`](specs/phases/03-market-ingestion/spec.md) |
| 4 | Flink processing | Stateful cost⋈market join, three-way pricing rule, dual sink (DynamoDB + Iceberg via CDC) | [`specs/phases/04-flink-processing/spec.md`](specs/phases/04-flink-processing/spec.md) |
| 5 | Persistence | DynamoDB Streams → Iceberg (`lakehouse-consumer`), Kimball marts (`transform/`, dbt) | [`specs/phases/05-persistence/spec.md`](specs/phases/05-persistence/spec.md) |
| 6 | Dashboard | Streamlit reading DynamoDB (hot) + dbt marts (cold) | [`specs/phases/06-dashboard/spec.md`](specs/phases/06-dashboard/spec.md) |
| 7 | Demo & docs | Real AWS demo footprint, `GlueCatalog` promotion, this manual | [`specs/phases/07-demo-docs/spec.md`](specs/phases/07-demo-docs/spec.md) |

---

## Architecture Decisions

See [`docs/adr/`](docs/adr/) for full decision records.

| ADR | Decision |
|---|---|
| ADR-0001 | Kafka for payment-events (Debezium constraint), Kinesis for market-price-events |
| ADR-0002 | PyFlink over Java Flink — single language stack (Python) |
| ADR-0003 | `payment_line.v1` is a flat, metadata-free CDC contract; `event_id` identifies a row and requires keyed-upsert consumption |
| ADR-0004 | pandas pinned to `2.2.x` workspace-wide to satisfy both `apache-flink` and `dashboard` under a single lockfile |
| ADR-0005 | `market-price-events` partitioned by market segment, not `apartment_id` |
| ADR-0006 | Flink writes only to DynamoDB; Iceberg is populated via DynamoDB Streams CDC, not a second Flink sink |
| ADR-0007 | `price_decision.v1` gets a third `rule_applied` state, `cost_protected` — the cost floor priced the apartment above its own market average |
| ADR-0008 | A small bridge service republishes Kinesis onto Kafka for Stage B (no Flink 2.x Kinesis connector) |
| ADR-0009 | Profitability floor reform — division-based (not multiplicative), commission-aware, antelación-tiered |

---

## Cost guardrails

Development and testing run **100% locally** via Docker Compose + LocalStack.  
AWS is used **only for the final demo** and must be torn down immediately after:

```bash
terraform -chdir=infra/terraform destroy
```

Set AWS Budget alerts at **$5 and $10** before touching any real AWS service.

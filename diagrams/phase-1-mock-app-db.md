# Phase 1 — Mock App & `payment_lines` DB (PR #3)

Diagrams for reviewers of [PR #3 "Restructure phase 1 mock app db"](https://github.com/moradabaz/pms-price-engine/pull/3).
Scope is intentionally narrow: **Postgres + a synthetic data generator only**. Kafka, Zookeeper, and
Debezium already exist in `infra/docker-compose.yml` from Phase 0 but are **not started or touched** by
this PR (AC-07) — they're shown dashed/out-of-scope below for orientation, not because this PR changed them.

## 1. Component / data-flow diagram

```mermaid
flowchart TB
    subgraph compose["infra/docker-compose.yml"]

        subgraph pg["postgres service (postgres:16)"]
            wal["wal_level=logical<br/>max_replication_slots=4<br/>max_wal_senders=4<br/><i>(set in Phase 0, confirmed by AC-01)</i>"]
            initscript["docker-entrypoint-initdb.d/01-payment_lines.sql<br/><i>mounted read-only from</i><br/>specs/phases/01-mock-app-db/payment_lines.sql"]
            table[("payment_lines table<br/>+ trg_payment_lines_updated_at trigger<br/>+ dbz_publication (for Debezium, Phase 2)")]
            initscript -->|"runs once, first boot"| table
        end

        subgraph app["mock-pm-app service — NEW"]
            settings["settings.py<br/>MockAppSettings<br/>(pydantic-settings, MOCK_APP_*)"]
            main["main.py"]
            seed["seed.py<br/>one-time backfill"]
            gen["generator.py<br/>run_forever() loop"]
            settings -.configures.-> main
            main -->|"already_seeded()? no"| seed
            main --> gen
        end

        app -->|"psycopg: plain SQL<br/>INSERT / UPDATE"| table
    end

    subgraph libcommon["libs/common — first real consumer"]
        logging["logging.py<br/>configure_logging() / get_logger()<br/>structlog, JSON renderer"]
    end
    app -."import common".-> logging

    subgraph manual["Manual WAL verification (AC-06)<br/>plain psql — no Debezium involved"]
        slot["pg_create_logical_replication_slot(<br/>'manual_ac06_check', 'pgoutput')"]
        decode["pg_logical_slot_get_binary_changes(...)<br/>shows raw I / U messages"]
        drop["pg_drop_replication_slot(...)<br/>cleanup"]
        slot --> decode --> drop
    end
    table -."WAL, read directly".-> slot

    subgraph phase2["Phase 2 — out of scope for this PR"]
        debezium["Debezium connector<br/>(Kafka Connect)"]
        kafkatopic["Kafka topic<br/>payment-events.v1"]
        debezium --> kafkatopic
    end
    table -."not registered/connected yet".-> debezium

    classDef newstuff fill:#1f6feb,color:#fff,stroke:#0b3d91;
    classDef outscope fill:transparent,stroke:#888,stroke-dasharray: 4 3,color:#888;
    class app,seed,gen,main,settings,table,initscript,logging newstuff;
    class debezium,kafkatopic,phase2 outscope;
```

**Legend:** solid blue boxes = built/changed in this PR. Dashed gray = exists conceptually (Phase 2 spec) but not started, wired, or touched here.

## 2. Runtime sequence — seed once, then generate forever

```mermaid
sequenceDiagram
    participant Compose as Docker Compose
    participant PG as Postgres (payment_lines)
    participant App as mock-pm-app
    participant Log as libs/common (structlog)

    Compose->>PG: start postgres
    PG->>PG: run 01-payment_lines.sql<br/>(create table, trigger, dbz_publication)
    PG-->>Compose: healthcheck OK (pg_isready)
    Compose->>App: start mock-pm-app<br/>(depends_on postgres: service_healthy)
    App->>Log: configure_logging(MOCK_APP_LOG_LEVEL)
    App->>PG: already_seeded()?

    alt payment_lines empty
        App->>PG: seed(): backfill ≥2 months history,<br/>≥10 apartments, ≥3 concepts, source='synthetic'
        App->>Log: log "seed_complete" (rows_inserted)
    else rows already present
        App->>Log: log "seed_skipped"
    end

    loop run_forever()
        App->>PG: INSERT new payment_line<br/>(random 10-30s interval)
        App->>Log: log "inserted_payment_line"
        opt every MOCK_APP_UPDATE_CHECK_INTERVAL_SECONDS (60s default)
            App->>PG: UPDATE one pending row → paid<br/>(payment_date set, trigger bumps updated_at)
            App->>Log: log "marked_payment_line_paid"
        end
    end

    Note over PG,App: Kafka / Debezium never started here (AC-07) —<br/>WAL inspected manually via pg_logical_slot_get_binary_changes (AC-06)
```

## What this PR does *not* include

- No Debezium, Kafka Connect, or `payment-events.v1` topic (Phase 2).
- No market ingestion, Flink, pricing logic, DynamoDB/Iceberg, or dashboard (Phases 3–6).
- No retry/failure handling in the generator beyond Compose's `restart: unless-stopped`.

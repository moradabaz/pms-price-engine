# Phase 3 — Market Ingestion (Kinesis)

Diagram for [`specs/phases/03-market-ingestion/spec.md`](../specs/phases/03-market-ingestion/spec.md) — implemented and verified live against a running LocalStack stack on 2026-07-25 (segment-hash partitioning per [ADR-0005](../docs/adr/ADR-0005-market-price-partition-key.md), log-normal price sampling per spec §5.2, bounded retry per spec §4). Verification covered both a direct `uv run` invocation and the actual `services/market-ingestor/Dockerfile` built and run via `infra/docker-compose.yml` — AC-02/03/04/06 all passed reading live records back from every shard, and the ADR-0005 hot-shard prediction was observed directly (a `30/30/10/20` record split across the 4 shards after several ticks). Scope is **`market-ingestor` + the `market-price-events` Kinesis stream only** — Phase 2's Kafka pipeline and Phase 4's Flink consumption are shown dashed, for orientation, not because this phase touches them.

## 1. Component / data-flow diagram

```mermaid
flowchart TB
    subgraph compose["infra/docker-compose.yml"]

        subgraph localstack["localstack service — Kinesis emulation (Phase 0)"]
            initscript["infra/localstack/init-aws.sh<br/><i>runs once, first boot</i>"]
            stream[("Kinesis stream<br/>market-price-events<br/>4 shards")]
            initscript -->|"awslocal kinesis create-stream"| stream
        end

        subgraph ingestor["market-ingestor service — NEW (Phase 3)"]
            settings["settings.py<br/>MarketIngestorSettings<br/>(pydantic-settings, MARKET_INGESTOR_*)"]
            segments["segments.py<br/>18 fixed segments<br/>+ reference price table (spec §5.2)"]
            pricing["pricing.py<br/>log-normal sampling →<br/>avg / p25 / p50 / p75"]
            main["main.py<br/>tick loop, every<br/>MARKET_INGESTOR_TICK_INTERVAL_SECONDS"]
            settings -.configures.-> main
            segments --> main
            main --> pricing
        end

        ingestor -->|"boto3 put_records (batch)<br/>PartitionKey = segment string, ADR-0005"| stream
    end

    subgraph sharedschemas["libs/shared-schemas — first real consumer"]
        model["market_price.py<br/>MarketPrice (pydantic)<br/>validates before publish"]
    end
    main -->|"builds dict"| model
    model -->|"validated → model_dump_json()"| main

    subgraph libcommon["libs/common"]
        logging["logging.py<br/>structlog"]
    end
    main -."import common".-> logging

    subgraph manual["Manual verification (AC-02/03/04)<br/>AWS CLI against LocalStack — no Flink involved"]
        shard_iter["get-shard-iterator"]
        get_records["get-records<br/>(grouped by ShardId)"]
        contract["specs/contracts/test_market_price_contract.py<br/>validate live records"]
        shard_iter --> get_records --> contract
    end
    stream -."read directly".-> shard_iter

    subgraph phase4["Phase 4 — out of scope for this spec"]
        flink["PyFlink job<br/>(Kinesis connector)"]
    end
    stream -."not consumed yet".-> flink

    subgraph phase2existing["Phase 2 — already implemented, parallel pipeline"]
        kafkatopic["Kafka topic<br/>payment-events.v1"]
    end

    classDef newstuff fill:#1f6feb,color:#fff,stroke:#0b3d91;
    classDef outscope fill:transparent,stroke:#888,stroke-dasharray: 4 3,color:#888;
    class ingestor,settings,segments,pricing,main,model,stream,initscript newstuff;
    class flink,phase4,kafkatopic,phase2existing outscope;
```

**Legend:** solid blue = to be built in this phase. Dashed gray = exists (LocalStack init from Phase 0) or is planned elsewhere (Phase 2, Phase 4) but not built/touched here.

## 2. Runtime sequence — one tick, including the retry path

```mermaid
sequenceDiagram
    participant Compose as Docker Compose
    participant LS as LocalStack (Kinesis)
    participant MI as market-ingestor
    participant SS as shared_schemas.MarketPrice
    participant Log as libs/common (structlog)

    Compose->>LS: start localstack
    LS->>LS: init-aws.sh creates<br/>market-price-events (4 shards)
    LS-->>Compose: healthcheck OK (awslocal kinesis list-streams)
    Compose->>MI: start market-ingestor<br/>(depends_on localstack: service_healthy)
    MI->>Log: configure_logging(MARKET_INGESTOR_LOG_LEVEL)

    loop every MARKET_INGESTOR_TICK_INTERVAL_SECONDS (60s default)
        loop for each of 18 segments (spec §5.1)
            MI->>MI: sample sample_size prices<br/>(log-normal, reference price × profile multiplier, §5.2)
            MI->>MI: derive avg/p25/p50/p75 from the sample<br/>+ pick random target_date (±MARKET_INGESTOR_FORECAST_DAYS)
            MI->>SS: MarketPrice(**event_dict)
            SS-->>MI: validated instance (or raises — fail before publish, not after)
            MI->>MI: partition_key = f"{city}|{neighborhood}|{type}|{bedrooms}"<br/>(ADR-0005 — plain string, no app-level hash)
        end
        MI->>LS: put_records(18 records, one batch call)
        LS-->>MI: response: FailedRecordCount + per-record status

        alt FailedRecordCount > 0
            loop up to MARKET_INGESTOR_PUBLISH_MAX_RETRIES, exponential backoff
                MI->>LS: put_records(failed records only — same bytes, same event_id)
                LS-->>MI: response
            end
            opt still failing after max retries
                MI->>Log: log "publish_failed" (WARNING: segment, ErrorCode) — then drop
            end
        end
        MI->>Log: log "tick_complete" (segments_published)
    end

    Note over LS,MI: Flink (Phase 4) is not consuming yet.<br/>Verified manually via aws kinesis get-records (AC-02/03/04),<br/>same role Phase 1's pg_recvlogical / Phase 2's kcat played for their transports.
```

## What this diagram does *not* include

- Phase 4 (Flink consuming `market-price-events`, joining against `payment-events.v1`, computing `price_decision.v1`).
- Phase 2's already-implemented Kafka/Debezium pipeline (shown only for orientation, dashed).
- The still-open Phase 4 dependency flagged in the spec's §8 (Follow-ups): nothing yet maps an `apartment_id` to a market segment — this diagram only covers producing the market side, not resolving that join.

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

## 3. Collaboration diagram — one tick's message exchange

Mermaid has no native UML collaboration/communication diagram type, so this is a flowchart used to
approximate one: a hub object (`main.py`) with numbered links to its collaborators, emphasizing *who talks
to whom* over strict time order (that's diagram 2's job).

```mermaid
flowchart TB
    MI["market-ingestor<br/>(main.py)"]
    SS["MarketPrice<br/>(shared_schemas)"]
    KS[("Kinesis<br/>market-price-events")]
    LG["structlog<br/>(libs/common)"]

    MI -->|"1: build_market_price_event()"| SS
    SS -->|"2: validated instance"| MI
    MI -->|"3: put_records(Records=[18])"| KS
    KS -->|"4: response (FailedRecordCount, per-record status)"| MI
    MI -->|"5: [if failures] put_records(failed only)"| KS
    KS -->|"6: response"| MI
    MI -->|"7: log(tick_complete)"| LG

    classDef hub fill:#1f6feb,color:#fff,stroke:#0b3d91;
    class MI hub;
```

## 4. How Kinesis's partition log actually works

Two mechanics this project's `PartitionKey` choice (ADR-0005) depends on: how a key maps to a shard, and
what a shard *is*.

**4.1 — `PartitionKey` → shard, via a 128-bit hash space**

```mermaid
flowchart LR
    pk["PartitionKey (string)<br/>e.g. 'Barcelona|Eixample|apartment|2'"] -->|"MD5 → 128-bit integer"| hash["hash(PartitionKey)"]
    hash -->|"falls inside this shard's range"| shard1

    subgraph ranges["Hash key space 0 .. 2^128-1 — one contiguous range per shard"]
        direction LR
        shard0["shard-0000<br/>range A"]
        shard1["shard-0001<br/>range B"]
        shard2["shard-0002<br/>range C"]
        shard3["shard-0003<br/>range D"]
    end
```

Kinesis owns the hashing — the producer never computes it (spec §4, Failure handling / partition key
notes). The same string always hashes to the same 128-bit integer, which always falls in the same shard's
range — that's the entire mechanism behind ADR-0005's partition affinity (AC-04).

**4.2 — a shard is an ordered, append-only log — not a queue, not a table**

```mermaid
flowchart LR
    subgraph shard["shard-0000 — ordered, append-only, 24h retention"]
        direction LR
        r1["seq 4961...001<br/>pk: BCN·Eixample·apt·2"]
        r2["seq 4961...002<br/>pk: MAD·Centro·studio·0"]
        r3["seq 4961...003<br/>pk: BCN·Eixample·apt·2"]
        r4["seq 4961...004<br/>..."]
        r1 --> r2 --> r3 --> r4
    end
    trim["TRIM_HORIZON<br/>(oldest still-retained record)"] -.-> r1
    latest["LATEST<br/>(next record to arrive)"] -.-> r4
```

Sequence numbers are monotonically increasing **within a shard only** — there is no global ordering across
shards. Records from *different* partition keys freely interleave in the same shard if they hash to the
same range (exactly what happened here: 18 segments, 4 shards, several segments per shard by construction).
`GetShardIterator` (`ShardIteratorType`: `TRIM_HORIZON` / `LATEST` / `AT_SEQUENCE_NUMBER` /
`AT_TIMESTAMP`) returns an opaque cursor into one shard's log; `GetRecords` reads forward from that cursor
and returns a `NextShardIterator` to keep polling — this is exactly what the verification script in PR #5's
"How to test" section does, once per shard.

## 5. Timeline — log growth per shard, tick by tick

Reconstructed exactly from the live verification numbers (30/30/10/20 final split, §"What was verified" in
`docs/AUDIT_DIARY.md`) divided by the 5 ticks observed — affinity was 100% stable (AC-04, zero violations),
so the same 6/6/2/4 segments land on the same shards *every single tick*, not just on average.

```mermaid
timeline
    title Records per shard, accumulated across ticks (AC-04: zero affinity violations)
    Tick 1 (t=0s)   : shard-0000 +6 (total 6)  : shard-0001 +6 (total 6)  : shard-0002 +2 (total 2)  : shard-0003 +4 (total 4)
    Tick 2 (t=60s)  : shard-0000 +6 (total 12) : shard-0001 +6 (total 12) : shard-0002 +2 (total 4)  : shard-0003 +4 (total 8)
    Tick 3 (t=120s) : shard-0000 +6 (total 18) : shard-0001 +6 (total 18) : shard-0002 +2 (total 6)  : shard-0003 +4 (total 12)
    Tick 4 (t=180s) : shard-0000 +6 (total 24) : shard-0001 +6 (total 24) : shard-0002 +2 (total 8)  : shard-0003 +4 (total 16)
    Tick 5 (t=240s) : shard-0000 +6 (total 30) : shard-0001 +6 (total 30) : shard-0002 +2 (total 10) : shard-0003 +4 (total 20)
```

The skew isn't random noise that might average out — it's a deterministic consequence of which 18 fixed
strings hash into which of the 4 ranges, so it repeats identically forever unless the key scheme or shard
count changes (ADR-0005's accepted trade-off).

## What this diagram does *not* include

- Phase 4 (Flink consuming `market-price-events`, joining against `payment-events.v1`, computing `price_decision.v1`).
- Phase 2's already-implemented Kafka/Debezium pipeline (shown only for orientation, dashed).
- The still-open Phase 4 dependency flagged in the spec's §8 (Follow-ups): nothing yet maps an `apartment_id` to a market segment — this diagram only covers producing the market side, not resolving that join.

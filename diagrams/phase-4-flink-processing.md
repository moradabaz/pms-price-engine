# Phase 4 — Flink Processing

Diagram for [`specs/phases/04-flink-processing/spec.md`](../specs/phases/04-flink-processing/spec.md) — implemented (`streaming/flink-jobs/`), not yet verified live against a running cluster (branch `phase-4-flink-processing`). Focus here is **how the Flink elements themselves talk to each other** inside `streaming/flink-jobs/` — the two-stage pipeline, its state, and the sink — not the surrounding infra (Kafka/Kinesis/DynamoDB are shown only as the boundary this job talks across).

## 1. Component diagram — state and control flow inside the job

```mermaid
flowchart TB
    subgraph sources["Sources"]
        kafka1[("Kafka<br/>payment-events.v1<br/>keyed by apartment_id")]
        kafka2[("Kafka<br/>apartment-market-segments.v1<br/>NEW — this phase's own Debezium wiring, not done yet")]
        kinesis[("Kinesis<br/>market-price-events<br/>carries full segment identity natively")]
    end

    subgraph stageA["Stage A — stage_cost_enrichment.py<br/>KeyedBroadcastProcessFunction, keyed by apartment_id"]
        costfn["CostEnrichmentFunction"]
        costmap[("MapState&lt;event_id, PaymentLine&gt;<br/>per apartment — upsert by event_id, ADR-0003")]
        bcstate[("BroadcastState<br/>apartment_id → segment + target_margin/discount")]
        costfn <--> costmap
        costfn <--> bcstate
    end

    kafka1 -->|"process_element()"| costfn
    kafka2 -->|"process_broadcast_element()"| costfn
    costfn -->|"yield CostAggregate"| rekey["key_by(segment_key)"]

    subgraph stageB["Stage B — stage_price_decision.py<br/>KeyedCoProcessFunction, keyed by segment"]
        pricefn["PriceDecisionFunction"]
        aps[("MapState apartments_in_segment<br/>Hoja 1")]
        nights[("MapState nights_in_segment<br/>Hoja 2")]
        deadlines[("MapState × 2<br/>apartment_deadlines / night_deadlines")]
        pricefn <--> aps
        pricefn <--> nights
        pricefn <--> deadlines
        pricefn -.register/fire.-> timers["TimerService<br/>(dead-man's-switch, E.1)"]
    end

    rekey -->|"process_element1()"| pricefn
    kinesis -->|"key_by(city, neighborhood,<br/>type, bedrooms)<br/>process_element2()"| pricefn

    pricefn -->|"yield PriceDecision<br/>(main output)"| dynamap["DynamoDbSinkFunction<br/>(MapFunction — no native<br/>Python Sink API, see checklist #9)"]
    timers -.->|"yield DATA_STALE_TAG, alert<br/>(side output)"| stale["side-output stream<br/>data-stale"]

    dynamap -->|"put_item, idempotent<br/>by decision_id"| dynamodb[("DynamoDB<br/>price_decision")]
    dynamap -->|"pass-through"| discard["DiscardingSink (Java)<br/>no-op terminal operator"]

    classDef built fill:#1f6feb,color:#fff,stroke:#0b3d91;
    classDef todo fill:transparent,stroke:#888,stroke-dasharray: 4 3,color:#888;
    class stageA,stageB,costfn,pricefn,rekey,dynamap,dynamodb built;
    class kafka2,stale todo;
```

**Legend:** solid blue = built and unit-tested (28 tests, no Flink runtime needed — `streaming/flink-jobs/tests/`). Dashed gray = depends on infrastructure this phase hasn't wired yet (`apartment-market-segments.v1` needs `infra/debezium/postgres-connector.json` updated, spec §4) or is a monitoring path (`data-stale`) with no consumer built yet.

## 2. Sequence diagram — a cost update and a market update, each triggering fan-out

Same numbers as the spec's worked example (§8): segment Barcelona/Eixample/studio, apartments `apt-A`/`apt-B`, nights `2026-08-10`/`2026-08-20`.

```mermaid
sequenceDiagram
    participant K1 as Kafka (payment-events.v1)
    participant SA as CostEnrichmentFunction
    participant BC as BroadcastState
    participant SB as PriceDecisionFunction
    participant KI as Kinesis (market-price-events)
    participant DB as DynamoDbSinkFunction

    Note over K1,DB: Cost update for apt-A arrives
    K1->>SA: PaymentLine(apt-A, amount_gross=...)
    SA->>SA: upsert MapState<event_id,PaymentLine> by event_id
    SA->>SA: aggregate_cost() → daily_cost_eur (§6)
    SA->>BC: get(apt-A)
    BC-->>SA: SegmentAssignment(segment, margin, discount)
    SA-->>SB: yield CostAggregate(apt-A, segment_key, ...)

    Note over SB: re-keyed by segment
    SB->>SB: staleness guard (§5) — discard if older than stored
    SB->>SB: apartments_in_segment.put(apt-A, ...)
    SB->>SB: register 48h timer, apartment_deadlines.put (E.1)
    loop for each known night in nights_in_segment
        SB->>SB: decide_price(cost, snapshot) — ADR-0007
        SB-->>DB: yield PriceDecision(apt-A, night)
    end
    DB->>DB: put_item (idempotent by decision_id)

    Note over KI,DB: Market update for one night arrives
    KI->>SB: MarketPrice(target_date=2026-08-10, avg_nightly_rate)
    SB->>SB: drop if target_date < today (Decision D guard)
    SB->>SB: evict expired nights_in_segment entries (§6)
    SB->>SB: staleness guard, then nights_in_segment.put(...)
    SB->>SB: register 48h timer, night_deadlines.put (E.1)
    loop for each known apartment in apartments_in_segment
        SB->>SB: decide_price(cost, snapshot)
        SB-->>DB: yield PriceDecision(apartment, 2026-08-10)
    end
    DB->>DB: put_item (idempotent by decision_id)

    Note over SB: 48h later, if nothing touched a key again
    SB->>SB: on_timer(fired_at) — expired_keys() scan (§7)
    SB-->>SB: yield DATA_STALE_TAG, (kind, key) — side output, not a PriceDecision
```

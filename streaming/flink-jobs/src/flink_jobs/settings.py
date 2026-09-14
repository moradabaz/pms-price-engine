from pydantic_settings import BaseSettings, SettingsConfigDict


class FlinkJobSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLINK_JOB_")

    kafka_bootstrap_servers: str
    payment_events_topic: str = "payment-events.v1"
    apartment_segments_topic: str = "apartment-market-segments.v1"
    # Phase 11 (ADR-0011 backlog #5).
    owner_contracts_topic: str = "owner-contracts.v1"
    # Phase 14 (ADR-0011 backlog #9).
    manual_overrides_topic: str = "manual-overrides.v1"
    # Phase 18 (ADR-0011 backlog #13 prerequisite).
    booking_events_topic: str = "booking-events.v1"
    # Phase 19 (ADR-0011 backlog #13, ADR-0012).
    cost_definitions_topic: str = "cost-definitions.v1"
    cost_allocation_rules_topic: str = "cost-allocation-rules.v1"
    company_cost_occurrences_topic: str = "company-cost-occurrences.v1"
    # Phase 25 (ADR-0018 §2).
    pricing_strategies_topic: str = "pricing-strategies.v1"
    kafka_consumer_group_id: str = "flink-price-engine"

    # Read via Kafka, not Kinesis directly — no Kinesis connector runs on
    # Flink 2.x (error-handling/flink-2x-removes-legacy-sourcefunction-...md).
    # kinesis-kafka-bridge republishes market-price-events onto this topic.
    market_price_topic: str = "market-price-bridge.v1"
    aws_region: str = "eu-west-1"

    dynamodb_table_name: str = "price_decision"
    dynamodb_endpoint_url: str | None = None

    checkpoint_interval_ms: int = 60_000
    checkpoint_storage_path: str
    s3_endpoint_url: str | None = None
    max_parallelism: int = 128
    parallelism: int = 4

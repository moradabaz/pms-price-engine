import json
from datetime import date, datetime

from pyflink.common import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import SinkFunction
from shared_schemas.market_price import MarketPrice
from shared_schemas.payment_line import PaymentLine

from flink_jobs.dynamodb_sink import DynamoDbSinkFunction
from flink_jobs.models import ApartmentSegmentRow, ManualOverrideRow, OwnerContractRow
from flink_jobs.settings import FlinkJobSettings
from flink_jobs.stage_cost_enrichment import (
    SEGMENT_BROADCAST_DESCRIPTOR,
    CostEnrichmentFunction,
)
from flink_jobs.stage_manual_override_enrichment import (
    MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR,
    ManualOverrideEnrichmentFunction,
)
from flink_jobs.stage_owner_contract_enrichment import (
    OWNER_CONTRACT_BROADCAST_DESCRIPTOR,
    OwnerContractEnrichmentFunction,
)
from flink_jobs.stage_price_decision import DATA_STALE_TAG, PriceDecisionFunction


def _configure_checkpointing(env, settings: FlinkJobSettings) -> None:
    """Enables EXACTLY_ONCE checkpointing on RocksDB + S3."""
    env.enable_checkpointing(settings.checkpoint_interval_ms)
    config = Configuration()
    config.set_string("state.backend.type", "rocksdb")
    config.set_string("state.backend.incremental", "true")
    config.set_string("state.checkpoints.dir", settings.checkpoint_storage_path)
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    if settings.s3_endpoint_url:
        config.set_string("s3.endpoint", settings.s3_endpoint_url)
        config.set_string("s3.path.style.access", "true")
        config.set_string("s3.access-key", "test")
        config.set_string("s3.secret-key", "test")
    env.configure(config)


# Phase 8 (ADR-0011 backlog #6): defensive-default pattern for the four
# Bonus/Malus columns added after this topic already had history.
_DEFAULT_QUALITY_TIER = "standard"
_DEFAULT_RATING = 4.0
_DEFAULT_HAS_VIEW = False
_DEFAULT_HAS_PARKING = False

# Phase 11 (ADR-0011 backlog #5): same defaults owner_contracts' own columns
# use — a CDC message predating this phase's column additions can't occur
# for a topic this phase itself introduces, but kept for symmetry with every
# other parser here and as a safety net against a partially-seeded row.
_DEFAULT_COMMISSION_BASE = "total_revenue"
_DEFAULT_COMMISSION_PCT = 0.15


def _parse_apartment_segment_row(raw: str) -> ApartmentSegmentRow:
    """Parses one apartment_market_segments CDC message. Returns a row."""
    data = json.loads(raw)
    return ApartmentSegmentRow(
        apartment_id=data["apartment_id"],
        city=data["city"],
        neighborhood=data["neighborhood"],
        property_type=data["property_type"],
        bedrooms=data["bedrooms"],
        target_margin=float(data["target_margin"]),
        competitiveness_discount=float(data["competitiveness_discount"]),
        quality_tier=data.get("quality_tier", _DEFAULT_QUALITY_TIER),
        rating=float(data.get("rating", _DEFAULT_RATING)),
        has_view=bool(data.get("has_view", _DEFAULT_HAS_VIEW)),
        has_parking=bool(data.get("has_parking", _DEFAULT_HAS_PARKING)),
    )


def _parse_owner_contract_row(raw: str) -> OwnerContractRow:
    """Parses one owner_contracts CDC message (Phase 11, ADR-0011 backlog
    #5). Returns a row."""
    data = json.loads(raw)
    return OwnerContractRow(
        apartment_id=data["apartment_id"],
        owner_id=data["owner_id"],
        commission_base=data.get("commission_base", _DEFAULT_COMMISSION_BASE),
        commission_pct=float(data.get("commission_pct", _DEFAULT_COMMISSION_PCT)),
    )


def _parse_manual_override_row(raw: str) -> ManualOverrideRow:
    """Parses one manual_overrides CDC message (Phase 14, ADR-0011 backlog
    #9). No defensive defaults needed — this topic has no history predating
    this phase, unlike apartment-segments/owner-contracts."""
    data = json.loads(raw)
    return ManualOverrideRow(
        apartment_id=data["apartment_id"],
        target_date=date.fromisoformat(data["target_date"]),
        override_price_eur=float(data["override_price_eur"]),
        reason=data["reason"],
        authorized_by=data["authorized_by"],
        valid_until=datetime.fromisoformat(data["valid_until"]),
    )


def build_job(env, settings: FlinkJobSettings) -> None:
    """Wires sources, Stage A/B, and the DynamoDB sink onto env."""
    env.set_max_parallelism(settings.max_parallelism)
    env.set_parallelism(settings.parallelism)
    _configure_checkpointing(env, settings)

    payment_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.payment_events_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        # committed_offsets() alone has no fallback (default NONE) and
        # throws NoOffsetForPartitionException the first time this consumer
        # group ever reads this topic — EARLIEST here mirrors Debezium's own
        # snapshot.mode:initial precedent (full history on first run).
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    segment_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.apartment_segments_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    payment_stream = (
        env.from_source(
            payment_source, WatermarkStrategy.no_watermarks(), "payment-events"
        )
        .map(PaymentLine.model_validate_json)
        .key_by(lambda line: line.apartment_id)
    )
    segment_stream = env.from_source(
        segment_source, WatermarkStrategy.no_watermarks(), "apartment-segments"
    ).map(_parse_apartment_segment_row)
    broadcast_segment_stream = segment_stream.broadcast(SEGMENT_BROADCAST_DESCRIPTOR)

    cost_aggregates = payment_stream.connect(broadcast_segment_stream).process(
        CostEnrichmentFunction()
    )

    # Phase 11 (ADR-0011 backlog #5, spec 11 §C): Stage A2, chained after
    # Stage A rather than a second broadcast input on CostEnrichmentFunction
    # — PyFlink's KeyedStream.connect() accepts exactly one broadcast stream
    # per process() call.
    owner_contract_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.owner_contracts_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    owner_contract_stream = env.from_source(
        owner_contract_source, WatermarkStrategy.no_watermarks(), "owner-contracts"
    ).map(_parse_owner_contract_row)
    broadcast_owner_contract_stream = owner_contract_stream.broadcast(
        OWNER_CONTRACT_BROADCAST_DESCRIPTOR
    )
    cost_aggregates_with_commission = (
        cost_aggregates.key_by(lambda ca: ca.apartment_id)
        .connect(broadcast_owner_contract_stream)
        .process(OwnerContractEnrichmentFunction())
    )

    market_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.market_price_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    market_stream = (
        env.from_source(
            market_source, WatermarkStrategy.no_watermarks(), "market-price-bridge"
        )
        .map(MarketPrice.model_validate_json)
        .key_by(
            lambda mp: (
                mp.market_area.city,
                mp.market_area.neighborhood,
                mp.property_profile.type,
                mp.property_profile.bedrooms,
            )
        )
    )

    keyed_cost_aggregates = cost_aggregates_with_commission.key_by(
        lambda ca: ca.segment_key
    )
    price_decisions = keyed_cost_aggregates.connect(market_stream).process(
        PriceDecisionFunction()
    )

    # E.1's dead-man's-switch — logged, not a price_decision (spec §7).
    # Retrieved from Stage B's own output, BEFORE Stage C below — unaffected
    # by the override enrichment chained after it (spec 14 §2).
    price_decisions.get_side_output(DATA_STALE_TAG).print()

    # Phase 14 (ADR-0011 backlog #9, spec 14 §2): Stage C, the first stage in
    # this project chained AFTER a price_decision is computed rather than
    # before it — republishes an authorized manual override in place of the
    # algorithmic suggestion when one is active for this (apartment_id,
    # target_date).
    manual_override_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.manual_overrides_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    manual_override_stream = env.from_source(
        manual_override_source, WatermarkStrategy.no_watermarks(), "manual-overrides"
    ).map(_parse_manual_override_row)
    broadcast_manual_override_stream = manual_override_stream.broadcast(
        MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR
    )
    final_decisions = (
        price_decisions.key_by(lambda pd: pd.apartment_id)
        .connect(broadcast_manual_override_stream)
        .process(ManualOverrideEnrichmentFunction())
    )

    dynamodb_writer = DynamoDbSinkFunction(
        table_name=settings.dynamodb_table_name,
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
    )
    final_decisions.map(dynamodb_writer).add_sink(
        # Flink 2.x moved this class under .legacy. (confirmed by scanning
        # flink-dist-2.3.0.jar — it wasn't deleted like RichParallelSourceFunction).
        SinkFunction(
            "org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink"
        )
    )

import json

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
from flink_jobs.models import ApartmentSegmentRow
from flink_jobs.settings import FlinkJobSettings
from flink_jobs.stage_cost_enrichment import (
    SEGMENT_BROADCAST_DESCRIPTOR,
    CostEnrichmentFunction,
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


# ADR-0009: same default as the DB column — this source replays from the
# earliest offset (job.py below), so CDC messages predating commission_pct's
# addition to apartment_market_segments don't carry the field at all.
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
        commission_pct=float(data.get("commission_pct", _DEFAULT_COMMISSION_PCT)),
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

    keyed_cost_aggregates = cost_aggregates.key_by(lambda ca: ca.segment_key)
    price_decisions = keyed_cost_aggregates.connect(market_stream).process(
        PriceDecisionFunction()
    )

    # E.1's dead-man's-switch — logged, not a price_decision (spec §7).
    price_decisions.get_side_output(DATA_STALE_TAG).print()

    dynamodb_writer = DynamoDbSinkFunction(
        table_name=settings.dynamodb_table_name,
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
    )
    price_decisions.map(dynamodb_writer).add_sink(
        # Flink 2.x moved this class under .legacy. (confirmed by scanning
        # flink-dist-2.3.0.jar — it wasn't deleted like RichParallelSourceFunction).
        SinkFunction(
            "org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink"
        )
    )

import json

from pyflink.common import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
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
from flink_jobs.stage_price_decision import PriceDecisionFunction


def _configure_checkpointing(env, settings: FlinkJobSettings) -> None:
    """Enables EXACTLY_ONCE checkpointing on RocksDB + S3."""
    env.enable_checkpointing(settings.checkpoint_interval_ms)
    config = Configuration()
    config.set_string("state.backend.type", "rocksdb")
    config.set_string("state.backend.incremental", "true")
    config.set_string("state.checkpoints.dir", settings.checkpoint_storage_path)
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    env.configure(config)


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
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
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

    kinesis_props = {"aws.region": settings.aws_region}
    if settings.kinesis_endpoint_url:
        kinesis_props["aws.endpoint"] = settings.kinesis_endpoint_url
    market_source = FlinkKinesisConsumer(
        settings.kinesis_stream_name, SimpleStringSchema(), kinesis_props
    )
    market_stream = (
        env.add_source(market_source)
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

    dynamodb_writer = DynamoDbSinkFunction(
        table_name=settings.dynamodb_table_name,
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
    )
    price_decisions.map(dynamodb_writer).add_sink(
        SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
    )

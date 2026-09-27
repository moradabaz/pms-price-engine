import logging

from flink_shared import configure_checkpointing
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import SinkFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from shared_schemas.booking import Booking
from shared_schemas.market_price import MarketPrice

from market_pulse_job.market_pulse import (
    ACC_TYPE,
    MarketPulseAggregateFunction,
    MarketPulseSinkFunction,
    MarketPulseWindowFunction,
    is_blended_snapshot,
    segment_key,
)
from market_pulse_job.settings import MarketPulseJobSettings

# Plain stdlib logging, not common.get_logger() (structlog): this module's
# lambdas run distributed inside PyFlink's Python worker sandbox, where
# structlog's PrintLoggerFactory breaks (`TypeError: CustomPrint.print() got
# an unexpected keyword argument 'flush'`) — the same reason
# streaming/flink-jobs/ never uses common.get_logger() inside any operator,
# only plain `logging.getLogger(__name__)` (see stage_price_decision.py).
logger = logging.getLogger(__name__)


def build_job(env, settings: MarketPulseJobSettings) -> None:
    """Wires the two Kafka sources with their per-source watermark
    strategies. Bloque 0 — no windows, no sinks yet, just a trivial log per
    parsed event to confirm both streams deserialize correctly."""
    env.set_parallelism(settings.parallelism)
    env.set_max_parallelism(settings.max_parallelism)
    configure_checkpointing(
        env,
        settings.checkpoint_interval_ms,
        settings.checkpoint_storage_path,
        settings.s3_endpoint_url,
    )

    market_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.market_price_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    booking_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.booking_events_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    market_watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(settings.watermark_out_of_orderness_seconds)
    ).with_idleness(Duration.of_seconds(settings.market_pulse_idleness_seconds))

    booking_watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(settings.watermark_out_of_orderness_seconds)
    ).with_idleness(Duration.of_seconds(settings.booking_idleness_seconds))

    market_stream = env.from_source(
        market_source, market_watermark_strategy, "market-price-bridge"
    ).map(MarketPrice.model_validate_json)
    # booking-events.v1 has only 1 real Kafka partition (docs/manual/MANUAL.md)
    # — parallelism forced to 1 on this operator specifically (spec §3.1),
    # otherwise 3 of the job's 4 default subtasks never get a partition
    # assigned and never advance their watermark.
    booking_stream = (
        env.from_source(booking_source, booking_watermark_strategy, "booking-events")
        .set_parallelism(1)
        .map(Booking.model_validate_json)
        .set_parallelism(1)
    )

    market_pulse_writer = MarketPulseSinkFunction(
        table_name=settings.market_pulse_table,
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
    )
    (
        market_stream.filter(is_blended_snapshot)
        .key_by(segment_key)
        .window(TumblingEventTimeWindows.of(Time.minutes(settings.market_pulse_window_minutes)))
        .aggregate(
            MarketPulseAggregateFunction(),
            MarketPulseWindowFunction(),
            accumulator_type=ACC_TYPE,
        )
        .map(market_pulse_writer)
        .add_sink(
            SinkFunction(
                "org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink"
            )
        )
    )

    booking_stream.map(
        lambda event: logger.info(
            "booking_event_received booking_id=%s", event.booking_id
        )
    ).set_parallelism(1)

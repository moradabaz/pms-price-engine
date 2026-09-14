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
from flink_jobs.models import (
    ApartmentSegmentRow,
    BookingRow,
    CompanyCostOccurrenceRow,
    CostAllocationRuleRow,
    CostDefinitionRow,
    ManualOverrideRow,
    OwnerContractRow,
    PricingStrategyRow,
)
from flink_jobs.settings import FlinkJobSettings
from flink_jobs.stage_allocation_correction import AllocationCorrectionFunction
from flink_jobs.stage_booking_enrichment import BookingEnrichmentFunction
from flink_jobs.stage_company_cost_enrichment import (
    COMPANY_COST_OCCURRENCE_BROADCAST_DESCRIPTOR as A4_OCCURRENCE_DESCRIPTOR,
)
from flink_jobs.stage_company_cost_enrichment import (
    COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR as A4_RULE_DESCRIPTOR,
)
from flink_jobs.stage_company_cost_enrichment import (
    COST_DEFINITION_BROADCAST_DESCRIPTOR as A4_DEFINITION_DESCRIPTOR,
)
from flink_jobs.stage_company_cost_enrichment import CompanyCostEnrichmentFunction
from flink_jobs.stage_cost_definition_resolution import (
    COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR as A0_RULE_DESCRIPTOR,
)
from flink_jobs.stage_cost_definition_resolution import (
    COST_DEFINITION_BROADCAST_DESCRIPTOR as A0_DEFINITION_DESCRIPTOR,
)
from flink_jobs.stage_cost_definition_resolution import CostDefinitionResolutionFunction
from flink_jobs.stage_cost_enrichment import (
    PRICING_STRATEGY_BROADCAST_DESCRIPTOR,
    SEGMENT_BROADCAST_DESCRIPTOR,
    CostEnrichmentFunction,
)
from flink_jobs.stage_manual_override_enrichment import (
    MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR,
    ManualOverrideEnrichmentFunction,
)
from flink_jobs.stage_owner_contract_enrichment import (
    OWNER_COMMISSION_COST_DEFINITION_BROADCAST_DESCRIPTOR,
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


def _parse_apartment_segment_row(raw: str) -> ApartmentSegmentRow:
    """Parses one apartment_market_segments CDC message. Returns a row.
    Phase 25 (ADR-0018 §1): target_margin/competitiveness_discount removed —
    a real substitution (see _parse_pricing_strategy_row below), not a
    redundant copy."""
    data = json.loads(raw)
    return ApartmentSegmentRow(
        apartment_id=data["apartment_id"],
        city=data["city"],
        neighborhood=data["neighborhood"],
        property_type=data["property_type"],
        bedrooms=data["bedrooms"],
        quality_tier=data.get("quality_tier", _DEFAULT_QUALITY_TIER),
        rating=float(data.get("rating", _DEFAULT_RATING)),
        has_view=bool(data.get("has_view", _DEFAULT_HAS_VIEW)),
        has_parking=bool(data.get("has_parking", _DEFAULT_HAS_PARKING)),
    )


def _parse_pricing_strategy_row(raw: str) -> PricingStrategyRow:
    """Parses one pricing_strategies CDC message (Phase 25, ADR-0018 §2) —
    insert-only, so unlike apartment_market_segments this topic has no
    pre-this-phase history to default against. Returns a row."""
    data = json.loads(raw)
    return PricingStrategyRow(
        apartment_id=data["apartment_id"],
        version=int(data["version"]),
        target_margin=float(data["target_margin"]),
        competitiveness_discount=float(data["competitiveness_discount"]),
        floor_policy_default=data.get("floor_policy_default", "soft"),
    )


def _parse_owner_contract_row(raw: str) -> OwnerContractRow:
    """Parses one owner_contracts CDC message (Phase 11, ADR-0011 backlog
    #5; rewired by Phase 20, ADR-0013 §4 — commission_base/commission_pct
    are gone, cost_definition_id replaces them). Returns a row."""
    data = json.loads(raw)
    return OwnerContractRow(
        apartment_id=data["apartment_id"],
        owner_id=data["owner_id"],
        cost_definition_id=data["cost_definition_id"],
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


def _parse_booking_row(raw: str) -> BookingRow:
    """Parses one bookings CDC message (Phase 18, ADR-0011 backlog #13
    prerequisite). No defensive defaults needed — this topic has no history
    predating this phase, same reasoning _parse_manual_override_row uses."""
    data = json.loads(raw)
    return BookingRow(
        booking_id=data["booking_id"],
        apartment_id=data["apartment_id"],
        check_in=date.fromisoformat(data["check_in"]),
        check_out=date.fromisoformat(data["check_out"]),
        channel=data["channel"],
        guests=int(data["guests"]),
        revenue_eur=float(data["revenue_eur"]),
        status=data["status"],
    )


def _parse_cost_definition_row(raw: str) -> CostDefinitionRow:
    """Parses one cost_definitions CDC message (Phase 19, ADR-0011 backlog
    #13). No defensive defaults — this topic has no history predating this
    phase."""
    data = json.loads(raw)
    return CostDefinitionRow(
        cost_definition_id=data["cost_definition_id"],
        concept=data["concept"],
        scope=data["scope"],
        behavior=data["behavior"],
        trigger=data["trigger"],
        calculation_base=data["calculation_base"],
        recurrence=data["recurrence"],
        revenue_base=data.get("revenue_base"),
        rate=float(data["rate"]) if data.get("rate") is not None else None,
    )


def _parse_cost_allocation_rule_row(raw: str) -> CostAllocationRuleRow:
    """Parses one cost_allocation_rules CDC message (Phase 19)."""
    data = json.loads(raw)
    return CostAllocationRuleRow(
        cost_definition_id=data["cost_definition_id"],
        method=data["method"],
        weight_config=data.get("weight_config"),
    )


def _parse_company_cost_occurrence_row(raw: str) -> CompanyCostOccurrenceRow:
    """Parses one company_cost_occurrences CDC message (Phase 19)."""
    data = json.loads(raw)
    return CompanyCostOccurrenceRow(
        company_cost_occurrence_id=data["company_cost_occurrence_id"],
        cost_definition_id=data["cost_definition_id"],
        billing_period_start=date.fromisoformat(data["billing_period_start"]),
        billing_period_end=date.fromisoformat(data["billing_period_end"]),
        amount_gross=float(data["amount_gross"]),
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
    # Phase 25 (ADR-0018 §2).
    pricing_strategy_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.pricing_strategies_topic)
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
    # Phase 25 (ADR-0018 §2): a genuinely independent CDC source from
    # segment_stream — unioned before broadcasting under two descriptors,
    # the same "one connected broadcast stream, several independently-keyed
    # descriptors" pattern stage_owner_contract_enrichment.py's own
    # owner_contract_stream/cost_definition_stream union already
    # established, since PyFlink's KeyedStream.connect() accepts exactly
    # one broadcast stream per process() call.
    pricing_strategy_stream = env.from_source(
        pricing_strategy_source,
        WatermarkStrategy.no_watermarks(),
        "pricing-strategies",
    ).map(_parse_pricing_strategy_row)
    broadcast_segment_stream = segment_stream.union(pricing_strategy_stream).broadcast(
        SEGMENT_BROADCAST_DESCRIPTOR, PRICING_STRATEGY_BROADCAST_DESCRIPTOR
    )

    # Phase 19 (ADR-0011 backlog #13, ADR-0012, spec 19 §4): Stage A0,
    # chained before Stage A. cost_definition_stream/cost_allocation_rule_
    # stream are each read from Kafka exactly once here and reused below for
    # Stage A4's own broadcast — a DataStream can fan out to multiple
    # downstream operators without re-subscribing to the topic (re-reading
    # the same topic under the same consumer group a second time would
    # instead split its single partition between two competing consumers).
    cost_definition_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.cost_definitions_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    cost_allocation_rule_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.cost_allocation_rules_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    cost_definition_stream = env.from_source(
        cost_definition_source, WatermarkStrategy.no_watermarks(), "cost-definitions"
    ).map(_parse_cost_definition_row)
    cost_allocation_rule_stream = env.from_source(
        cost_allocation_rule_source,
        WatermarkStrategy.no_watermarks(),
        "cost-allocation-rules",
    ).map(_parse_cost_allocation_rule_row)

    broadcast_cost_config_stream_a0 = cost_definition_stream.union(
        cost_allocation_rule_stream
    ).broadcast(A0_DEFINITION_DESCRIPTOR, A0_RULE_DESCRIPTOR)

    enriched_payment_stream = payment_stream.connect(
        broadcast_cost_config_stream_a0
    ).process(CostDefinitionResolutionFunction())

    cost_aggregates = (
        enriched_payment_stream.key_by(lambda line: line.apartment_id)
        .connect(broadcast_segment_stream)
        .process(CostEnrichmentFunction())
    )

    # Phase 18 (ADR-0011 backlog #13 prerequisite, spec 18 §4): a regular
    # two-keyed-stream join (not a broadcast, unlike Stage A2 below) —
    # bookings vary per apartment, not shared config every apartment reads
    # alike. Chained right after Stage A, before Stage A2.
    booking_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.booking_events_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    booking_stream = (
        env.from_source(booking_source, WatermarkStrategy.no_watermarks(), "bookings")
        .map(_parse_booking_row)
        .key_by(lambda row: row.apartment_id)
    )
    cost_aggregates_with_occupancy = (
        cost_aggregates.key_by(lambda ca: ca.apartment_id)
        .connect(booking_stream)
        .process(BookingEnrichmentFunction())
    )

    # Phase 19 (ADR-0011 backlog #13, spec 19 §4): Stage A-correction —
    # resolves the occupied_night/booking allocation methods Stage A
    # couldn't compute yet (needed Phase 18's occupied_nights/booking_count,
    # only available from here on). Plain MapFunction, no state, no
    # broadcast.
    cost_aggregates_corrected = cost_aggregates_with_occupancy.map(
        AllocationCorrectionFunction()
    )

    # Phase 19: Stage A4, the corrected company-cost fan-out (spec 19 §5) —
    # reuses cost_definition_stream/cost_allocation_rule_stream from Stage
    # A0 above (same DataStream objects, fanned out again, not re-read from
    # Kafka) plus company_cost_occurrence_stream, its own 3-way broadcast.
    company_cost_occurrence_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.company_cost_occurrences_topic)
        .set_group_id(settings.kafka_consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    company_cost_occurrence_stream = env.from_source(
        company_cost_occurrence_source,
        WatermarkStrategy.no_watermarks(),
        "company-cost-occurrences",
    ).map(_parse_company_cost_occurrence_row)

    broadcast_cost_config_stream_a4 = cost_definition_stream.union(
        cost_allocation_rule_stream, company_cost_occurrence_stream
    ).broadcast(
        A4_DEFINITION_DESCRIPTOR,
        A4_RULE_DESCRIPTOR,
        A4_OCCURRENCE_DESCRIPTOR,
    )

    cost_aggregates_with_company_costs = (
        cost_aggregates_corrected.key_by(lambda ca: ca.apartment_id)
        .connect(broadcast_cost_config_stream_a4)
        .process(CompanyCostEnrichmentFunction())
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
    # Phase 20 (ADR-0013 §4): reuses cost_definition_stream (already parsed
    # for Stage A0/A4 above) so this stage can resolve owner_contracts.
    # cost_definition_id into its rate/revenue_base — same "one source
    # stream, several independently-connected broadcasts, each under its own
    # descriptor" pattern already used for cost_definitions/cost_allocation_
    # rules. Not cost_allocation_rule_stream too — the commission
    # CostDefinition's own allocation rule (always 'direct') is never read
    # here.
    broadcast_owner_contract_stream = owner_contract_stream.union(
        cost_definition_stream
    ).broadcast(
        OWNER_CONTRACT_BROADCAST_DESCRIPTOR,
        OWNER_COMMISSION_COST_DEFINITION_BROADCAST_DESCRIPTOR,
    )
    cost_aggregates_with_commission = (
        cost_aggregates_with_company_costs.key_by(lambda ca: ca.apartment_id)
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

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

# Mirrors specs/events/price_decision.v1.json field-for-field (spec 05 §4),
# plus dynamodb_event_name/ingested_at appended by this consumer. Field IDs
# are assigned once and must never be reused/renumbered — Iceberg schema
# evolution (AC-05) tracks columns by ID, not name or position.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "decision_id", StringType(), required=True),
    NestedField(2, "apartment_id", StringType(), required=True),
    NestedField(3, "apartment_reference", StringType()),
    NestedField(4, "target_date", DateType()),
    NestedField(5, "decided_at", TimestampType(), required=True),
    NestedField(
        6,
        "cost_inputs",
        StructType(
            NestedField(
                7,
                "billing_period",
                StructType(
                    NestedField(8, "start", DateType()),
                    NestedField(9, "end", DateType()),
                ),
            ),
            NestedField(10, "total_monthly_cost_eur", DoubleType()),
            NestedField(11, "available_days", IntegerType()),
            NestedField(12, "fixed_cost_eur", DoubleType()),
            NestedField(13, "variable_cost_eur", DoubleType()),
            NestedField(14, "one_time_cost_eur", DoubleType()),
            NestedField(15, "cost_lines_count", IntegerType()),
        ),
    ),
    NestedField(
        16,
        "market_inputs",
        StructType(
            NestedField(17, "market_area", StringType()),
            NestedField(18, "avg_nightly_rate_eur", DoubleType()),
            NestedField(19, "occupancy_rate", DoubleType()),
            NestedField(20, "sample_size", IntegerType()),
            NestedField(21, "collected_at", TimestampType()),
            NestedField(22, "data_age_seconds", IntegerType()),
        ),
    ),
    NestedField(
        23,
        "calculation",
        StructType(
            NestedField(24, "target_margin", DoubleType()),
            NestedField(25, "minimum_price_eur", DoubleType()),
            NestedField(26, "floor_type", StringType()),
            NestedField(27, "commission_pct", DoubleType()),
            NestedField(28, "days_to_arrival", IntegerType()),
            NestedField(29, "competitiveness_discount", DoubleType()),
            NestedField(30, "market_reference_price_eur", DoubleType()),
            NestedField(31, "rule_applied", StringType()),
        ),
    ),
    NestedField(
        32,
        "output",
        StructType(
            NestedField(33, "suggested_price_eur", DoubleType()),
            NestedField(34, "currency", StringType()),
            NestedField(35, "effective_margin", DoubleType()),
            NestedField(36, "below_market_by", DoubleType()),
        ),
    ),
    NestedField(37, "dynamodb_event_name", StringType(), required=True),
    NestedField(38, "ingested_at", TimestampType(), required=True),
)

# Source column ID `decided_at` partitions on — kept alongside the schema it
# describes so the two never drift apart independently.
DECIDED_AT_FIELD_ID = 5

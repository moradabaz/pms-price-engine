from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

# Field ID note (Phase 15, ADR-0011 backlog #12): the highest ID in use
# before this phase is 67 (Phase 14's manual_override.expected_loss_eur).
# This phase's new IDs therefore start at 68.
# Field ID note (Phase 16, ADR-0011 backlog #2): the highest ID in use before
# this phase is 72 (Phase 15's suggested_price_per_reservation_eur). This
# phase's new IDs therefore start at 73.

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
            # Phase 8 (ADR-0011 backlog #6): appended, not inserted before
            # rule_applied's ID — new IDs go at the end of the sequence
            # regardless of logical position in the struct (see module docstring).
            NestedField(39, "property_attribute_factor", DoubleType()),
            NestedField(40, "property_reference_price_eur", DoubleType()),
            # Phase 9 (ADR-0011 backlog #1): first list field in this schema —
            # element_id/its own nested field IDs also continue the sequence,
            # never reused/renumbered (see module docstring).
            NestedField(
                41,
                "los_floor_matrix",
                ListType(
                    element_id=42,
                    element_type=StructType(
                        NestedField(43, "stay_length", IntegerType()),
                        NestedField(44, "minimum_price_eur", DoubleType()),
                        NestedField(45, "floor_type", StringType()),
                        NestedField(46, "rule_applied", StringType()),
                        # Phase 12 (ADR-0011 backlog #10): appended, not
                        # inserted next to floor_type/45 — new IDs always go
                        # at the end of the sequence (see module docstring).
                        NestedField(61, "floor_policy", StringType()),
                        NestedField(47, "suggested_price_eur", DoubleType()),
                        NestedField(48, "effective_margin", DoubleType()),
                        # Phase 10 (ADR-0011 backlog #4): a list nested inside
                        # this list — this candidate's own rule component only
                        # (spec 10 §E), appended, never inserted before 43-48.
                        NestedField(
                            54,
                            "decision_components",
                            ListType(
                                element_id=55,
                                element_type=StructType(
                                    NestedField(56, "code", StringType()),
                                    NestedField(57, "label", StringType()),
                                    NestedField(58, "impact", DoubleType()),
                                ),
                                element_required=True,
                            ),
                        ),
                    ),
                    element_required=True,
                ),
            ),
            # Phase 10 (ADR-0011 backlog #4): the 4 property + 1 rule
            # components for the stay_length=1 (top-level) decision.
            NestedField(
                49,
                "decision_components",
                ListType(
                    element_id=50,
                    element_type=StructType(
                        NestedField(51, "code", StringType()),
                        NestedField(52, "label", StringType()),
                        NestedField(53, "impact", DoubleType()),
                    ),
                    element_required=True,
                ),
            ),
            # Phase 11 (ADR-0011 backlog #5): a cheap scalar addition, not a
            # new shape (a string, like floor_type/rule_applied already are).
            NestedField(59, "commission_base", StringType()),
            # Phase 12 (ADR-0011 backlog #10): same kind of cheap scalar
            # addition, classifying floor_type into Hard/Soft.
            NestedField(60, "floor_policy", StringType()),
            # Phase 14 (ADR-0011 backlog #9): the first OPTIONAL
            # (required=False) nested struct in this schema — every prior
            # nested field here has been required=True. None (absent) is the
            # correct, common case: no active override for this decision.
            NestedField(
                62,
                "manual_override",
                StructType(
                    NestedField(63, "override_price_eur", DoubleType()),
                    NestedField(64, "reason", StringType()),
                    NestedField(65, "authorized_by", StringType()),
                    NestedField(66, "valid_until", TimestampType()),
                    NestedField(67, "expected_loss_eur", DoubleType()),
                ),
                required=False,
            ),
            # Phase 15 (ADR-0011 backlog #12): always present, unlike
            # manual_override above — computed from los_floor_matrix, which
            # is itself always present. required=True at the struct level,
            # but its own leaf fields are individually nullable (all four
            # can be None together, iff recommended_min_stay is None) — same
            # convention market_inputs.occupancy_rate/sample_size already use.
            NestedField(
                68,
                "minimum_stay_recommendation",
                StructType(
                    NestedField(69, "recommended_min_stay", IntegerType()),
                    NestedField(70, "floor_relief_eur", DoubleType()),
                    # Reservation-total fields, appended out of struct order
                    # (see module docstring) — a whole reservation's cost and
                    # what it should be priced at, at recommended_min_stay.
                    NestedField(71, "cost_per_reservation_eur", DoubleType()),
                    NestedField(
                        72, "suggested_price_per_reservation_eur", DoubleType()
                    ),
                ),
            ),
            # Phase 16 (ADR-0011 backlog #2): required at the struct/list
            # level (like los_floor_matrix, not manual_override) — always
            # present, but legitimately an empty list until market-ingestor's
            # channel-specific events for this night have arrived.
            NestedField(
                73,
                "channel_price_matrix",
                ListType(
                    element_id=74,
                    element_type=StructType(
                        NestedField(75, "platform", StringType()),
                        NestedField(76, "avg_nightly_rate_eur", DoubleType()),
                        NestedField(77, "commission_pct", DoubleType()),
                        NestedField(78, "market_reference_price_eur", DoubleType()),
                        NestedField(79, "minimum_price_eur", DoubleType()),
                        NestedField(80, "floor_type", StringType()),
                        NestedField(81, "floor_policy", StringType()),
                        NestedField(82, "rule_applied", StringType()),
                        NestedField(83, "suggested_price_eur", DoubleType()),
                        NestedField(84, "effective_margin", DoubleType()),
                        NestedField(
                            85,
                            "decision_components",
                            ListType(
                                element_id=86,
                                element_type=StructType(
                                    NestedField(87, "code", StringType()),
                                    NestedField(88, "label", StringType()),
                                    NestedField(89, "impact", DoubleType()),
                                ),
                                element_required=True,
                            ),
                        ),
                    ),
                    element_required=True,
                ),
            ),
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

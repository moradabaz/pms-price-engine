from datetime import date, datetime
from typing import Any


def _date(value: Any) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _ts(value: Any) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _num(value: Any) -> float | None:
    return float(value) if value is not None else None


def _int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _decision_components(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Coerces one decision_components array (Phase 10, ADR-0011 backlog #4)
    — either calculation's own or one los_floor_matrix candidate's. Returns
    the coerced list."""
    return [
        {
            "code": component["code"],
            "label": component["label"],
            "impact": _num(component["impact"]),
        }
        for component in components
    ]


def row_from_new_image(
    new_image: dict[str, Any], event_name: str, ingested_at: datetime
) -> dict[str, Any]:
    """Builds one Iceberg row from a deserialized DynamoDB Streams NewImage —
    the item is already the same field shape as price_decision.v1 (Phase 4's
    DynamoDbSinkFunction writes it via model_dump(mode="json")), so this is
    mostly type coercion: JSON-string dates/timestamps back into date/datetime,
    Decimal numbers back into float/int. Returns the row, ready for
    pyarrow.Table.from_pylist against schema.ICEBERG_SCHEMA's arrow
    equivalent."""
    cost_inputs = new_image["cost_inputs"]
    market_inputs = new_image["market_inputs"]
    calculation = new_image["calculation"]
    output = new_image["output"]
    billing_period = cost_inputs["billing_period"]

    return {
        "decision_id": str(new_image["decision_id"]),
        "apartment_id": new_image["apartment_id"],
        "apartment_reference": new_image.get("apartment_reference"),
        "target_date": _date(new_image.get("target_date")),
        "decided_at": _ts(new_image["decided_at"]),
        "cost_inputs": {
            "billing_period": {
                "start": _date(billing_period["start"]),
                "end": _date(billing_period["end"]),
            },
            "total_monthly_cost_eur": _num(cost_inputs["total_monthly_cost_eur"]),
            "available_days": _int(cost_inputs["available_days"]),
            "fixed_cost_eur": _num(cost_inputs["fixed_cost_eur"]),
            "variable_cost_eur": _num(cost_inputs["variable_cost_eur"]),
            "one_time_cost_eur": _num(cost_inputs["one_time_cost_eur"]),
            "cost_lines_count": _int(cost_inputs.get("cost_lines_count")),
        },
        "market_inputs": {
            "market_area": market_inputs["market_area"],
            "avg_nightly_rate_eur": _num(market_inputs["avg_nightly_rate_eur"]),
            "occupancy_rate": _num(market_inputs.get("occupancy_rate")),
            "sample_size": _int(market_inputs.get("sample_size")),
            "collected_at": _ts(market_inputs["collected_at"]),
            "data_age_seconds": _int(market_inputs["data_age_seconds"]),
        },
        "calculation": {
            "target_margin": _num(calculation["target_margin"]),
            "minimum_price_eur": _num(calculation["minimum_price_eur"]),
            "floor_type": calculation["floor_type"],
            "commission_pct": _num(calculation["commission_pct"]),
            "days_to_arrival": _int(calculation["days_to_arrival"]),
            "competitiveness_discount": _num(calculation["competitiveness_discount"]),
            # Phase 8/9 fields (ADR-0011 backlog #6/#1): defensive defaults for
            # DynamoDB Streams records predating each field's rollout — the
            # same gap ADR-0009's own commission_pct fallback (job.py) already
            # hit once for a different consumer. property_reference_price_eur
            # defaults to avg_nightly_rate_eur (the true pre-Phase-8 value,
            # since property_attribute_factor's own default is 1.0); an empty
            # los_floor_matrix is the honest answer for a record that predates
            # LOS-aware pricing entirely — never fabricated.
            "property_attribute_factor": _num(
                calculation.get("property_attribute_factor", 1.0)
            ),
            "property_reference_price_eur": _num(
                calculation.get(
                    "property_reference_price_eur",
                    market_inputs["avg_nightly_rate_eur"],
                )
            ),
            "market_reference_price_eur": _num(
                calculation["market_reference_price_eur"]
            ),
            "rule_applied": calculation["rule_applied"],
            "los_floor_matrix": [
                {
                    "stay_length": _int(candidate["stay_length"]),
                    "minimum_price_eur": _num(candidate["minimum_price_eur"]),
                    "floor_type": candidate["floor_type"],
                    "rule_applied": candidate["rule_applied"],
                    "suggested_price_eur": _num(candidate["suggested_price_eur"]),
                    "effective_margin": _num(candidate["effective_margin"]),
                    # Phase 10 (ADR-0011 backlog #4): a record predating this
                    # phase has no decision_components at all, at either
                    # nesting level — empty list is the honest answer, same
                    # convention Phase 9 established for los_floor_matrix
                    # itself.
                    "decision_components": _decision_components(
                        candidate.get("decision_components", [])
                    ),
                }
                for candidate in calculation.get("los_floor_matrix", [])
            ],
            "decision_components": _decision_components(
                calculation.get("decision_components", [])
            ),
        },
        "output": {
            "suggested_price_eur": _num(output["suggested_price_eur"]),
            "currency": output["currency"],
            "effective_margin": _num(output["effective_margin"]),
            "below_market_by": _num(output["below_market_by"]),
        },
        "dynamodb_event_name": event_name,
        "ingested_at": ingested_at,
    }

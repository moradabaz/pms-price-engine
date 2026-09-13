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


def _floor_policy_for(floor_type: str) -> str:
    """Same derivation as pricing.py's floor_policy_for (ADR-0011 backlog
    #10) — duplicated here since lakehouse-consumer doesn't depend on
    flink_jobs. Used only as a fallback for records predating Phase 12,
    which never have a fabricated floor_policy, only a re-derived one."""
    return "hard" if floor_type == "contribution" else "soft"


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


def _manual_override(calculation: dict[str, Any]) -> dict[str, Any] | None:
    """Coerces calculation.manual_override (Phase 14, ADR-0011 backlog #9).
    Returns None both for a record predating this phase (key absent) and for
    a post-Phase-14 record with no active override (key present, value
    null) — the same value for both, since neither case has an override to
    describe. Returns the coerced dict when one is present."""
    manual_override = calculation.get("manual_override")
    if manual_override is None:
        return None
    return {
        "override_price_eur": _num(manual_override["override_price_eur"]),
        "reason": manual_override["reason"],
        "authorized_by": manual_override["authorized_by"],
        "valid_until": _ts(manual_override["valid_until"]),
        "expected_loss_eur": _num(manual_override["expected_loss_eur"]),
    }


def _minimum_stay_recommendation(calculation: dict[str, Any]) -> dict[str, Any]:
    """Coerces calculation.minimum_stay_recommendation (Phase 15, ADR-0011
    backlog #12). Always returns a dict — both-None field values for a
    record predating this phase (key absent, since the recommendation was
    never computed for it, not fabricated), the coerced values otherwise."""
    recommendation = calculation.get("minimum_stay_recommendation") or {}
    return {
        "recommended_min_stay": _int(recommendation.get("recommended_min_stay")),
        "floor_relief_eur": _num(recommendation.get("floor_relief_eur")),
        "cost_per_reservation_eur": _num(
            recommendation.get("cost_per_reservation_eur")
        ),
        "suggested_price_per_reservation_eur": _num(
            recommendation.get("suggested_price_per_reservation_eur")
        ),
    }


def _channel_price_matrix(calculation: dict[str, Any]) -> list[dict[str, Any]]:
    """Coerces calculation.channel_price_matrix (Phase 16, ADR-0011 backlog
    #2). [] both for a record predating this phase (key absent) and for one
    where no channel data had reached this night yet (key present, value
    []) — the same honest answer for both, same convention los_floor_matrix
    itself established."""
    return [
        {
            "platform": candidate["platform"],
            "avg_nightly_rate_eur": _num(candidate["avg_nightly_rate_eur"]),
            "commission_pct": _num(candidate["commission_pct"]),
            "market_reference_price_eur": _num(
                candidate["market_reference_price_eur"]
            ),
            "minimum_price_eur": _num(candidate["minimum_price_eur"]),
            "floor_type": candidate.get("floor_type"),
            "floor_policy": candidate["floor_policy"],
            "rule_applied": candidate["rule_applied"],
            "suggested_price_eur": _num(candidate["suggested_price_eur"]),
            "effective_margin": _num(candidate["effective_margin"]),
            "decision_components": _decision_components(
                candidate.get("decision_components", [])
            ),
        }
        for candidate in calculation.get("channel_price_matrix", [])
    ]


def _cost_breakdown(cost_inputs: dict[str, Any]) -> list[dict[str, Any]]:
    """Coerces cost_inputs.cost_breakdown (Phase 17, ADR-0011 backlog #3;
    extended by Phase 20, ADR-0013, spec 20 §2). [] both for a record
    predating Phase 17 (key absent) and for one with no matching lines in
    the period (shouldn't happen in practice, but the same honest answer
    either way) — same convention channel_price_matrix already established.
    A record between Phase 17 and Phase 20 has entries without the 6 new
    dimension keys — None for each (honest: those dimensions were never
    computed for it), not fabricated."""
    return [
        {
            "concept": entry["concept"],
            "amount_eur": _num(entry["amount_eur"]),
            "scope": entry.get("scope"),
            "behavior": entry.get("behavior"),
            "trigger": entry.get("trigger"),
            "calculation_base": entry.get("calculation_base"),
            "recurrence": entry.get("recurrence"),
            "allocation_method": entry.get("allocation_method"),
        }
        for entry in cost_inputs.get("cost_breakdown", [])
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
            # Phase 20 (ADR-0013): renamed from one_time_cost_eur — a record
            # predating this phase only has the old key, never both.
            "per_booking_cost_eur": _num(
                cost_inputs.get(
                    "per_booking_cost_eur", cost_inputs.get("one_time_cost_eur")
                )
            ),
            "cost_lines_count": _int(cost_inputs.get("cost_lines_count")),
            # Phase 17 (ADR-0011 backlog #3).
            "cost_breakdown": _cost_breakdown(cost_inputs),
            # Phase 20 (ADR-0013): a record predating this phase has neither
            # key — re-derived from that same record's own fixed_cost_eur/
            # variable_cost_eur (fixed_and_allocated_costs_eur) or left at
            # 0.0 (p — no percentage-cost concept existed to sum), never a
            # fabricated constant.
            "fixed_and_allocated_costs_eur": _num(
                cost_inputs.get(
                    "fixed_and_allocated_costs_eur",
                    cost_inputs["fixed_cost_eur"] + cost_inputs["variable_cost_eur"],
                )
            ),
            "p": _num(cost_inputs.get("p", 0.0)),
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
            # Phase 20 (ADR-0013 §5): RETIRED — never written for a new
            # record (column stays forever, just always null going forward).
            "floor_type": calculation.get("floor_type"),
            # Phase 12 (ADR-0011 backlog #10): a record predating that phase
            # has no floor_policy at all — re-derived from that same
            # record's own floor_type when present (pre-Phase-20 shape),
            # else from target_margin (Phase 20 onward, ADR-0013 §5), never a
            # fabricated constant.
            "floor_policy": calculation.get("floor_policy")
            or (
                _floor_policy_for(calculation["floor_type"])
                if calculation.get("floor_type")
                else ("hard" if calculation["target_margin"] == 0 else "soft")
            ),
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
                    "floor_type": candidate.get("floor_type"),
                    "floor_policy": candidate.get("floor_policy")
                    or (
                        _floor_policy_for(candidate["floor_type"])
                        if candidate.get("floor_type")
                        else ("hard" if calculation["target_margin"] == 0 else "soft")
                    ),
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
            # Phase 11 (ADR-0011 backlog #5): a record predating this phase
            # has no commission_base at all — total_revenue is the true
            # pre-Phase-11 value (decide_price()'s own default), not a
            # fabricated one.
            "commission_base": calculation.get("commission_base", "total_revenue"),
            # Phase 14 (ADR-0011 backlog #9): None for a record predating
            # this phase and for a decision with no active override — both
            # the honest answer, never fabricated.
            "manual_override": _manual_override(calculation),
            # Phase 15 (ADR-0011 backlog #12).
            "minimum_stay_recommendation": _minimum_stay_recommendation(calculation),
            # Phase 16 (ADR-0011 backlog #2).
            "channel_price_matrix": _channel_price_matrix(calculation),
            # Phase 20 (ADR-0013): a record predating this phase has neither
            # — re-derived as best as possible from minimum_price_eur/
            # target_margin (never fabricated from nothing), same fallback
            # spirit as fixed_and_allocated_costs_eur above.
            "break_even_revenue_eur": _num(
                calculation.get(
                    "break_even_revenue_eur", calculation["minimum_price_eur"]
                )
            ),
            "profitable_floor_eur": _num(
                calculation.get(
                    "profitable_floor_eur", calculation["minimum_price_eur"]
                )
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

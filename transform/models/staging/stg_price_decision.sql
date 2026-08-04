-- 1:1 cleanup of the Iceberg source (spec 05 §7) — no business logic here,
-- just flattening the nested structs price_decision.v1 arrived with and
-- renaming a couple of ambiguous fields (collected_at -> market_collected_at,
-- since dim_date/downstream models already have a decided_at-based day).
select
    decision_id,
    apartment_id,
    apartment_reference,
    target_date,
    decided_at,

    cost_inputs.billing_period.start as billing_period_start,
    cost_inputs.billing_period."end" as billing_period_end,
    cost_inputs.total_monthly_cost_eur,
    cost_inputs.available_days,
    cost_inputs.fixed_cost_eur,
    cost_inputs.variable_cost_eur,
    cost_inputs.one_time_cost_eur,
    cost_inputs.cost_lines_count,

    market_inputs.market_area,
    market_inputs.avg_nightly_rate_eur,
    market_inputs.occupancy_rate,
    market_inputs.sample_size,
    market_inputs.collected_at as market_collected_at,
    market_inputs.data_age_seconds,

    calculation.target_margin,
    calculation.minimum_price_eur,
    calculation.floor_type,
    calculation.commission_pct,
    calculation.days_to_arrival,
    calculation.competitiveness_discount,
    calculation.market_reference_price_eur,
    calculation.rule_applied,

    output.suggested_price_eur,
    output.currency,
    output.effective_margin,
    output.below_market_by,

    dynamodb_event_name,
    ingested_at
from {{ source('lakehouse', 'price_decision_raw') }}

-- Transaction fact, 1 row per decision emitted (spec 05 §8) — same grain as
-- the source, literally the audit trail price_decision.v1 itself promises
-- ("reconstruct exactly why a price was set, months later").
select
    decision_id,
    apartment_id,
    target_date,
    decided_at,
    total_monthly_cost_eur,
    fixed_cost_eur,
    variable_cost_eur,
    one_time_cost_eur,
    cost_lines_count,
    avg_nightly_rate_eur,
    occupancy_rate,
    sample_size,
    market_collected_at,
    data_age_seconds,
    target_margin,
    minimum_price_eur,
    floor_type,
    commission_pct,
    days_to_arrival,
    competitiveness_discount,
    market_reference_price_eur,
    rule_applied,
    suggested_price_eur,
    currency,
    effective_margin,
    below_market_by
from {{ ref('stg_price_decision') }}

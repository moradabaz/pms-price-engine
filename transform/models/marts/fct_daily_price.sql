-- Periodic snapshot fact, 1 row per (apartment, target_date, day) — spec 05
-- §8: what a price-evolution chart actually plots, without every
-- recalculation's noise (that's fct_price_decision's job).
select
    apartment_id,
    target_date,
    snapshot_day,
    decision_id,
    suggested_price_eur,
    rule_applied,
    floor_type,
    effective_margin
from {{ ref('int_latest_decision_per_night') }}

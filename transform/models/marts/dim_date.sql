-- Conformed calendar dimension (spec 05 §8) — a fixed, pre-built range
-- (Kimball convention), not derived from fact-table min/max, so it doesn't
-- shrink/grow with what happens to be in price_decision_raw today. Reuses
-- market-ingestor's own seasonality bands (seeds/seasonality.csv, AC-08)
-- rather than reinventing them here.
with spine as (
    select unnest(generate_series(
        date '2025-01-01', date '2027-12-31', interval 1 day
    )) as date_day
)

select
    spine.date_day,
    extract(year from spine.date_day) as year,
    extract(month from spine.date_day) as month,
    extract(quarter from spine.date_day) as quarter,
    extract(dow from spine.date_day) as day_of_week,
    extract(dow from spine.date_day) in (0, 6) as is_weekend,
    seasonality.season_label,
    seasonality.multiplier as seasonality_multiplier
from spine
left join {{ ref('seasonality') }} as seasonality
    on extract(month from spine.date_day) = seasonality.month

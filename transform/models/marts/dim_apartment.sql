-- SCD1: latest apartment_reference/city/neighborhood seen per apartment_id
-- (spec 05 §8). No property_type/bedrooms here — confirmed 2026-08-04:
-- price_decision.v1's market_area is only "city/neighborhood"
-- (stage_price_decision.py: f"{city}/{neighborhood}", ADR-0006/spec 05 §4
-- lists price_decision_raw as dbt's only input) — property_type/bedrooms
-- live in apartment_market_segments (Postgres), never forwarded into
-- price_decision.v1. Explicit known limitation (spec 05 §13), not a
-- silent gap — would need either Phase 4 forwarding those fields, or a
-- second dbt source, neither in scope here.
with parsed as (
    select
        apartment_id,
        apartment_reference,
        split_part(market_area, '/', 1) as city,
        nullif(split_part(market_area, '/', 2), '') as neighborhood,
        decided_at
    from {{ ref('stg_price_decision') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by apartment_id order by decided_at desc
        ) as rn
    from parsed
)

select apartment_id, apartment_reference, city, neighborhood
from ranked
where rn = 1

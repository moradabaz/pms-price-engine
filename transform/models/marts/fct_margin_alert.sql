-- Factless/accumulating fact — the README's own "margin alerts" model (spec
-- 05 §8): 1 row per decision where the cost floor pushed the price above the
-- raw market average (ADR-0007's cost_protected rule).
select *
from {{ ref('fct_price_decision') }}
where rule_applied = 'cost_protected'

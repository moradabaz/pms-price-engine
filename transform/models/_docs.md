{#
  Shared column-level documentation (dbt doc blocks). Referenced from
  staging/_staging.yml, intermediate/_intermediate.yml and marts/_marts.yml
  via `{{ doc('name') }}`, so a field's meaning is written in exactly one
  place, no matter how many models forward it.

  Mirrors specs/events/price_decision.v1.json's own field descriptions.
  See docs/metrics-dictionary.md for the plain-language business version.
#}

{% docs decision_id %}
Unique id of one pricing decision (one Flink run for one apartment/night).
Every re-decision of the same apartment/night gets a new decision_id. This
is a transaction-grain key, not a stable "current price" key.
{% enddocs %}

{% docs apartment_id %}
Surrogate key for the apartment, assigned by the PMS database (Postgres).
Stable even if the human-readable reference (apartment_reference) changes.
{% enddocs %}

{% docs apartment_reference %}
Human-readable property code used by the property manager (e.g. BCN-042).
Use apartment_id for joins. This field is for display only.
{% enddocs %}

{% docs target_date %}
The night being priced (check-in date). Not the date the decision was
made. See decided_at for that.
{% enddocs %}

{% docs decided_at %}
Timestamp when the pricing engine produced this decision. A single
apartment/night is re-decided every time new cost or market data arrives,
so one target_date can have many rows, each with a different decided_at.
{% enddocs %}

{% docs available_days %}
Number of days in billing_period that fixed_cost_eur and variable_cost_eur
are divided by to get a per-night cost. Normally the number of days in the
calendar month billing_period covers.
{% enddocs %}

{% docs total_monthly_cost_eur %}
Sum of every payment_line (electricity, cleaning, OTA fees, and so on)
allocated to this apartment for the billing period, before per-night
amortisation.
{% enddocs %}

{% docs fixed_cost_eur %}
Informational only (since the Phase 20 cost-model rewrite, ADR-0013) —
total for the billing period of costs whose CostDefinition classifies them
as behavior=fixed (community fee, insurance, subscriptions, rent, and so
on), grouped for display. No longer read directly by the pricing formula;
see fixed_and_allocated_costs_eur.
{% enddocs %}

{% docs variable_cost_eur %}
Informational only (since the Phase 20 cost-model rewrite, ADR-0013) —
total for the billing period of costs whose CostDefinition classifies them
as behavior=variable/semi_variable, grouped for display. No longer read
directly by the pricing formula; see fixed_and_allocated_costs_eur.
{% enddocs %}

{% docs per_booking_cost_eur %}
Renamed from one_time_cost_eur (Phase 20 cost-model rewrite, ADR-0013).
Total for the billing period of costs whose CostAllocationRule allocates
them per booking (a single non-recurring or per-turnover charge, e.g. a
repair), amortised across a stay's nights rather than divided evenly like
fixed_and_allocated_costs_eur.
{% enddocs %}

{% docs fixed_and_allocated_costs_eur %}
The external spec's own Break-Even/Profitable Floor term (ADR-0013):
fixed_cost_eur plus variable_cost_eur — every non-percentage cost, already
allocated to a per-night figure. Feeds minimum_price_eur/
break_even_revenue_eur/profitable_floor_eur directly.
{% enddocs %}

{% docs p %}
Sum of every applicable percentage cost's rate for this apartment/period
(ADR-0013) — OTA fees, owner commission, and any other cost defined as a
percentage of revenue. Used as the "p" term in the Break-Even/Profitable
Floor formula: BER = fixed_and_allocated_costs_eur / (1 - p).
{% enddocs %}

{% docs cost_lines_count %}
How many individual payment_line rows were aggregated into this
decision's cost figures. An audit count, not used in any pricing formula.
{% enddocs %}

{% docs cost_breakdown %}
Per-night cost broken down by concept (electricity, cleaning, ota_fee,
and so on), plus (since the Phase 20 cost-model rewrite) each concept's own
scope/behavior/trigger/calculation_base/recurrence/allocation_method
dimensions. See docs/metrics-dictionary.md for what each concept means.
Sparse: a concept absent from the billing period has no row here, never
a zero row. Already fully counted inside fixed_cost_eur, variable_cost_eur
and per_booking_cost_eur above. This is a breakdown for transparency, not
an extra cost.
{% enddocs %}

{% docs avg_nightly_rate_eur %}
Average nightly rate comparable properties in the same market area are
charging right now. The raw "what the market pays" figure, before any
adjustment for this specific property (see property_reference_price_eur).
{% enddocs %}

{% docs occupancy_rate %}
Fraction of comparable properties in the market area currently booked for
this date (0-1). Higher occupancy is a signal for scarcity-driven
pricing.
{% enddocs %}

{% docs sample_size %}
How many comparable properties fed into avg_nightly_rate_eur and
occupancy_rate. A small sample_size means the market figure is less
reliable.
{% enddocs %}

{% docs market_collected_at %}
When the market snapshot (avg_nightly_rate_eur, occupancy_rate, and so
on) was captured, not when the pricing decision ran. See
data_age_seconds for how stale it was at decision time.
{% enddocs %}

{% docs data_age_seconds %}
Seconds between market_collected_at and decided_at. A large value means
the decision was made on stale market data.
{% enddocs %}

{% docs target_margin %}
The minimum profit margin the business requires on top of cost, as a
fraction (e.g. 0.20 = 20%). Used to compute minimum_price_eur.
{% enddocs %}

{% docs minimum_price_eur %}
The minimum profitable price (cost floor): the lowest nightly price that
still covers cost and clears target_margin, after accounting for
commission_pct/p on top. Always equals profitable_floor_eur (Phase 20
cost-model rewrite, ADR-0013). suggested_price_eur is never allowed below
this, except through an explicit manual_override.
{% enddocs %}

{% docs break_even_revenue_eur %}
BER, the external spec's own Break-Even Revenue (ADR-0013): the price that
covers fixed_and_allocated_costs_eur with zero margin (p included, m=0).
Informational/explainability only — never itself substituted as the
enforced floor; see minimum_price_eur/profitable_floor_eur.
{% enddocs %}

{% docs profitable_floor_eur %}
MPR, the external spec's own Minimum Profitable Revenue (ADR-0013): the
price that covers fixed_and_allocated_costs_eur plus target_margin (p
included). Always equals minimum_price_eur — the floor actually enforced.
{% enddocs %}

{% docs floor_type %}
Retired by the Phase 20 cost-model rewrite (ADR-0013) — the antelación-
tiered floor this column used to name no longer exists; every decision
since uses the same flat Break-Even/Profitable Floor formula regardless of
days_to_arrival. Kept only for rows decided before that rewrite; always
null afterward. See floor_policy/break_even_revenue_eur/
profitable_floor_eur for the current model.
{% enddocs %}

{% docs floor_policy %}
Whether minimum_price_eur is a hard floor (never publish below it) or a
soft floor (may be crossed only via an authorised manual_override, with
the expected loss recorded). Since the Phase 20 cost-model rewrite
(ADR-0013): hard iff target_margin is 0 (break_even_revenue_eur equals
profitable_floor_eur), soft otherwise — no longer derived from floor_type.
See docs/profitable-pricing-glossary.md section 9.
{% enddocs %}

{% docs commission_pct %}
The fraction of revenue the sales channel (OTA) or payment processor
takes as commission. Used to gross up minimum_price_eur so the property
still nets target_margin after commission.
{% enddocs %}

{% docs commission_base %}
Which revenue figure commission_pct is charged against: total_revenue,
revenue_minus_ota, or revenue_minus_ota_minus_cleaning. Depends on the
owner's contract terms (see docs/profitable-pricing-glossary.md section
6).
{% enddocs %}

{% docs days_to_arrival %}
Days between decided_at and target_date. Used by last-minute pricing
rules: a booking decided very close to arrival is treated differently
from one decided months ahead.
{% enddocs %}

{% docs competitiveness_discount %}
Fraction shaved off property_reference_price_eur to line up this
property's listed price with market positioning strategy (e.g. slightly
undercutting comparable listings). 0 means no discount applied.
{% enddocs %}

{% docs property_attribute_factor %}
The "Bonus/Malus" multiplier (docs/profitable-pricing-glossary.md section
3). Adjusts the raw market rate up or down for this specific property's
own features (terrace, floor, no elevator, reviews, and so on). 1.0
means no adjustment. Above 1.0 means the property is worth more than the
market average. Below 1.0 means it is worth less.
{% enddocs %}

{% docs property_reference_price_eur %}
avg_nightly_rate_eur adjusted by property_attribute_factor. What this
specific property is worth, before any commercial strategy or channel
rule is applied. Can legitimately be above the raw market average for a
premium property.
{% enddocs %}

{% docs market_reference_price_eur %}
property_reference_price_eur reduced by competitiveness_discount. The
target "sticker price" before floor or channel rules apply. Despite the
name, this is one step past the glossary's own "Market Reference Price"
(see docs/metrics-dictionary.md). It is closer to that glossary's "Base
Price" step.
{% enddocs %}

{% docs rule_applied %}
Which guardrail decided the final price for this row. market_competitive:
the market price already clears the cost floor, used as-is. minimum_floor:
the market price was below the raw market reference but still above cost,
so the floor was market-based. minimum_profitable_price: the market price
would have been below cost, so the cost-derived floor was charged
instead. Renamed 2026-09-12 from "cost_protected". See
docs/metrics-dictionary.md.
{% enddocs %}

{% docs los_floor_matrix %}
One minimum_price_eur, suggested_price_eur and rule_applied set per
possible stay length (1 night, 2 nights, and so on) for this same
apartment/target_date. A fixed cost line matters much less per night on
a 10-night stay than on a 1-night stay, so the cost floor is not a single
number, it is a matrix. See docs/profitable-pricing-glossary.md section
4.
{% enddocs %}

{% docs decision_components %}
Structured, auditable list of reason codes explaining how this price was
built (e.g. "+10% high season", "floor protected at 142 EUR"). The
machine-readable alternative to a free-text explanation. See
docs/profitable-pricing-glossary.md section 10.
{% enddocs %}

{% docs manual_override %}
Populated only when a human deliberately overrode the algorithmic price
below its floor (with a reason, an authoriser and an expiry). Null on
the overwhelming majority of decisions. See
docs/profitable-pricing-glossary.md section 11.
{% enddocs %}

{% docs minimum_stay_recommendation %}
Whether requiring a longer minimum stay would make an otherwise-unviable
single night profitable, derived from los_floor_matrix. Always present,
even when no recommendation applies (all fields null in that case).
{% enddocs %}

{% docs channel_price_matrix %}
One price per sales channel (Airbnb, Booking, Vrbo, and so on), each run
through the same cost-floor logic with that channel's own market rate
and commission. A Booking.com listing and a direct-channel listing are
not forced to the same price. See docs/profitable-pricing-glossary.md
section 8 for why this does not yet do commission "gross-up".
{% enddocs %}

{% docs suggested_price_eur %}
The final nightly price the engine recommends, after market, floor and
channel rules. This is the number the property manager would actually
publish.
{% enddocs %}

{% docs effective_margin %}
The actual profit margin suggested_price_eur delivers over cost, as a
fraction. Compare against target_margin to see whether this decision
met, exceeded, or (only via manual_override) fell short of the
business's minimum requirement.
{% enddocs %}

{% docs below_market_by %}
suggested_price_eur minus avg_nightly_rate_eur (raw market average).
Negative means priced below the raw market. Positive means priced above
it, commonly a premium property's property_attribute_factor at work. See
docs/metrics-dictionary.md's note on the BCN-003 case.
{% enddocs %}

{% docs currency %}
Always EUR in this PoC. See specs/events/price_decision.v1.json.
{% enddocs %}

{% docs dynamodb_event_name %}
The DynamoDB Streams event type (INSERT/MODIFY) that produced this row.
Lets the lakehouse-consumer's CDC pipeline be audited. Not a pricing
field.
{% enddocs %}

{% docs ingested_at %}
When services/lakehouse-consumer wrote this row into Iceberg. Used by
the dashboard's freshness indicator and by dbt source freshness checks,
not by any pricing formula.
{% enddocs %}

# Metrics and cost type dictionary

**Who this is for:** anyone who needs to know exactly what a column, a metric, or a cost type
means, without reading Python or SQL to find out.

**What this is not:** it does not repeat the "why each concept matters" story. That story already
lives in [`docs/profitable-pricing-glossary.md`](profitable-pricing-glossary.md) (Market Reference
Price, Bonus/Malus, Break-Even, and so on). This document is the level of detail below that: field
by field, what it is, where it comes from, and which table/column it lives in.

**How this stays up to date:** the same definitions, in a more technical style, live as dbt doc
blocks in `transform/models/_docs.md`. Those are browsable through `dbt docs generate` and
`dbt docs serve`, which is the "live" data catalog. This Markdown file is the plain-language
version for quick reference without running anything.

---

## 1. Cost types (`cost_type`)

Every cost that enters the system (an electricity bill, a Booking commission, a cleaning fee) is
classified into one of three types. This decides how the cost is spread across nights, not what
the expense is about.

| Type | What it means | How it enters the formula | Typical examples |
|---|---|---|---|
| `fixed` | Happens regardless of whether the apartment is booked. | Divided by the days available in the period (`available_days`) to get a per-night cost. | Community fee, insurance, PMS subscription, shared office rent. |
| `variable` | Depends on how much the apartment is used or booked. | Scales with the number of bookings or nights, not a flat daily split. | Cleaning per stay, OTA commission, channel manager fee. |
| `one_time` | A single, non-recurring charge. | Amortised or excluded depending on configuration, unlike the even split fixed costs get. | A one-off repair. |

**Where it lives:** `payment_line.cost_type` (source) is aggregated into
`price_decision.cost_inputs.fixed_cost_eur` / `variable_cost_eur` / `one_time_cost_eur`, one total
per type for the billing period.

**Status:** this is the current classification, a fixed set of 3 values in code. Roadmap item `#13`
(see `docs/post-poc-roadmap.md`) proposes modelling this in more detail (is it recurring, exactly
how is it split, when does it apply). Not implemented yet. This needs a business decision first.

---

## 2. Cost concepts (`concept` / `cost_breakdown`)

While `cost_type` says how a cost is spread, `concept` says what the expense is about. The two are
independent (for example, `cleaning` is usually `variable`, but `insurance` is `fixed`).

| Concept | In one line |
|---|---|
| `electricity` | The apartment's electricity bill. |
| `water` | Water bill. |
| `gas` | Gas bill. |
| `internet` | Internet/wifi subscription for the apartment. |
| `pms_subscription` | Property management software subscription. |
| `ota_fee` | Commission taken by the sales platform (Airbnb, Booking, Vrbo). |
| `channel_manager` | Subscription for the software that syncs prices and availability across platforms. |
| `office_rent` | Office rent, shared across apartments when it is a shared cost (`is_shared`). |
| `cleaning` | Cleaning the stay. |
| `maintenance` | Maintenance and repairs. |
| `insurance` | Apartment insurance. |
| `community_fee` | Homeowners' association fee. |
| `other` | Any expense that does not fit the categories above. |

**Where it lives:** `payment_line.concept` (source, 13 fixed values) is aggregated by concept and
shows up in `price_decision.cost_inputs.cost_breakdown` (Phase 17): a list of
`{concept, amount_eur}` per night, with only the concepts actually present that period. If an
apartment had no gas expense, there is no row with 0 euros. It is simply absent.

**Important:** `cost_breakdown` exists only for transparency. It is a breakdown of a cost that is
already counted inside `fixed_cost_eur`, `variable_cost_eur`, or `one_time_cost_eur`. It is not an
extra cost, and it does not change any pricing calculation. It shows up in the dashboard's
"Apartment detail" tab.

**Status:** implemented, see [Phase 17](../specs/phases/17-cost-concept-breakdown/spec.md).
Roadmap item `#14` (company costs, see `docs/post-poc-roadmap.md`) proposes modelling shared
concepts across apartments (like `office_rent`) more precisely. Not implemented yet.

---

## 3. Cost metrics (`cost_inputs.*`)

| Field | What it means |
|---|---|
| `total_monthly_cost_eur` | Sum of every cost for the apartment that period (fixed plus variable plus one time), before splitting per night. |
| `available_days` | Days in the billing period that `fixed_cost_eur` and `variable_cost_eur` are divided by to get a per-night cost. |
| `fixed_cost_eur` | Total fixed costs for the period. See section 1. |
| `variable_cost_eur` | Total variable costs for the period. See section 1. |
| `one_time_cost_eur` | Total one-time costs for the period. See section 1. |
| `cost_lines_count` | How many individual cost lines were aggregated into this decision. An audit count, not used in any pricing formula. |
| `cost_breakdown` | Cost split by concept. See section 2. |

---

## 4. Market metrics (`market_inputs.*`)

| Field | What it means |
|---|---|
| `avg_nightly_rate_eur` | Average nightly rate comparable apartments in the same area are charging right now. The raw "Market Reference Price" (see `profitable-pricing-glossary.md` section 2), not yet adjusted for this specific apartment. |
| `occupancy_rate` | Fraction of comparable apartments already booked for this date (0 to 1). Higher occupancy signals scarcity. |
| `sample_size` | How many comparable apartments were used to calculate `avg_nightly_rate_eur` and `occupancy_rate`. A small sample makes the figure less reliable. |
| `market_collected_at` | When the market snapshot was captured, not when the price was decided. |
| `data_age_seconds` | Seconds between `market_collected_at` and the moment the decision was made. A high value means the decision used stale market data. |

**Status of the market data source:** still synthetic (simulated segments), not real market data.
See roadmap item `#11` (`docs/post-poc-roadmap.md`).

---

## 5. Calculation metrics (`calculation.*`)

| Field | What it means |
|---|---|
| `target_margin` | Minimum profit margin the business requires, as a fraction (0.20 means 20%). |
| `minimum_price_eur` | The "minimum profitable price" (cost floor): the lowest nightly price that covers cost plus `target_margin`, already accounting for channel commission. `suggested_price_eur` never goes below this, except through an authorised manual override. |
| `floor_type` | Which formula produced `minimum_price_eur`. `structural_full_margin`: healthy occupancy, full margin required. `structural_reduced_margin`: low occupancy, margin relaxed to avoid an empty apartment. `contribution`: accept a lower margin just to avoid losing money. |
| `floor_policy` | Whether `minimum_price_eur` is a hard floor (never crossed) or a soft floor (can be crossed only with explicit authorisation, with the expected loss recorded). |
| `commission_pct` | Fraction the sales channel or payment processor takes as commission. |
| `commission_base` | Which revenue figure `commission_pct` is charged against. Depends on the owner's contract terms (see `profitable-pricing-glossary.md` section 6). |
| `days_to_arrival` | Days between the decision and the booking date. Used by last-minute pricing rules. |
| `competitiveness_discount` | Discount (as a fraction) applied to `property_reference_price_eur` for commercial strategy reasons. |
| `property_attribute_factor` | The "Bonus/Malus" multiplier (see `profitable-pricing-glossary.md` section 3). Adjusts the market rate based on this apartment's own features. `1.0` means no adjustment. |
| `property_reference_price_eur` | `avg_nightly_rate_eur` adjusted by `property_attribute_factor`. What this specific apartment is worth, before commercial strategy is applied. Can legitimately sit above the raw market average for a premium apartment. This is exactly the case that motivated this document, see the note below on the BCN-003 case. |
| `market_reference_price_eur` | `property_reference_price_eur` minus `competitiveness_discount`. The "starting price" before the cost floor or channel rules apply. Naming note: despite sharing a name with the glossary's "Market Reference Price", in the code this field sits one step further down the ladder. It matches the glossary's "Base Price" step more closely. It has not been renamed in code yet, since that would change the event contract. If this is confusing, this is the field to remember. |
| `rule_applied` | Which guardrail decided the final price. `market_competitive`: the market price already clears the cost floor, used as is. `minimum_floor`: the market price was below the raw market reference but still above cost, so a market-based floor applied. `minimum_profitable_price`: the market price would have gone below cost, so the cost-derived floor applied instead. See the naming note below. |
| `los_floor_matrix` | One `minimum_price_eur`, `suggested_price_eur`, and `rule_applied` set per possible stay length (1 night, 2 nights, and so on). A fixed cost line weighs much more per night on a short stay than on a long one. |
| `decision_components` | Structured, auditable list explaining how this price was built (for example, "+10% high season"). |
| `manual_override` | Only has a value when a human deliberately forced a price below the algorithmic floor. |
| `minimum_stay_recommendation` | Whether requiring a longer minimum stay would make an otherwise unprofitable single night viable. |
| `channel_price_matrix` | One price per sales channel (Airbnb, Booking, Vrbo), each with its own market rate and commission. |

### A note on `cost_protected` and `minimum_profitable_price`

This field was called `cost_protected` until 2026-09-12. It was renamed to
**`minimum_profitable_price`** because the old name was confusing. It sounded like "protected from
cost" when it actually means "the minimum price needed to avoid losing money was applied". The
dashboard never shows the old term, not even for historical decisions. There is a display-layer
cleanup step for that in `dashboard/src/dashboard/hot_path.py::descrub()`. The raw data in Iceberg
does still carry the old term for decisions made before the rename. The audit trail is never
rewritten. That is why the dbt tests in `transform/models/marts/_marts.yml` accept both values as
valid, and `fct_margin_alert` filters on both. If it had filtered on the new name only, 2179
historical margin alerts would have disappeared silently. That was a real bug, found and fixed
while writing this document.

---

## 6. Output metrics (`output.*`)

| Field | What it means |
|---|---|
| `suggested_price_eur` | The final nightly price the engine recommends. What the property manager would actually publish. |
| `effective_margin` | The actual profit margin `suggested_price_eur` delivers over cost, as a fraction. Compare against `target_margin`. |
| `below_market_by` | `suggested_price_eur` minus `avg_nightly_rate_eur`. Negative means priced below the raw market. Positive means priced above it, usually a premium apartment's Bonus/Malus at work. |

---

## 7. A dashboard-only metric: `status` (Current price)

This does not come from `price_decision.v1`. The dashboard itself calculates it
(`dashboard/src/dashboard/hot_path.py::price_status()`) to color the "Status" column.

| Status | Condition | What it means |
|---|---|---|
| Price Below Cost | `suggested_price_eur < total_cost_eur` | Loses money outright. Should never happen in healthy data. |
| Price Below Profit | covers cost but not `target_margin` | Covers expenses but does not reach the minimum required margin. |
| Price Above Market | above `avg_nightly_rate_eur` | Meets the margin, and also sits above the raw market. Typically a premium apartment. |
| Market Competitive | meets margin and stays at or below market | The usual, desired state. |

---

## How this relates to the rest of the documentation

- [`docs/profitable-pricing-glossary.md`](profitable-pricing-glossary.md): the "why" behind each
  concept (Market Reference Price, Bonus/Malus, Break-Even, layered Revenue Management, and so
  on), written for someone new to the domain.
- [`docs/post-poc-roadmap.md`](post-poc-roadmap.md): what from all of this is implemented and what
  is not.
- `transform/models/_docs.md`: the same column definitions, as reusable dbt doc blocks shared
  between `staging` and `marts` (`dbt docs generate` makes them browsable).
- This document: the quick, field-by-field reference, so nobody has to read code to answer "what
  does this column mean."

# Glossary: Profitable Dynamic Pricing (explained without industry jargon)

**Who this is for:** someone with no prior background in vacation rental or revenue management
who needs to understand what the concepts in
`docs/adr/ADR-0011-profitable-pricing-target-architecture.md` and `docs/post-poc-roadmap.md`
mean, and why they matter.

**What this is not:** it is not an ADR (it does not record a decision) and not a phase spec (it
does not define acceptance criteria to implement anything). It is a map of concepts, in plain
language, with its current status in this repo. When a concept is already implemented, it links to
the phase that implemented it. When it is not, it links to the backlog item in
`post-poc-roadmap.md`.

---

## 1. The core idea, in one sentence

In vacation rental, two forces compete to decide the price of a night: the market (how much people
are willing to pay) and the real cost of that specific booking (cleaning, OTA commission, the
apartment's own share of rent, and so on). A "normal" pricing engine only looks at the market. This
project builds one that never lets the market price push the price below what it actually costs to
run that booking, and that also explains why it landed on that number.

---

## 2. The price ladder (from abstract to concrete)

These are 6 concepts that are not synonyms, even though they are sometimes confused. Each one is a
more concrete step than the previous one.

| Concept | In one line | Analogy |
|---|---|---|
| Market Reference Price | What comparable apartments in the same area are charging right now. | The "market price" of a used car of the same model. |
| Property Reference Price | The above, adjusted because this specific apartment is better or worse than average (it has a pool, no elevator, and so on). | The same car, corrected for its real condition and extras. |
| Base Price | The Property Reference Price, adjusted by the chosen commercial strategy (aggressive, conservative, and so on). | The listing price you would set before negotiating. |
| Break-Even ADR | The minimum price to avoid losing money on that specific booking. | The price at which a car dealer neither gains nor loses. |
| Profitable Floor | The Break-Even price plus the minimum margin the business requires. | The lowest price the dealer accepts and still sleeps soundly. |
| Final Rate | The price that actually gets published, after market, channel, and rules are applied. Never below the Floor, except with an authorised exception. | The final price on the tag. |

**Why it matters:** tools like PriceLabs or Beyond only calculate up to a market-adjusted "Base
Price". They do not know the real cost of this specific booking, so they sometimes recommend
prices below Break-Even without knowing it. This project calculates the full ladder.

---

## 3. Bonus/Malus (Property Attribute Factor)

**What it is:** a multiplier that raises or lowers the market price based on the apartment's own
features (terrace, pool, floor, noise, review quality) instead of treating every apartment in an
area as worth the same.

**Example:** if the market pays an average of 185 euros a night in the area, but this specific
apartment has no elevator and no parking, Bonus/Malus might lower it to around 157 euros. That is
the apartment's "structural value", not the final price for one specific night.

**Status:** implemented. See [Phase 8](../specs/phases/08-property-bonus-malus/spec.md).

---

## 4. Stay cost and why duration matters (LOS)

**LOS means Length of Stay**: how many nights the booking lasts.

**The counterintuitive part:** a fixed booking cost (cleaning plus laundry, say 110 euros) weighs a
lot if the stay is 1 night (110 euros per night), but almost nothing if the stay is 10 nights (11
euros per night). Because of this, the same date can be profitable for a long booking and not
viable for a single-night one, at the same nightly price.

**Practical consequence:** the "minimum profitable price" is not a single number per apartment. It
is a matrix: one value per combination of arrival date and number of nights.

**Status:** implemented. [Phase 9](../specs/phases/09-los-floor-matrix/spec.md) calculates that
matrix. What is not done yet: using that matrix to actively recommend raising the minimum stay when
a single night is not profitable (see the "min-stay lever" item in `post-poc-roadmap.md`).

---

## 5. Break-Even and Profitable Floor (the formula, in words)

- Break-Even Revenue: the minimum you need to charge to cover expenses. When some costs are a
  percentage of what you charge (OTA commission, payment processing commission), the formula is
  not "add up the costs". It requires dividing by what remains after those percentages, because
  the more you raise the price, the more the commission grows too.
- Profitable Floor: the same idea, but also requiring the business's minimum profit margin, not
  just covering expenses.

**Status:** implemented, with the correct formula (division, not multiplication). Fixed in
ADR-0009 after an earlier calculation error was found and corrected.

---

## 6. Owner Contract (contract with the apartment owner)

**The problem:** most property managers do not keep the entire booking revenue. They pay a
commission or "payout" to the apartment owner. But that commission is not always calculated on the
same base. It can be over the total booking, over the total minus the OTA commission, over the
total minus OTA and cleaning, and so on. If the pricing engine assumes the wrong base, it
miscalculates what the manager actually keeps, and therefore miscalculates the floor.

**Status:** implemented for the common cases (closed, known bases). See
[Phase 11](../specs/phases/11-owner-contract/spec.md). Deliberately not implemented: a generic
"solver" for non-standard or non-linear contract formulas. This was a conscious decision, not an
oversight, documented in Phase 11's own spec.

---

## 7. Layered Revenue Management

**What "Revenue Management" is:** the set of rules that adjust the price up or down based on
context: high season, weekends, low occupancy, last-minute bookings, a nearby event (concert,
trade fair), and so on. This is the "smart" part that reacts to the moment, on top of the
apartment's structural price.

**Why "layered":** if every rule adds or subtracts a percentage with no control, discounts can
stack up until the price falls below cost without anyone noticing. Organising this into layers
(structure, market, own performance, lead time, availability, promotions, guardrails) allows a
limit per layer and a global limit, and above all it prevents the final result from crossing the
Profitable Floor, except with an authorised exception.

**Status:** implemented. See [Phase 13](../specs/phases/13-layered-rm-engine/spec.md).

---

## 8. Channel pricing and "gross-up"

**The problem:** selling through Booking.com is not the same as selling direct. Booking takes a
much higher commission than charging directly by card (in the spec's example, around 17% versus
around 2%). If the same price is applied on both channels, one of them earns much less margin
without it showing in the published price.

**"Gross-up"** simply means raising the published price on the expensive channel just enough that,
after the channel takes its commission, the same margin remains as on the cheap channel. It is not
an arbitrary markup. It is math derived from the real cost of selling through that channel.

**Status:** not implemented. The system today uses a single commission figure, blind to channel.
See backlog items `#2` and `#8` (`post-poc-roadmap.md`).

---

## 9. Guardrails and Hard/Soft Floor policy

**What it is:** the rule that decides what happens when the price the market or the commercial
rules "want" falls below the Profitable Floor.

- Hard floor: never publish below the floor, period.
- Soft floor: selling below is allowed, but only with explicit authorisation, and it always
  records the expected loss. Never silently.

**Status:** implemented. See [Phase 12](../specs/phases/12-floor-policy/spec.md).

---

## 10. Explainability (Decision Components and reason codes)

**The problem this solves:** a property manager will not trust a price just because "the machine
said so". They need to see something like: "this price went up 10% for high season, down 3% for
low occupancy, and the profitability floor was 142 euros for this date and channel combination."

**What "reason codes" are:** instead of just free text, each adjustment is stored as structured
data (for example `RULE_SEASON_HIGH`, `FLOOR_PROTECTED`) that can be queried, filtered, and
audited, not just read.

**Status:** implemented. See [Phase 10](../specs/phases/10-decision-components/spec.md).

---

## 11. Manual Override (authorised manual exception)

**What it is:** sometimes the business deliberately wants to sell below the profitability floor.
For example, to avoid leaving an apartment empty before an event, or for a one-off commercial
relationship. This should be possible, but with a responsible user, a reason, an expiry date, and
the expected loss recorded. Never as a silent exception that breaks trust in the system.

**Status:** not implemented. Today the system is read-only downstream of the pricing engine. There
is no path for a human to write an exception back into the system. This is the largest pending
architecture change (backlog item `#9`).

---

## 12. Market Snapshot and comp-set

**Comp-set** ("competitive set"): the group of comparable apartments (same area, size, quality)
used as the market reference. Comparing against the average of an entire city with no filtering
would compare apples to oranges.

**Market Snapshot:** a "photo" of the market at a given moment (average price, percentiles,
occupancy, source, and date of the photo), so a price decision can be audited later against the
information it was made with.

**Status:** the data model is already prepared for this (percentiles, source, capture date). See
[Phase 3](../specs/phases/03-market-ingestion/spec.md). The source is still synthetic (simulated
segments), not real market data. See backlog item `#11`.

---

## 13. Quick acronym glossary

| Acronym | Meaning |
|---|---|
| ADR | Average Daily Rate. Average price per night. |
| LOS | Length of Stay. Number of nights in the booking. |
| OTA | Online Travel Agency. Booking, Airbnb, Vrbo, and so on. |
| RM | Revenue Management. |
| PMS | Property Management System. The software used to manage the apartment and bookings. |
| PoC | Proof of Concept. This project, a technical trial, not a finished product. |

---

## How this relates to the rest of the documentation

- `docs/adr/ADR-0011-profitable-pricing-target-architecture.md`: the decision to adopt the
  external spec as the target architecture.
- `docs/post-poc-roadmap.md`: the prioritised backlog, item by item, with what is done and what is
  missing.
- `docs/metrics-dictionary.md`: the field-by-field reference (cost types, cost concepts, and every
  metric column), one level more concrete than this document.
- This document: the "translator" of the above two, for someone new to the domain.

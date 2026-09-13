import random
from dataclasses import dataclass
from datetime import date, timedelta

# Restricted to the 3 cities services/market-ingestor/src/market_ingestor/segments.py
# actually prices (Phase 4, Decision C.1 / ADR-0006 context) — an apartment must
# belong to a market segment that genuinely exists, not just a plausible-looking
# city code with nothing to compare its cost against. Kept as a literal here, not
# imported cross-service: mock-pm-app and market-ingestor are independently
# deployable services with their own lockfiles. If this list drifts from
# market-ingestor's SEGMENTS, apartment_market_segments' contract test should
# catch it (specs/contracts/).
_NEIGHBORHOODS_BY_CITY: dict[str, list[str]] = {
    "Barcelona": ["Eixample", "Gràcia"],
    "Madrid": ["Centro", "Chamberí"],
    "Valencia": ["Ruzafa", "El Carmen"],
}
_CITY_CODES_BY_NAME: dict[str, str] = {
    "Barcelona": "BCN",
    "Madrid": "MAD",
    "Valencia": "VLC",
}
_PROPERTY_PROFILES: list[tuple[str, int]] = [
    ("studio", 0),
    ("apartment", 1),
    ("apartment", 2),
]

CITY_CODES = list(_CITY_CODES_BY_NAME.values())


def _segment_combos() -> list[tuple[str, str, str, int]]:
    # Same nested order as market-ingestor's _build_segments() (18 combos) —
    # apartments are cycled through it so every one lands on a real segment.
    return [
        (city, neighborhood, property_type, bedrooms)
        for city, neighborhoods in _NEIGHBORHOODS_BY_CITY.items()
        for neighborhood in neighborhoods
        for property_type, bedrooms in _PROPERTY_PROFILES
    ]


_SEGMENT_COMBOS = _segment_combos()


# Phase 8 (docs/adr/ADR-0011, backlog #6): raw Property Bonus/Malus
# attributes, synthesized per apartment — not modeled on real property data
# (see Phase 1 spec, Known limitations, same caveat as CONCEPT_PROFILES below).
_QUALITY_TIERS = ["basic", "standard", "premium", "luxury"]


@dataclass(frozen=True)
class Apartment:
    apartment_id: str
    apartment_reference: str
    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    quality_tier: str
    rating: float
    has_view: bool
    has_parking: bool


def build_apartment_pool(
    count: int, rng: random.Random | None = None
) -> list[Apartment]:
    rng = rng or random.Random()
    apartments = []
    for i in range(count):
        city, neighborhood, property_type, bedrooms = _SEGMENT_COMBOS[
            i % len(_SEGMENT_COMBOS)
        ]
        reference = f"{_CITY_CODES_BY_NAME[city]}-{i + 1:03d}"
        apartments.append(
            Apartment(
                apartment_id=reference,
                apartment_reference=reference,
                city=city,
                neighborhood=neighborhood,
                property_type=property_type,
                bedrooms=bedrooms,
                quality_tier=rng.choice(_QUALITY_TIERS),
                rating=round(rng.uniform(3.0, 5.0), 1),
                has_view=rng.random() < 0.3,
                has_parking=rng.random() < 0.3,
            )
        )
    return apartments


# Phase 11 (docs/adr/ADR-0011, backlog #5): a small pool, so several
# apartments genuinely share the same owner — the actual point of a real
# owner entity instead of a flat per-apartment column (spec 11 pre-spec §A).
_OWNER_COUNT = 6
_COMMISSION_BASES = [
    "total_revenue",
    "revenue_minus_ota",
    "revenue_minus_ota_minus_cleaning",
]


@dataclass(frozen=True)
class Owner:
    owner_id: str
    owner_name: str


def build_owner_pool(count: int = _OWNER_COUNT) -> list[Owner]:
    return [
        Owner(owner_id=f"OWN-{i + 1:03d}", owner_name=f"Owner {i + 1}")
        for i in range(count)
    ]


@dataclass(frozen=True)
class OwnerContract:
    apartment_id: str
    owner_id: str
    commission_base: str
    commission_pct: float


def build_owner_contracts(
    apartments: list[Apartment], owners: list[Owner], rng: random.Random | None = None
) -> list[OwnerContract]:
    """Assigns each apartment round-robin to an owner, then a randomized
    commission_base/commission_pct — deliberately not all total_revenue, so
    the netted-base formula (pricing.py) is actually exercised by seeded
    data, not just by unit tests. Returns one contract per apartment."""
    rng = rng or random.Random()
    return [
        OwnerContract(
            apartment_id=apartment.apartment_id,
            owner_id=owners[i % len(owners)].owner_id,
            commission_base=rng.choice(_COMMISSION_BASES),
            commission_pct=round(rng.uniform(0.10, 0.20), 4),
        )
        for i, apartment in enumerate(apartments)
    ]


# Phase 18 (docs/adr/ADR-0011, backlog #13 prerequisite): synthetic
# occupancy data, so occupied/available night allocation rules have
# something real to compute against. Deliberately not "every night booked"
# nor "every night empty" — a mix of gaps and back-to-back stays, so the
# occupancy calculation is actually exercised.
_CHANNELS = ["airbnb", "booking", "vrbo", "direct"]
_STAY_LENGTHS_NIGHTS = [1, 2, 3, 5, 7]


@dataclass(frozen=True)
class Booking:
    apartment_id: str
    check_in: date
    check_out: date
    channel: str
    guests: int
    revenue_eur: float
    status: str


def build_bookings(
    apartments: list[Apartment], rng: random.Random, today: date
) -> list[Booking]:
    """One booking history per apartment: a handful of past/current stays,
    each separated by a randomized gap (so some nights are occupied and some
    are genuinely available), starting 45 days before today. A small
    fraction are cancelled — excluded from occupied-night calculations from
    the moment their status is observed (booking.v1's own contract note).
    Returns every apartment's bookings, flattened."""
    bookings: list[Booking] = []
    for apartment in apartments:
        cursor = today - timedelta(days=45)
        for _ in range(6):
            gap_nights = rng.randint(0, 4)
            cursor = cursor + timedelta(days=gap_nights)
            stay_length = rng.choice(_STAY_LENGTHS_NIGHTS)
            check_in = cursor
            check_out = check_in + timedelta(days=stay_length)
            bookings.append(
                Booking(
                    apartment_id=apartment.apartment_id,
                    check_in=check_in,
                    check_out=check_out,
                    channel=rng.choice(_CHANNELS),
                    guests=rng.randint(1, 4),
                    revenue_eur=round(rng.uniform(60.0, 220.0) * stay_length, 2),
                    status="cancelled" if rng.random() < 0.1 else "confirmed",
                )
            )
            cursor = check_out
    return bookings


@dataclass(frozen=True)
class ConceptProfile:
    # Phase 19 (ADR-0012): cost_type dropped — classification now lives on
    # CostDefinition. This stays a pure generation-parameters spec (which
    # concept, and a plausible VAT rate / amount range for it).
    concept: str
    vat_rate: float
    amount_range: tuple[float, float]


# Amount ranges are plausible EUR figures for a single Spanish vacation
# apartment's monthly cost line — not modeled on real cost data (see Phase 1
# spec, Known limitations). office_rent is deliberately absent here — it is
# scope=company (see COST_DEFINITION_SPECS below) and never appears as a
# per-apartment payment_lines row; it lives in company_cost_occurrences.
CONCEPT_PROFILES = [
    ConceptProfile("electricity", 0.21, (40.0, 180.0)),
    ConceptProfile("water", 0.10, (15.0, 60.0)),
    ConceptProfile("gas", 0.21, (20.0, 90.0)),
    ConceptProfile("internet", 0.21, (30.0, 50.0)),
    ConceptProfile("pms_subscription", 0.21, (20.0, 100.0)),
    ConceptProfile("ota_fee", 0.21, (50.0, 400.0)),
    ConceptProfile("channel_manager", 0.21, (15.0, 40.0)),
    ConceptProfile("cleaning", 0.10, (40.0, 120.0)),
    # Deliberately narrower than a real repair invoice's full range (which
    # can genuinely spike into the hundreds) — this is the one concept whose
    # entire amount lands on a single stay_length=1 decision (recurrence=
    # one_off, no dilution across nights the way LOS-matrix candidates get,
    # spec 20's per_booking_cost_eur). A wide range here overwhelmingly
    # skews the "Current price" dashboard (which only shows the 1-night
    # figure) toward "Price Above Market" regardless of how reasonable every
    # other concept is.
    ConceptProfile("maintenance", 0.21, (20.0, 150.0)),
    ConceptProfile("insurance", 0.0, (20.0, 60.0)),
    ConceptProfile("community_fee", 0.0, (50.0, 150.0)),
]

PAYMENT_METHODS = ["bank_transfer", "direct_debit", "card", "cash"]


# Phase 19 (ADR-0011 backlog #13, ADR-0012): one CostDefinition per concept
# (13, matching payment_line.v1's own concept enum), classifying it along
# the external spec's dimensions instead of the old flat cost_type. Chosen
# deliberately to exercise a broad spread rather than defaulting everything
# to the same values:
# - office_rent: the spec's own canonical shared-cost example, scope=company
#   (never generated as a payment_lines row today — this is what closes
#   that gap), allocation=weighted with weight_config=None (equal split).
# - cleaning: allocation=occupied_night — the whole reason Phase 18 exists.
# - ota_fee: scope=booking, calculation_base=pct_adjusted_revenue,
#   allocation=booking.
# - insurance: recurrence=annual — the "annual lump sum" example the
#   roadmap itself names.
# - maintenance: behavior=semi_variable, trigger=event, allocation=direct —
#   a one-off repair, same "one_time" semantics the old cost_type carried,
#   now expressed as recurrence=one_off instead (see migrations.py's
#   backfill comment for why "one_time" has no matching behavior value).
@dataclass(frozen=True)
class CostDefinitionSpec:
    concept: str
    scope: str
    behavior: str
    trigger: str
    calculation_base: str
    recurrence: str
    revenue_base: str | None
    # Phase 20 (ADR-0013 §3): required exactly when calculation_base is
    # percentage-based — real configuration, never inferred (see
    # cost_definitions.sql's own rate/calculation_base pairing constraint).
    rate: float | None
    allocation_method: str
    weight_config: str | None  # JSON string, only for allocation_method='weighted'


COST_DEFINITION_SPECS = [
    CostDefinitionSpec(
        "electricity", "property", "variable", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "water", "property", "variable", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "gas", "property", "variable", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "internet", "property", "fixed", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "pms_subscription", "property", "fixed", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "ota_fee", "booking", "variable", "reservation", "pct_adjusted_revenue",
        "per_booking", "total_revenue", 0.15, "booking", None,
    ),
    CostDefinitionSpec(
        "channel_manager", "property", "fixed", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "office_rent", "company", "fixed", "time", "fixed_amount",
        "monthly", None, None, "weighted", None,
    ),
    CostDefinitionSpec(
        "cleaning", "booking", "variable", "reservation", "fixed_amount",
        "per_booking", None, None, "occupied_night", None,
    ),
    CostDefinitionSpec(
        "maintenance", "property", "semi_variable", "event", "fixed_amount",
        "one_off", None, None, "direct", None,
    ),
    CostDefinitionSpec(
        "insurance", "property", "fixed", "time", "fixed_amount",
        "annual", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "community_fee", "property", "fixed", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
    CostDefinitionSpec(
        "other", "property", "variable", "time", "fixed_amount",
        "monthly", None, None, "calendar_day", None,
    ),
]


@dataclass(frozen=True)
class CompanyCostOccurrenceSeed:
    cost_definition_id: str
    billing_period_start: date
    billing_period_end: date
    amount_gross: float
    description: str


def build_company_cost_occurrences(
    office_rent_cost_definition_id: str, today: date
) -> list[CompanyCostOccurrenceSeed]:
    """One office_rent occurrence per of the last 2 calendar months — enough
    to exercise the company-cost fan-out (Phase 19 §5) without needing a
    long history. Returns the seed rows."""
    occurrences = []
    for months_ago in (1, 2):
        year = today.year
        month = today.month - months_ago
        while month <= 0:
            month += 12
            year -= 1
        start = date(year, month, 1)
        next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month - timedelta(days=1)
        occurrences.append(
            CompanyCostOccurrenceSeed(
                cost_definition_id=office_rent_cost_definition_id,
                billing_period_start=start,
                billing_period_end=end,
                amount_gross=1200.00,
                description=f"Shared office rent - {start:%Y-%m}",
            )
        )
    return occurrences

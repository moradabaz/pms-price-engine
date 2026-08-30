import random
from dataclasses import dataclass

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


@dataclass(frozen=True)
class ConceptProfile:
    concept: str
    cost_type: str
    vat_rate: float
    amount_range: tuple[float, float]


# Amount ranges are plausible EUR figures for a single Spanish vacation
# apartment's monthly cost line — not modeled on real cost data (see Phase 1
# spec, Known limitations).
CONCEPT_PROFILES = [
    ConceptProfile("electricity", "variable", 0.21, (40.0, 180.0)),
    ConceptProfile("water", "variable", 0.10, (15.0, 60.0)),
    ConceptProfile("gas", "variable", 0.21, (20.0, 90.0)),
    ConceptProfile("internet", "fixed", 0.21, (30.0, 50.0)),
    ConceptProfile("pms_subscription", "fixed", 0.21, (20.0, 100.0)),
    ConceptProfile("ota_fee", "variable", 0.21, (50.0, 400.0)),
    ConceptProfile("channel_manager", "fixed", 0.21, (15.0, 40.0)),
    ConceptProfile("cleaning", "variable", 0.10, (40.0, 120.0)),
    ConceptProfile("maintenance", "one_time", 0.21, (30.0, 500.0)),
    ConceptProfile("insurance", "fixed", 0.0, (20.0, 60.0)),
    ConceptProfile("community_fee", "fixed", 0.0, (50.0, 150.0)),
]

PAYMENT_METHODS = ["bank_transfer", "direct_debit", "card", "cash"]

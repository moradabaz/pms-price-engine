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
_CITY_CODES_BY_NAME: dict[str, str] = {"Barcelona": "BCN", "Madrid": "MAD", "Valencia": "VLC"}
_PROPERTY_PROFILES: list[tuple[str, int]] = [("studio", 0), ("apartment", 1), ("apartment", 2)]

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


@dataclass(frozen=True)
class Apartment:
    apartment_id: str
    apartment_reference: str
    city: str
    neighborhood: str
    property_type: str
    bedrooms: int


def build_apartment_pool(count: int) -> list[Apartment]:
    apartments = []
    for i in range(count):
        city, neighborhood, property_type, bedrooms = _SEGMENT_COMBOS[i % len(_SEGMENT_COMBOS)]
        reference = f"{_CITY_CODES_BY_NAME[city]}-{i + 1:03d}"
        apartments.append(
            Apartment(
                apartment_id=reference,
                apartment_reference=reference,
                city=city,
                neighborhood=neighborhood,
                property_type=property_type,
                bedrooms=bedrooms,
            )
        )
    return apartments


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

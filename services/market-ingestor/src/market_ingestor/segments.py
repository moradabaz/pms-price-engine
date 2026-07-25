from dataclasses import dataclass

# Reference median nightly rate (EUR, 1-bedroom apartment), anchored to
# 2025-2026 market observations — see specs/phases/03-market-ingestion/spec.md
# §5.2 for sources and the per-neighborhood rationale.
_NEIGHBORHOOD_REFERENCE_PRICES: dict[tuple[str, str], float] = {
    ("Barcelona", "Eixample"): 160.0,
    ("Barcelona", "Gràcia"): 110.0,
    ("Madrid", "Centro"): 130.0,
    ("Madrid", "Chamberí"): 95.0,
    ("Valencia", "Ruzafa"): 140.0,
    ("Valencia", "El Carmen"): 125.0,
}

_COUNTRY_CODES: dict[str, str] = {
    "Barcelona": "ES",
    "Madrid": "ES",
    "Valencia": "ES",
}

# (property_type, bedrooms, max_guests, multiplier applied to the
# neighborhood's 1-bedroom reference price) — spec §5.2.
_PROPERTY_PROFILES: list[tuple[str, int, int, float]] = [
    ("studio", 0, 2, 0.7),
    ("apartment", 1, 3, 1.0),
    ("apartment", 2, 5, 1.45),
]


@dataclass(frozen=True)
class Segment:
    city: str
    neighborhood: str
    country_code: str
    property_type: str
    bedrooms: int
    max_guests: int
    reference_median_price: float


def _build_segments() -> list[Segment]:
    return [
        Segment(
            city=city,
            neighborhood=neighborhood,
            country_code=_COUNTRY_CODES[city],
            property_type=property_type,
            bedrooms=bedrooms,
            max_guests=max_guests,
            reference_median_price=round(base_price * multiplier, 2),
        )
        for (city, neighborhood), base_price in _NEIGHBORHOOD_REFERENCE_PRICES.items()
        for property_type, bedrooms, max_guests, multiplier in _PROPERTY_PROFILES
    ]


SEGMENTS: list[Segment] = _build_segments()

from dataclasses import dataclass

CITY_CODES = ["BCN", "MAD", "VLC", "SEV", "PMI", "IBZ"]


@dataclass(frozen=True)
class Apartment:
    apartment_id: str
    apartment_reference: str


def build_apartment_pool(count: int) -> list[Apartment]:
    return [
        Apartment(
            apartment_id=f"{CITY_CODES[i % len(CITY_CODES)]}-{i + 1:03d}",
            apartment_reference=f"{CITY_CODES[i % len(CITY_CODES)]}-{i + 1:03d}",
        )
        for i in range(count)
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

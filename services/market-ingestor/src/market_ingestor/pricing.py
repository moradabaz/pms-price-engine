import math
import random
import statistics
from typing import TypedDict

from market_ingestor.segments import Segment

# Coefficient of variation for the synthetic per-listing price sample — a
# plausible spread for a rental-price market, not itself sourced from a
# specific report (spec §5.2).
_TARGET_COEFFICIENT_OF_VARIATION = 0.35


class PricingStats(TypedDict):
    avg_nightly_rate: float
    p25: float
    p50: float
    p75: float
    currency: str


def sample_pricing(segment: Segment, rng: random.Random) -> tuple[PricingStats, int]:
    """Draw a synthetic per-listing price sample and derive avg/p25/p50/p75
    from it directly, per spec §5.2 — never generated independently, which
    would risk statistically incoherent output (e.g. p75 < p25)."""
    sample_size = rng.randint(3, 45)
    sigma = math.sqrt(math.log(1 + _TARGET_COEFFICIENT_OF_VARIATION**2))
    mu = math.log(segment.reference_median_price) - sigma**2 / 2
    draws = [rng.lognormvariate(mu, sigma) for _ in range(sample_size)]

    p25, p50, p75 = statistics.quantiles(draws, n=4, method="inclusive")
    pricing: PricingStats = {
        "avg_nightly_rate": round(statistics.fmean(draws), 2),
        "p25": round(p25, 2),
        "p50": round(p50, 2),
        "p75": round(p75, 2),
        "currency": "EUR",
    }
    return pricing, sample_size


def sample_occupancy_rate(rng: random.Random) -> float:
    return round(rng.uniform(0.45, 0.85), 2)

import math
import random
import statistics
from datetime import date
from typing import TypedDict

from market_ingestor.seasonality import seasonal_multiplier
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


def sample_pricing(
    segment: Segment, target_date: date, rng: random.Random
) -> tuple[PricingStats, int]:
    """Draw a synthetic per-listing price sample and derive avg/p25/p50/p75
    from it directly, per spec §5.2 — never generated independently, which
    would risk statistically incoherent output (e.g. p75 < p25). The
    segment's reference median is seasonally adjusted for target_date first
    (Decision D.2) — Phase 4 never needs to know this happened, it just reads
    avg_nightly_rate_eur(target_date) already seasoned."""
    sample_size = rng.randint(3, 45)
    sigma = math.sqrt(math.log(1 + _TARGET_COEFFICIENT_OF_VARIATION**2))
    seasoned_median = segment.reference_median_price * seasonal_multiplier(target_date)
    mu = math.log(seasoned_median) - sigma**2 / 2
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


# Phase 16 (ADR-0011 backlog #2): a plausible per-platform spread applied to
# the same seasoned reference median sample_pricing() uses — Airbnb typically
# prices a little above the blended average, Booking.com close to it, Vrbo a
# little below. Not sourced from a specific report, same caveat
# _TARGET_COEFFICIENT_OF_VARIATION already carries.
CHANNEL_PRICE_MULTIPLIERS: dict[str, float] = {
    "airbnb": 1.08,
    "booking": 1.00,
    "vrbo": 0.92,
}


def sample_channel_pricing(
    segment: Segment, platform: str, target_date: date, rng: random.Random
) -> tuple[PricingStats, int]:
    """Draw a synthetic per-listing price sample for one channel — same
    lognormal draw as sample_pricing(), with the seasoned reference median
    scaled by this platform's own multiplier (CHANNEL_PRICE_MULTIPLIERS)
    before drawing, so each channel's avg/p25/p50/p75 are still statistically
    coherent on their own, not just a scalar applied after the fact. Returns
    the channel's pricing stats and sample size."""
    sample_size = rng.randint(3, 45)
    sigma = math.sqrt(math.log(1 + _TARGET_COEFFICIENT_OF_VARIATION**2))
    seasoned_median = (
        segment.reference_median_price
        * seasonal_multiplier(target_date)
        * CHANNEL_PRICE_MULTIPLIERS[platform]
    )
    mu = math.log(seasoned_median) - sigma**2 / 2
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

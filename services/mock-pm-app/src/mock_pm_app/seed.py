import random
from datetime import date, timedelta
from typing import Any

from mock_pm_app.data import CONCEPT_PROFILES, Apartment
from mock_pm_app.rows import build_historical_row, insert_row
from mock_pm_app.settings import MockAppSettings


def already_seeded(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM payment_lines")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_segments(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM apartment_market_segments")
        (count,) = cur.fetchone()
    return bool(count > 0)


def seed_apartment_market_segments(conn: Any, apartments: list[Apartment]) -> int:
    # Decision C.1: seed once, deterministically, from mock-pm-app's own
    # apartment pool — target_margin/competitiveness_discount are left to the
    # table's own DEFAULT 0.05 (Decision C.2), not set here, so the schema
    # stays the single source of truth for that default.
    with conn.cursor() as cur:
        for apartment in apartments:
            cur.execute(
                """
                INSERT INTO apartment_market_segments
                    (apartment_id, apartment_reference, city, neighborhood, property_type, bedrooms)
                VALUES (%(apartment_id)s, %(apartment_reference)s, %(city)s,
                        %(neighborhood)s, %(property_type)s, %(bedrooms)s)
                ON CONFLICT (apartment_id) DO NOTHING
                """,
                {
                    "apartment_id": apartment.apartment_id,
                    "apartment_reference": apartment.apartment_reference,
                    "city": apartment.city,
                    "neighborhood": apartment.neighborhood,
                    "property_type": apartment.property_type,
                    "bedrooms": apartment.bedrooms,
                },
            )
    conn.commit()
    return len(apartments)


def _month_bounds(months_ago: int, today: date) -> tuple[date, date]:
    year = today.year
    month = today.month - months_ago
    while month <= 0:
        month += 12
        year -= 1
    start = date(year, month, 1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(days=1)
    return start, end


def seed(
    conn: Any,
    settings: MockAppSettings,
    apartments: list[Apartment],
    rng: random.Random,
    today: date,
) -> int:
    rows: list[dict[str, Any]] = []
    for apartment in apartments:
        for months_ago in range(1, settings.seed_months + 1):
            period_start, period_end = _month_bounds(months_ago, today)
            profiles = rng.sample(
                CONCEPT_PROFILES, k=rng.randint(3, len(CONCEPT_PROFILES))
            )
            for profile in profiles:
                rows.append(
                    build_historical_row(
                        apartment, profile, period_start, period_end, rng
                    )
                )

    with conn.cursor() as cur:
        for row in rows:
            insert_row(cur, row)
    conn.commit()
    return len(rows)

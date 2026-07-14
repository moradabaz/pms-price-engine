import random
import time
from datetime import date, timedelta
from typing import Any

from common import get_logger

from mock_pm_app.data import CONCEPT_PROFILES, Apartment
from mock_pm_app.rows import build_live_row, insert_row
from mock_pm_app.settings import MockAppSettings

logger = get_logger(__name__)


def _current_month_bounds(today: date) -> tuple[date, date]:
    start = today.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(days=1)
    return start, end


def insert_one(conn: Any, apartments: list[Apartment], rng: random.Random) -> None:
    apartment = rng.choice(apartments)
    profile = rng.choice(CONCEPT_PROFILES)
    period_start, period_end = _current_month_bounds(date.today())
    row = build_live_row(apartment, profile, period_start, period_end, rng)
    with conn.cursor() as cur:
        event_id = insert_row(cur, row)
    conn.commit()
    logger.info(
        "inserted_payment_line",
        event_id=str(event_id),
        apartment_id=apartment.apartment_id,
        concept=profile.concept,
    )


def flip_one_pending_to_paid(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id FROM payment_lines
            WHERE payment_status = 'pending' AND source = 'synthetic'
            ORDER BY random()
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            return
        (event_id,) = row
        cur.execute(
            """
            UPDATE payment_lines
            SET payment_status = 'paid', payment_date = CURRENT_DATE
            WHERE event_id = %s
            """,
            (event_id,),
        )
    conn.commit()
    logger.info("marked_payment_line_paid", event_id=str(event_id))


def run_forever(
    conn: Any, settings: MockAppSettings, apartments: list[Apartment]
) -> None:
    rng = random.Random()
    next_insert_at = time.monotonic()
    next_update_check_at = time.monotonic() + settings.update_check_interval_seconds

    while True:
        now = time.monotonic()

        if now >= next_insert_at:
            insert_one(conn, apartments, rng)
            interval = rng.uniform(
                settings.insert_interval_min_seconds,
                settings.insert_interval_max_seconds,
            )
            next_insert_at = now + interval

        if now >= next_update_check_at:
            flip_one_pending_to_paid(conn)
            next_update_check_at = now + settings.update_check_interval_seconds

        time.sleep(1)

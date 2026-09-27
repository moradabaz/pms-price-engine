import random
import time
from datetime import date, timedelta
from typing import Any

from common import get_logger

from mock_pm_app.bookings_rows import insert_booking
from mock_pm_app.data import CONCEPT_PROFILES, Apartment, Booking
from mock_pm_app.rows import build_live_row, insert_row
from mock_pm_app.settings import MockAppSettings

logger = get_logger(__name__)


def _current_month_bounds(today: date) -> tuple[date, date]:
    start = today.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(days=1)
    return start, end


def _already_has_line_for_period(
    conn: Any, apartment_id: str, cost_definition_id: str, period_start: date
) -> bool:
    """A real PMS records one bill per (apartment, concept, month) — not
    an unbounded, ever-growing ledger. Without this guard, insert_one()
    running for hours keeps adding more lines for the same concept into the
    same still-open billing period, so total cost (and therefore the
    profitability floor) only ever grows the longer the demo has been up,
    regardless of how reasonable any single amount is. Returns True if a
    synthetic line already covers this apartment/concept/period."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM payment_lines
            WHERE apartment_id = %(apartment_id)s
              AND cost_definition_id = %(cost_definition_id)s
              AND billing_period_start = %(period_start)s
              AND source = 'synthetic'
            LIMIT 1
            """,
            {
                "apartment_id": apartment_id,
                "cost_definition_id": cost_definition_id,
                "period_start": period_start,
            },
        )
        return cur.fetchone() is not None


def insert_one(
    conn: Any,
    apartments: list[Apartment],
    rng: random.Random,
    concept_to_cost_definition_id: dict[str, str],
) -> None:
    apartment = rng.choice(apartments)
    profile = rng.choice(CONCEPT_PROFILES)
    period_start, period_end = _current_month_bounds(date.today())
    cost_definition_id = concept_to_cost_definition_id[profile.concept]

    if _already_has_line_for_period(
        conn, apartment.apartment_id, cost_definition_id, period_start
    ):
        logger.info(
            "skipped_duplicate_concept_for_period",
            apartment_id=apartment.apartment_id,
            concept=profile.concept,
        )
        return

    row = build_live_row(
        apartment,
        profile,
        cost_definition_id,
        period_start,
        period_end,
        rng,
    )
    with conn.cursor() as cur:
        event_id = insert_row(cur, row)
    conn.commit()
    logger.info(
        "inserted_payment_line",
        event_id=str(event_id),
        apartment_id=apartment.apartment_id,
        concept=profile.concept,
    )


def insert_one_booking(
    conn: Any, apartments: list[Apartment], rng: random.Random
) -> None:
    # Phase 18 (ADR-0011 backlog #13 prerequisite): keeps producing fresh
    # occupancy changes while the stack is running, same "insert on an
    # interval loop" shape as insert_one() above.
    apartment = rng.choice(apartments)
    check_in = date.today() + timedelta(days=rng.randint(1, 30))
    stay_length = rng.choice([1, 2, 3, 5, 7])
    check_out = check_in + timedelta(days=stay_length)
    booking = Booking(
        apartment_id=apartment.apartment_id,
        check_in=check_in,
        check_out=check_out,
        channel=rng.choice(["airbnb", "booking", "vrbo", "direct"]),
        guests=rng.randint(1, 4),
        revenue_eur=round(rng.uniform(60.0, 220.0) * stay_length, 2),
        status="confirmed",
    )
    with conn.cursor() as cur:
        booking_id = insert_booking(cur, booking)
    conn.commit()
    logger.info(
        "inserted_booking",
        booking_id=str(booking_id),
        apartment_id=apartment.apartment_id,
        check_in=check_in.isoformat(),
        check_out=check_out.isoformat(),
    )


def flip_one_confirmed_to_cancelled(conn: Any) -> None:
    """Cancels one random still-upcoming confirmed booking. Phase 26: the
    only live source of a real UPDATE on bookings.status — without this,
    booking-events.v1 never carries a cancellation with a real updated_at,
    only the seed's already-cancelled rows (updated_at NULL, since they were
    INSERTed as cancelled, never UPDATEd)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT booking_id FROM bookings
            WHERE status = 'confirmed' AND check_in > CURRENT_DATE
            ORDER BY random()
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            return
        (booking_id,) = row
        cur.execute(
            "UPDATE bookings SET status = 'cancelled' WHERE booking_id = %s",
            (booking_id,),
        )
    conn.commit()
    logger.info("cancelled_booking", booking_id=str(booking_id))


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
    conn: Any,
    settings: MockAppSettings,
    apartments: list[Apartment],
    concept_to_cost_definition_id: dict[str, str],
) -> None:
    rng = random.Random()
    next_insert_at = time.monotonic()
    next_update_check_at = time.monotonic() + settings.update_check_interval_seconds
    # Phase 18: its own timer, same insert-interval range as payment lines
    # but independent so the two don't collide/starve each other.
    next_booking_insert_at = time.monotonic()
    # Phase 26: its own timer too, independent of the two above.
    next_cancellation_check_at = (
        time.monotonic() + settings.cancellation_check_interval_seconds
    )

    while True:
        now = time.monotonic()

        if now >= next_insert_at:
            insert_one(conn, apartments, rng, concept_to_cost_definition_id)
            interval = rng.uniform(
                settings.insert_interval_min_seconds,
                settings.insert_interval_max_seconds,
            )
            next_insert_at = now + interval

        if now >= next_booking_insert_at:
            insert_one_booking(conn, apartments, rng)
            interval = rng.uniform(
                settings.insert_interval_min_seconds,
                settings.insert_interval_max_seconds,
            )
            next_booking_insert_at = now + interval

        if now >= next_update_check_at:
            flip_one_pending_to_paid(conn)
            next_update_check_at = now + settings.update_check_interval_seconds

        if now >= next_cancellation_check_at:
            flip_one_confirmed_to_cancelled(conn)
            next_cancellation_check_at = (
                now + settings.cancellation_check_interval_seconds
            )

        time.sleep(1)

from datetime import date

from flink_jobs.models import BookingRow
from flink_jobs.occupancy import avg_guests, occupied_nights


def _booking(check_in, check_out, status="confirmed", guests=2):
    return BookingRow(
        booking_id="00000000-0000-0000-0000-000000000001",
        apartment_id="BCN-001",
        check_in=check_in,
        check_out=check_out,
        channel="airbnb",
        guests=guests,
        revenue_eur=300.0,
        status=status,
    )


def test_no_bookings_returns_zero():
    assert occupied_nights([], date(2026, 6, 1), date(2026, 6, 30)) == 0


def test_single_booking_within_period():
    booking = _booking(date(2026, 6, 10), date(2026, 6, 13))
    # [10, 11, 12] occupied — check_out night itself is not occupied.
    assert occupied_nights([booking], date(2026, 6, 1), date(2026, 6, 30)) == 3


def test_booking_partially_outside_period_only_counts_overlap():
    booking = _booking(date(2026, 5, 28), date(2026, 6, 3))
    # Nights: 28, 29, 30, 31 (May), 1, 2 (June) — only Jun 1-2 fall in period.
    assert occupied_nights([booking], date(2026, 6, 1), date(2026, 6, 30)) == 2


def test_cancelled_booking_excluded():
    booking = _booking(date(2026, 6, 10), date(2026, 6, 13), status="cancelled")
    assert occupied_nights([booking], date(2026, 6, 1), date(2026, 6, 30)) == 0


def test_overlapping_bookings_not_double_counted():
    booking_a = _booking(date(2026, 6, 10), date(2026, 6, 13))
    booking_b = _booking(date(2026, 6, 12), date(2026, 6, 15))
    # Union of nights: 10, 11, 12, 13, 14 -> 5, not 3 + 3 = 6.
    assert (
        occupied_nights([booking_a, booking_b], date(2026, 6, 1), date(2026, 6, 30))
        == 5
    )


def test_booking_entirely_outside_period_counts_zero():
    booking = _booking(date(2026, 5, 1), date(2026, 5, 5))
    assert occupied_nights([booking], date(2026, 6, 1), date(2026, 6, 30)) == 0


def test_avg_guests_no_bookings_returns_zero():
    assert avg_guests([], date(2026, 6, 1), date(2026, 6, 30)) == 0.0


def test_avg_guests_averages_overlapping_confirmed_bookings():
    booking_a = _booking(date(2026, 6, 10), date(2026, 6, 13), guests=2)
    booking_b = _booking(date(2026, 6, 15), date(2026, 6, 18), guests=4)
    assert (
        avg_guests([booking_a, booking_b], date(2026, 6, 1), date(2026, 6, 30)) == 3.0
    )


def test_avg_guests_excludes_cancelled_bookings():
    booking_a = _booking(date(2026, 6, 10), date(2026, 6, 13), guests=2)
    booking_b = _booking(
        date(2026, 6, 15), date(2026, 6, 18), guests=4, status="cancelled"
    )
    result = avg_guests([booking_a, booking_b], date(2026, 6, 1), date(2026, 6, 30))
    assert result == 2.0

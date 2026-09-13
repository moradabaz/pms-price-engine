from collections.abc import Iterable
from datetime import date, timedelta

from flink_jobs.models import BookingRow


def occupied_nights(
    bookings: Iterable[BookingRow], period_start: date, period_end: date
) -> int:
    """Counts distinct nights within [period_start, period_end] (inclusive)
    covered by at least one confirmed booking. Nights occupied by a booking
    are [check_in, check_out) — the night of check_out itself is not
    occupied. Unions across bookings before counting, so two overlapping
    confirmed bookings never double-count a night. Cancelled bookings are
    excluded entirely. Returns 0 for no bookings (or none confirmed)."""
    occupied: set[date] = set()
    for booking in bookings:
        if booking.status != "confirmed":
            continue
        nights = (booking.check_out - booking.check_in).days
        occupied.update(
            booking.check_in + timedelta(days=offset) for offset in range(nights)
        )

    in_period = {night for night in occupied if period_start <= night <= period_end}
    return len(in_period)


def booking_count(
    bookings: Iterable[BookingRow], period_start: date, period_end: date
) -> int:
    """Counts distinct confirmed bookings whose stay overlaps
    [period_start, period_end] at all (Phase 19, ADR-0011 backlog #13) —
    needed by the 'booking' allocation method. Cancelled bookings are
    excluded. Returns 0 for no bookings (or none confirmed/overlapping)."""
    return sum(
        1
        for booking in bookings
        if booking.status == "confirmed"
        and booking.check_in <= period_end
        and booking.check_out > period_start
    )


def avg_guests(
    bookings: Iterable[BookingRow], period_start: date, period_end: date
) -> float:
    """Average guests per confirmed booking overlapping [period_start,
    period_end] (Phase 20, ADR-0013 §2) — needed by
    calculation_base='per_guest'. Same overlap rule as booking_count().
    Returns 0.0 for no matching bookings (never a division by zero)."""
    overlapping = [
        booking
        for booking in bookings
        if booking.status == "confirmed"
        and booking.check_in <= period_end
        and booking.check_out > period_start
    ]
    if not overlapping:
        return 0.0
    return round(sum(b.guests for b in overlapping) / len(overlapping), 2)

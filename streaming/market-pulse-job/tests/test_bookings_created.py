from datetime import date, datetime
from unittest.mock import MagicMock
from uuid import uuid4

from market_pulse_job.bookings_created import (
    BookingCreatedDedupFunction,
    apartment_key,
    booking_id_key,
)
from shared_schemas.booking import Booking


def _booking(*, apartment_id: str = "apt-1", booking_id=None, status: str = "confirmed") -> Booking:
    return Booking(
        booking_id=booking_id or uuid4(),
        apartment_id=apartment_id,
        check_in=date(2026, 11, 1),
        check_out=date(2026, 11, 3),
        channel="airbnb",
        guests=2,
        revenue_eur=200.0,
        status=status,
        created_at=datetime(2026, 10, 1, 12, 0, 0),
    )


def test_booking_id_key_uses_booking_id():
    booking = _booking()
    assert booking_id_key(booking) == str(booking.booking_id)


def test_apartment_key_uses_apartment_id():
    booking = _booking(apartment_id="apt-42")
    assert apartment_key(booking) == "apt-42"


def _dedup_fn_with_state(initial_seen: bool) -> tuple[BookingCreatedDedupFunction, MagicMock]:
    fn = BookingCreatedDedupFunction()
    state = MagicMock()
    state.value.return_value = initial_seen
    fn.seen_state = state
    return fn, state


def test_dedup_passes_first_seen_booking():
    fn, state = _dedup_fn_with_state(initial_seen=False)
    booking = _booking()
    result = list(fn.process_element(booking, ctx=None))
    assert result == [booking]
    state.update.assert_called_once_with(True)


def test_dedup_drops_already_seen_booking():
    fn, state = _dedup_fn_with_state(initial_seen=True)
    booking = _booking()
    result = list(fn.process_element(booking, ctx=None))
    assert result == []
    state.update.assert_not_called()

from typing import Any
from uuid import UUID

from mock_pm_app.data import Booking

INSERT_SQL = """
    INSERT INTO bookings (
        apartment_id, check_in, check_out, channel, guests, revenue_eur, status
    ) VALUES (
        %(apartment_id)s, %(check_in)s, %(check_out)s, %(channel)s,
        %(guests)s, %(revenue_eur)s, %(status)s
    )
    RETURNING booking_id
"""


def insert_booking(cur: Any, booking: Booking) -> UUID:
    cur.execute(
        INSERT_SQL,
        {
            "apartment_id": booking.apartment_id,
            "check_in": booking.check_in,
            "check_out": booking.check_out,
            "channel": booking.channel,
            "guests": booking.guests,
            "revenue_eur": booking.revenue_eur,
            "status": booking.status,
        },
    )
    (booking_id,) = cur.fetchone()
    return booking_id  # type: ignore[no-any-return]

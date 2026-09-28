from datetime import date, datetime
from uuid import uuid4

import pytest
from market_pulse_job.bookings_created import (
    BookingCreatedWindowFunction,
    BookingProfitEnrichmentFunction,
    enriched_apartment_key,
)
from shared_schemas.booking import Booking


def _booking(
    *, apartment_id: str = "apt-1", check_in=date(2026, 11, 1), check_out=date(2026, 11, 4),
    revenue_eur: float = 300.0,
) -> Booking:
    return Booking(
        booking_id=uuid4(),
        apartment_id=apartment_id,
        check_in=check_in,
        check_out=check_out,
        channel="airbnb",
        guests=2,
        revenue_eur=revenue_eur,
        status="confirmed",
        created_at=datetime(2026, 10, 1, 12, 0, 0),
    )


def test_enriched_apartment_key_reads_booking_apartment_id():
    booking = _booking(apartment_id="apt-99")
    enriched = {"booking": booking, "profit_eur": None}
    assert enriched_apartment_key(enriched) == "apt-99"


def _price_decision_item(
    *, fixed_and_allocated_costs_eur: float, per_booking_cost_eur: float, p: float
) -> dict:
    return {
        "Item": {
            "cost_inputs": {
                "M": {
                    "fixed_and_allocated_costs_eur": {"N": str(fixed_and_allocated_costs_eur)},
                    "per_booking_cost_eur": {"N": str(per_booking_cost_eur)},
                    "p": {"N": str(p)},
                }
            }
        }
    }


class _FakeDynamoClient:
    def __init__(self, get_item_response: dict):
        self._response = get_item_response

    def get_item(self, **kwargs):
        return self._response


def test_resolve_profit_matches_manual_formula():
    # 3 nights, matches spec §4.5's nights * fixed_and_allocated + per_booking,
    # then revenue * (1 - p) - total_cost.
    booking = _booking(
        check_in=date(2026, 11, 1), check_out=date(2026, 11, 4), revenue_eur=300.0
    )
    fn = BookingProfitEnrichmentFunction(table_name="price_decision", endpoint_url=None)
    fn.client = _FakeDynamoClient(
        _price_decision_item(
            fixed_and_allocated_costs_eur=10.0, per_booking_cost_eur=5.0, p=0.2
        )
    )
    profit_eur = fn._resolve_profit(booking)
    nights = 3
    total_cost_eur = 10.0 * nights + 5.0
    expected = 300.0 * (1 - 0.2) - total_cost_eur
    assert profit_eur == pytest.approx(expected)
    assert expected == pytest.approx(205.0)


def test_resolve_profit_returns_none_when_no_price_decision():
    booking = _booking()
    fn = BookingProfitEnrichmentFunction(table_name="price_decision", endpoint_url=None)
    fn.client = _FakeDynamoClient({})
    assert fn._resolve_profit(booking) is None


def test_window_excludes_unresolved_bookings_from_average():
    fn = BookingCreatedWindowFunction()
    resolved_a = {"booking": _booking(), "profit_eur": 100.0}
    resolved_b = {"booking": _booking(), "profit_eur": 200.0}
    unresolved = {"booking": _booking(), "profit_eur": None}

    class _FakeWindow:
        start = 0
        end = 300_000

    class _FakeContext:
        def window(self):
            return _FakeWindow()

    (result,) = list(
        fn.process("apt-1", _FakeContext(), [resolved_a, resolved_b, unresolved])
    )
    assert result["booking_count"] == 3
    assert result["profit_sample_count"] == 2
    assert result["unresolved_cost_count"] == 1
    assert result["avg_profit_eur"] == pytest.approx(150.0)


def test_window_avg_profit_is_none_when_nothing_resolved():
    fn = BookingCreatedWindowFunction()
    unresolved = {"booking": _booking(), "profit_eur": None}

    class _FakeWindow:
        start = 0
        end = 300_000

    class _FakeContext:
        def window(self):
            return _FakeWindow()

    (result,) = list(fn.process("apt-1", _FakeContext(), [unresolved]))
    assert result["booking_count"] == 1
    assert result["profit_sample_count"] == 0
    assert result["unresolved_cost_count"] == 1
    assert result["avg_profit_eur"] is None

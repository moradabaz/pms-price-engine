import random
from datetime import date, timedelta
from typing import Any
from uuid import UUID

from mock_pm_app.data import PAYMENT_METHODS, Apartment, ConceptProfile

INSERT_SQL = """
    INSERT INTO payment_lines (
        apartment_id, apartment_reference, concept, cost_type,
        is_shared, allocation_ratio, description,
        supplier_name, supplier_tax_id, invoice_number,
        billing_period_start, billing_period_end,
        amount_gross, vat_rate, currency,
        due_date, payment_date, payment_method, payment_status, source
    ) VALUES (
        %(apartment_id)s, %(apartment_reference)s, %(concept)s, %(cost_type)s,
        %(is_shared)s, %(allocation_ratio)s, %(description)s,
        %(supplier_name)s, %(supplier_tax_id)s, %(invoice_number)s,
        %(billing_period_start)s, %(billing_period_end)s,
        %(amount_gross)s, %(vat_rate)s, %(currency)s,
        %(due_date)s, %(payment_date)s, %(payment_method)s, %(payment_status)s,
        %(source)s
    )
    RETURNING event_id
"""


def _base_row(
    apartment: Apartment,
    profile: ConceptProfile,
    period_start: date,
    period_end: date,
    rng: random.Random,
) -> dict[str, Any]:
    amount = round(rng.uniform(*profile.amount_range), 2)
    label = profile.concept.replace("_", " ").title()
    return {
        "apartment_id": apartment.apartment_id,
        "apartment_reference": apartment.apartment_reference,
        "concept": profile.concept,
        "cost_type": profile.cost_type,
        "is_shared": False,
        "allocation_ratio": None,
        "description": (
            f"{label} — {apartment.apartment_reference} {period_start:%Y-%m}"
        ),
        "supplier_name": None,
        "supplier_tax_id": None,
        "invoice_number": None,
        "billing_period_start": period_start,
        "billing_period_end": period_end,
        "amount_gross": amount,
        "vat_rate": profile.vat_rate,
        "currency": "EUR",
        "source": "synthetic",
    }


def build_historical_row(
    apartment: Apartment,
    profile: ConceptProfile,
    period_start: date,
    period_end: date,
    rng: random.Random,
) -> dict[str, Any]:
    row = _base_row(apartment, profile, period_start, period_end, rng)
    due_date = period_end + timedelta(days=15)
    row.update(
        due_date=due_date,
        payment_date=due_date,
        payment_method=rng.choice(PAYMENT_METHODS),
        payment_status="paid",
    )
    return row


def build_live_row(
    apartment: Apartment,
    profile: ConceptProfile,
    period_start: date,
    period_end: date,
    rng: random.Random,
) -> dict[str, Any]:
    row = _base_row(apartment, profile, period_start, period_end, rng)
    row.update(
        due_date=period_end + timedelta(days=15),
        payment_date=None,
        payment_method=None,
        payment_status="pending",
    )
    return row


def insert_row(cur: Any, row: dict[str, Any]) -> UUID:
    cur.execute(INSERT_SQL, row)
    (event_id,) = cur.fetchone()
    return event_id  # type: ignore[no-any-return]

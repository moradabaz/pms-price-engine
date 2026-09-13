from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/payment_line.v1.json field-for-field.
# Phase 19 (ADR-0012): schema_version 2.0. concept/cost_type/is_shared/
# allocation_ratio removed, replaced by cost_definition_id.


class PaymentLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal["2.0"] = "2.0"

    apartment_id: str
    apartment_reference: str

    cost_definition_id: UUID

    description: str

    supplier_name: str | None = None
    supplier_tax_id: str | None = None

    invoice_number: str | None = None

    billing_period_start: date
    billing_period_end: date

    amount_gross: float = Field(ge=0)
    vat_rate: Literal[0.0, 0.04, 0.10, 0.21]  # type: ignore[valid-type]
    amount_net: float | None = Field(default=None, ge=0)
    currency: Literal["EUR"] = "EUR"

    due_date: date | None = None
    payment_date: date | None = None
    payment_method: Literal["bank_transfer", "direct_debit", "card", "cash"] | None = (
        None
    )
    payment_status: Literal["pending", "paid", "overdue", "disputed"]

    source: Literal["bank_statement", "manual_entry", "synthetic"]
    created_at: datetime
    updated_at: datetime | None = None

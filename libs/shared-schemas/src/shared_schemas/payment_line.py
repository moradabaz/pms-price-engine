from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/payment_line.v1.json field-for-field.


class PaymentLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal["1.0"] = "1.0"

    apartment_id: str
    apartment_reference: str

    concept: Literal[
        "electricity",
        "water",
        "gas",
        "internet",
        "pms_subscription",
        "ota_fee",
        "channel_manager",
        "office_rent",
        "cleaning",
        "maintenance",
        "insurance",
        "community_fee",
        "other",
    ]
    cost_type: Literal["fixed", "variable", "one_time"]

    is_shared: bool = False
    allocation_ratio: float | None = Field(default=None, ge=0, le=1)

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

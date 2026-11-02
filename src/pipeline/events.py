"""Event model(s) flowing through the pipeline."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class OrderEvent(BaseModel):
    """A single order line event.

    This is the unit of data the pipeline ingests. Revenue is derived rather
    than stored, so it can never disagree with quantity x price.
    """

    event_id: str = Field(min_length=1)
    occurred_at: datetime
    product_id: str = Field(min_length=1)
    category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0.0)

    @field_validator("occurred_at")
    @classmethod
    def _naive_to_utc_is_fine(cls, v: datetime) -> datetime:
        # Accept both naive and aware timestamps; the pipeline treats them as a
        # single logical timeline. Normalisation to UTC is the ingester's job.
        return v

    @property
    def revenue(self) -> float:
        return self.quantity * self.unit_price

    @classmethod
    def from_json(cls, raw: str | bytes) -> "OrderEvent":
        return cls.model_validate_json(raw)

    def to_json(self) -> str:
        return self.model_dump_json()

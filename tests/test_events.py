from datetime import datetime

import pytest
from pydantic import ValidationError

from pipeline.events import OrderEvent


def _event(**overrides):
    base = dict(
        event_id="e1",
        occurred_at=datetime(2026, 1, 1, 0, 0, 0),
        product_id="P-1",
        category="books",
        quantity=3,
        unit_price=10.0,
    )
    base.update(overrides)
    return OrderEvent(**base)


def test_revenue_is_quantity_times_price():
    assert _event(quantity=3, unit_price=10.0).revenue == 30.0


def test_quantity_must_be_positive():
    with pytest.raises(ValidationError):
        _event(quantity=0)


def test_negative_price_rejected():
    with pytest.raises(ValidationError):
        _event(unit_price=-1.0)


def test_json_round_trip():
    original = _event()
    restored = OrderEvent.from_json(original.to_json())
    assert restored == original

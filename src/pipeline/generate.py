"""Deterministic synthetic event generator for demos and local runs."""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from .events import OrderEvent

_CATALOG = {
    "electronics": [("E-1001", 299.0), ("E-1002", 59.0), ("E-1003", 999.0)],
    "books": [("B-2001", 14.5), ("B-2002", 29.0)],
    "home": [("H-3001", 42.0), ("H-3002", 120.0), ("H-3003", 8.0)],
}


def generate_events(
    count: int = 500,
    start: datetime | None = None,
    seconds_between: float = 1.5,
    seed: int = 42,
) -> Iterator[OrderEvent]:
    """Yield ``count`` order events on an advancing timeline across categories."""
    rng = random.Random(seed)
    ts = start or datetime(2026, 1, 1, 0, 0, 0)
    for i in range(count):
        category = rng.choice(list(_CATALOG))
        product_id, price = rng.choice(_CATALOG[category])
        yield OrderEvent(
            event_id=f"evt-{i:06d}",
            occurred_at=ts,
            product_id=product_id,
            category=category,
            quantity=rng.randint(1, 5),
            unit_price=price,
        )
        ts = ts + timedelta(seconds=seconds_between)

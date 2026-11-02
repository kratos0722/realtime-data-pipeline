"""Tumbling-window aggregation — the pure processing core of the pipeline.

No Kafka, no database: just events in, aggregated windows out. That is what
makes the interesting logic (windowing, watermarks, lateness) unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


def window_start(ts: datetime, window_seconds: int) -> datetime:
    """Floor a timestamp to the start of its tumbling window.

    Timestamps are bucketed against the Unix epoch so that windows are stable
    and independent of which event happens to arrive first.
    """
    if ts.tzinfo is None:
        anchor = datetime(1970, 1, 1)
    else:
        anchor = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = (ts - anchor).total_seconds()
    bucket = int(elapsed // window_seconds) * window_seconds
    return anchor + timedelta(seconds=bucket)


@dataclass(frozen=True)
class WindowKey:
    window_start: datetime
    category: str
    product_id: str


@dataclass
class Aggregate:
    units: int = 0
    revenue: float = 0.0
    orders: int = 0

    def add(self, quantity: int, revenue: float) -> None:
        self.units += quantity
        self.revenue += revenue
        self.orders += 1


@dataclass
class WindowedRow:
    window_start: datetime
    window_seconds: int
    category: str
    product_id: str
    units: int
    revenue: float
    orders: int


@dataclass
class TumblingWindowAggregator:
    """Accumulates events into fixed-size, non-overlapping time windows.

    ``flush_completed`` emits (and forgets) every window that has fully closed
    relative to a watermark, which lets a streaming runner release results
    incrementally while still tolerating out-of-order events up to the
    watermark's lateness allowance.
    """

    window_seconds: int = 60
    _buckets: dict[WindowKey, Aggregate] = field(default_factory=dict)

    def add(self, *, occurred_at: datetime, category: str, product_id: str,
            quantity: int, revenue: float) -> None:
        key = WindowKey(window_start(occurred_at, self.window_seconds), category, product_id)
        self._buckets.setdefault(key, Aggregate()).add(quantity, revenue)

    def _row(self, key: WindowKey, agg: Aggregate) -> WindowedRow:
        return WindowedRow(
            window_start=key.window_start,
            window_seconds=self.window_seconds,
            category=key.category,
            product_id=key.product_id,
            units=agg.units,
            revenue=round(agg.revenue, 6),
            orders=agg.orders,
        )

    def flush_completed(self, watermark: datetime) -> list[WindowedRow]:
        """Return and remove every window whose end is at or before ``watermark``."""
        completed: list[WindowedRow] = []
        remaining: dict[WindowKey, Aggregate] = {}
        for key, agg in self._buckets.items():
            window_end = key.window_start + timedelta(seconds=self.window_seconds)
            if window_end <= watermark:
                completed.append(self._row(key, agg))
            else:
                remaining[key] = agg
        self._buckets = remaining
        completed.sort(key=lambda r: (r.window_start, r.category, r.product_id))
        return completed

    def drain(self) -> list[WindowedRow]:
        """Return and remove all remaining windows (end-of-stream)."""
        rows = [self._row(k, a) for k, a in self._buckets.items()]
        self._buckets = {}
        rows.sort(key=lambda r: (r.window_start, r.category, r.product_id))
        return rows

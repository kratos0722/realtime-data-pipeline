"""Wires a source -> windowed aggregator -> sink into a streaming loop."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

from .events import OrderEvent
from .sinks import DuckDBSink
from .windows import TumblingWindowAggregator


def run_stream(
    source: Iterable[OrderEvent],
    aggregator: TumblingWindowAggregator,
    sink: DuckDBSink,
    allowed_lateness_seconds: int = 0,
) -> int:
    """Consume events, releasing each window to the sink once it has closed.

    A watermark is tracked as ``max(occurred_at) - allowed_lateness``. When the
    watermark passes a window's end, that window is final and gets written. Any
    windows still open at end-of-stream are drained. Returns the number of
    window rows written.
    """
    written = 0
    max_ts = None

    for event in source:
        aggregator.add(
            occurred_at=event.occurred_at,
            category=event.category,
            product_id=event.product_id,
            quantity=event.quantity,
            revenue=event.revenue,
        )
        if max_ts is None or event.occurred_at > max_ts:
            max_ts = event.occurred_at
        watermark = max_ts - timedelta(seconds=allowed_lateness_seconds)
        written += sink.write(aggregator.flush_completed(watermark))

    written += sink.write(aggregator.drain())
    return written

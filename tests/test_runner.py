from datetime import datetime

from pipeline.events import OrderEvent
from pipeline.runner import run_stream
from pipeline.sinks import DuckDBSink
from pipeline.sources import InMemorySource
from pipeline.windows import TumblingWindowAggregator


def _evt(i, ts, product, category, qty, price):
    return OrderEvent(event_id=f"e{i}", occurred_at=ts, product_id=product,
                      category=category, quantity=qty, unit_price=price)


def test_end_to_end_streaming_aggregation():
    events = [
        _evt(0, datetime(2026, 1, 1, 0, 0, 5), "A", "x", 2, 10.0),   # window 0: +20
        _evt(1, datetime(2026, 1, 1, 0, 0, 40), "A", "x", 1, 10.0),  # window 0: +10
        _evt(2, datetime(2026, 1, 1, 0, 1, 15), "B", "y", 1, 5.0),   # window 1: +5
        _evt(3, datetime(2026, 1, 1, 0, 2, 0), "A", "x", 1, 10.0),   # window 2: +10
    ]
    aggregator = TumblingWindowAggregator(window_seconds=60)
    with DuckDBSink(":memory:") as sink:
        written = run_stream(InMemorySource(events), aggregator, sink)

        assert written == 3  # three distinct (window, product) rows
        assert sink.total_revenue() == 45.0

        top = sink.top_products()
        assert top[0].product_id == "A"
        assert top[0].units == 4
        assert top[0].revenue == 40.0

        windows = sink.revenue_by_window()
        assert [w.window_start for w in windows] == [
            datetime(2026, 1, 1, 0, 0, 0),
            datetime(2026, 1, 1, 0, 1, 0),
            datetime(2026, 1, 1, 0, 2, 0),
        ]
        assert windows[0].revenue == 30.0

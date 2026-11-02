from datetime import datetime

from pipeline.windows import TumblingWindowAggregator, window_start


def test_window_start_floors_to_bucket():
    ts = datetime(2026, 1, 1, 0, 1, 37)
    assert window_start(ts, 60) == datetime(2026, 1, 1, 0, 1, 0)
    assert window_start(ts, 30) == datetime(2026, 1, 1, 0, 1, 30)


def test_aggregator_sums_within_window():
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(occurred_at=datetime(2026, 1, 1, 0, 0, 1), category="books",
            product_id="P-1", quantity=2, revenue=20.0)
    agg.add(occurred_at=datetime(2026, 1, 1, 0, 0, 30), category="books",
            product_id="P-1", quantity=1, revenue=10.0)

    rows = agg.drain()
    assert len(rows) == 1
    row = rows[0]
    assert row.units == 3
    assert row.revenue == 30.0
    assert row.orders == 2
    assert row.window_start == datetime(2026, 1, 1, 0, 0, 0)


def test_flush_completed_releases_only_closed_windows():
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(occurred_at=datetime(2026, 1, 1, 0, 0, 10), category="books",
            product_id="P-1", quantity=1, revenue=5.0)   # window [00:00, 00:01)
    agg.add(occurred_at=datetime(2026, 1, 1, 0, 1, 10), category="books",
            product_id="P-1", quantity=1, revenue=5.0)   # window [00:01, 00:02)

    # Watermark at 00:01:00 closes the first window but not the second.
    completed = agg.flush_completed(datetime(2026, 1, 1, 0, 1, 0))
    assert [r.window_start for r in completed] == [datetime(2026, 1, 1, 0, 0, 0)]

    # The still-open window remains until drained.
    remaining = agg.drain()
    assert [r.window_start for r in remaining] == [datetime(2026, 1, 1, 0, 1, 0)]

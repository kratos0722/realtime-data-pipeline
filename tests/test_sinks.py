from datetime import datetime

from pipeline.sinks import DuckDBSink
from pipeline.windows import WindowedRow


def _row(product_id, revenue, units=1, orders=1, ws=datetime(2026, 1, 1, 0, 0, 0)):
    return WindowedRow(window_start=ws, window_seconds=60, category="books",
                       product_id=product_id, units=units, revenue=revenue, orders=orders)


def test_write_and_query_top_products():
    with DuckDBSink(":memory:") as sink:
        sink.write([_row("P-1", 30.0, units=3), _row("P-2", 10.0, units=1)])
        top = sink.top_products()
        assert [p.product_id for p in top] == ["P-1", "P-2"]
        assert top[0].units == 3
        assert sink.total_revenue() == 40.0


def test_revenue_by_window_groups_and_orders():
    with DuckDBSink(":memory:") as sink:
        sink.write([
            _row("P-1", 10.0, ws=datetime(2026, 1, 1, 0, 0, 0)),
            _row("P-2", 5.0, ws=datetime(2026, 1, 1, 0, 1, 0)),
        ])
        windows = sink.revenue_by_window()
        assert [w.window_start for w in windows] == [
            datetime(2026, 1, 1, 0, 0, 0),
            datetime(2026, 1, 1, 0, 1, 0),
        ]
        assert windows[0].revenue == 10.0


def test_insert_or_replace_is_idempotent():
    with DuckDBSink(":memory:") as sink:
        sink.write([_row("P-1", 30.0, units=3)])
        sink.write([_row("P-1", 30.0, units=3)])  # replay same window identity
        assert sink.total_revenue() == 30.0  # not doubled

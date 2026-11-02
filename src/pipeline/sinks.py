"""DuckDB warehouse sink + query layer."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .windows import WindowedRow

_SCHEMA = """
create table if not exists window_metrics (
    window_start    timestamp not null,
    window_seconds  integer   not null,
    category        varchar   not null,
    product_id      varchar   not null,
    units           bigint    not null,
    revenue         double    not null,
    orders          bigint    not null,
    primary key (window_start, window_seconds, category, product_id)
);
"""


@dataclass
class ProductRevenue:
    product_id: str
    units: int
    revenue: float


@dataclass
class WindowRevenue:
    window_start: object
    revenue: float
    orders: int


class DuckDBSink:
    """Append windowed aggregates to DuckDB and query them back.

    Writes use ``INSERT OR REPLACE`` keyed on the window identity, so replaying
    the same window (e.g. after a restart) is idempotent rather than
    double-counting.
    """

    def __init__(self, database: str = ":memory:") -> None:
        self._con = duckdb.connect(database)
        self._con.execute(_SCHEMA)

    def write(self, rows: list[WindowedRow]) -> int:
        if not rows:
            return 0
        payload = [
            (r.window_start, r.window_seconds, r.category, r.product_id,
             r.units, r.revenue, r.orders)
            for r in rows
        ]
        self._con.executemany(
            "insert or replace into window_metrics values (?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
        return len(payload)

    def top_products(self, limit: int = 5) -> list[ProductRevenue]:
        rows = self._con.execute(
            """
            select product_id, sum(units) as units, sum(revenue) as revenue
              from window_metrics
             group by product_id
             order by revenue desc, product_id
             limit ?
            """,
            [limit],
        ).fetchall()
        return [ProductRevenue(p, int(u), float(rev)) for p, u, rev in rows]

    def revenue_by_window(self) -> list[WindowRevenue]:
        rows = self._con.execute(
            """
            select window_start, sum(revenue) as revenue, sum(orders) as orders
              from window_metrics
             group by window_start
             order by window_start
            """
        ).fetchall()
        return [WindowRevenue(ws, float(rev), int(o)) for ws, rev, o in rows]

    def total_revenue(self) -> float:
        (value,) = self._con.execute("select coalesce(sum(revenue), 0.0) from window_metrics").fetchone()
        return float(value)

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "DuckDBSink":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

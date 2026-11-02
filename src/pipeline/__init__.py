"""Streaming event-analytics pipeline.

Ingest order events (from Kafka or any iterable source), aggregate them into
tumbling time windows, and land the results in a DuckDB analytical warehouse
for querying. The processing core is deliberately free of I/O so it can be
unit-tested without a broker or a database.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"

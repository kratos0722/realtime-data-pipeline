"""Command-line entry point: demo, produce, consume, query."""

from __future__ import annotations

import argparse

from .generate import generate_events
from .runner import run_stream
from .sinks import DuckDBSink
from .sources import InMemorySource, KafkaSource
from .windows import TumblingWindowAggregator


def _print_report(sink: DuckDBSink) -> None:
    print("\nTop products by revenue")
    print("  product    units        revenue")
    for p in sink.top_products():
        print(f"  {p.product_id:<8s} {p.units:>6d} {p.revenue:>14.2f}")
    print("\nRevenue by window")
    for w in sink.revenue_by_window():
        print(f"  {w.window_start}  revenue={w.revenue:>12.2f}  orders={w.orders}")
    print(f"\nTotal revenue: {sink.total_revenue():.2f}")


def cmd_demo(args: argparse.Namespace) -> int:
    events = list(generate_events(count=args.count, seconds_between=args.spacing))
    aggregator = TumblingWindowAggregator(window_seconds=args.window)
    with DuckDBSink(args.db) as sink:
        rows = run_stream(InMemorySource(events), aggregator, sink,
                          allowed_lateness_seconds=args.lateness)
        print(f"Ingested {len(events)} events -> {rows} window rows")
        _print_report(sink)
    return 0


def cmd_produce(args: argparse.Namespace) -> int:
    from .producer import KafkaEventProducer

    producer = KafkaEventProducer(args.topic, bootstrap_servers=args.bootstrap)
    try:
        sent = producer.publish_all(generate_events(count=args.count, seconds_between=args.spacing))
        print(f"Published {sent} events to topic '{args.topic}'")
    finally:
        producer.close()
    return 0


def cmd_consume(args: argparse.Namespace) -> int:
    source = KafkaSource(args.topic, bootstrap_servers=args.bootstrap,
                         group_id=args.group, max_messages=args.max_messages)
    aggregator = TumblingWindowAggregator(window_seconds=args.window)
    with DuckDBSink(args.db) as sink:
        rows = run_stream(source, aggregator, sink, allowed_lateness_seconds=args.lateness)
        print(f"Consumed from '{args.topic}' -> {rows} window rows into {args.db}")
        _print_report(sink)
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    with DuckDBSink(args.db) as sink:
        _print_report(sink)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="Run an in-memory end-to-end demo (no Kafka needed)")
    demo.add_argument("--count", type=int, default=500)
    demo.add_argument("--spacing", type=float, default=1.5, help="seconds between events")
    demo.add_argument("--window", type=int, default=60, help="window size in seconds")
    demo.add_argument("--lateness", type=int, default=0)
    demo.add_argument("--db", default=":memory:")
    demo.set_defaults(func=cmd_demo)

    produce = sub.add_parser("produce", help="Publish synthetic events to Kafka")
    produce.add_argument("--bootstrap", default="localhost:9092")
    produce.add_argument("--topic", default="orders")
    produce.add_argument("--count", type=int, default=500)
    produce.add_argument("--spacing", type=float, default=1.5)
    produce.set_defaults(func=cmd_produce)

    consume = sub.add_parser("consume", help="Consume events from Kafka into the warehouse")
    consume.add_argument("--bootstrap", default="localhost:9092")
    consume.add_argument("--topic", default="orders")
    consume.add_argument("--group", default="pipeline")
    consume.add_argument("--window", type=int, default=60)
    consume.add_argument("--lateness", type=int, default=0)
    consume.add_argument("--max-messages", type=int, default=None, dest="max_messages")
    consume.add_argument("--db", default="warehouse.duckdb")
    consume.set_defaults(func=cmd_consume)

    query = sub.add_parser("query", help="Report metrics from an existing warehouse file")
    query.add_argument("--db", default="warehouse.duckdb")
    query.set_defaults(func=cmd_query)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)

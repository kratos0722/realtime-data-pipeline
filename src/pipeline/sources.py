"""Event sources. The pipeline consumes any iterable of OrderEvent, so the
processing code is identical whether events come from Kafka or a test list."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from .events import OrderEvent


class InMemorySource:
    """Wraps an in-memory collection of events (used in tests and the demo)."""

    def __init__(self, events: Iterable[OrderEvent]) -> None:
        self._events = list(events)

    def __iter__(self) -> Iterator[OrderEvent]:
        return iter(self._events)


class KafkaSource:
    """Consumes JSON-encoded OrderEvents from a Kafka topic.

    ``kafka-python`` is imported lazily so the core pipeline and its tests do
    not require the dependency (or a running broker).
    """

    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092",
                 group_id: str = "pipeline", poll_timeout_ms: int = 1000,
                 max_messages: int | None = None) -> None:
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.poll_timeout_ms = poll_timeout_ms
        self.max_messages = max_messages

    def __iter__(self) -> Iterator[OrderEvent]:
        from kafka import KafkaConsumer  # lazy import

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self.poll_timeout_ms,
        )
        seen = 0
        try:
            for message in consumer:
                yield OrderEvent.from_json(message.value)
                seen += 1
                if self.max_messages is not None and seen >= self.max_messages:
                    break
        finally:
            consumer.close()

"""Kafka producer for publishing OrderEvents (demo / ingestion side)."""

from __future__ import annotations

from collections.abc import Iterable

from .events import OrderEvent


class KafkaEventProducer:
    """Publishes OrderEvents as JSON to a Kafka topic.

    ``kafka-python`` is imported lazily so importing this module never requires
    the optional dependency.
    """

    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092") -> None:
        from kafka import KafkaProducer  # lazy import

        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda e: e.to_json().encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def publish(self, event: OrderEvent) -> None:
        self._producer.send(self.topic, key=event.product_id, value=event)

    def publish_all(self, events: Iterable[OrderEvent]) -> int:
        count = 0
        for event in events:
            self.publish(event)
            count += 1
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.close()

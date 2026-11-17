"""Dead-letter queue: poison events go to a Kafka topic (replayable after a fix)
instead of blocking the pipeline (head-of-line blocking) or vanishing silently."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer

log = logging.getLogger("migrator.dlq")

DLQ_TOPIC = "migrator.dlq"


class DeadLetterQueue:
    def __init__(self, bootstrap: str, topic: str = DLQ_TOPIC):
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "acks": "all"})
        self.count = 0

    def send(self, ev_topic: str, partition: int, offset: int, key: bytes | None,
             value: bytes | None, error: Exception | str) -> None:
        payload = {
            "error": f"{type(error).__name__}: {error}" if isinstance(error, Exception) else error,
            "topic": ev_topic, "partition": partition, "offset": offset,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "key": key.decode() if key else None,
            "value": value.decode() if value else None,
        }
        self.producer.produce(self.topic, key=key, value=json.dumps(payload).encode())
        # flush before the consumer commits the source offset: never lose a poison event
        self.producer.flush(10)
        self.count += 1
        log.error("DLQ <- %s[%d]@%d: %s", ev_topic, partition, offset, payload["error"])

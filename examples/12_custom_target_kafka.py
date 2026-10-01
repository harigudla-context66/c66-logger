"""
Case 12 — a future target: a Kafka stream.

Entries go to one topic (key = log_id), runs to another (key = run_id, so a
compacted topic keeps each run's latest state). Consumers can load them into
logs.log_entry / logs.log_run with the same field names.

Runs anywhere: without C66_EXAMPLE_KAFKA_BOOTSTRAP it uses a stand-in producer
that prints what it would send; with it set (plus confluent-kafka), it produces
to the real topics.

    python examples/12_custom_target_kafka.py
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence

from _settings import TENANT

from c66_logger import AuditLogger, BaseSink, LogEntry, LogRun, register_sink


class KafkaSink(BaseSink):
    target_type = "kafka"

    def __init__(self, *, entry_topic: str = "logs.log_entry", run_topic: str = "logs.log_run",
                 bootstrap_servers: Optional[str] = None, producer: Optional[Any] = None,
                 producer_config: Optional[Dict[str, Any]] = None, flush_timeout: float = 10.0):
        if (bootstrap_servers is None) == (producer is None):
            raise ValueError("KafkaSink needs exactly one of bootstrap_servers or producer")
        self.entry_topic, self.run_topic = entry_topic, run_topic
        self._flush_timeout = flush_timeout
        self._owns_producer = producer is None
        if producer is None:
            from confluent_kafka import Producer  # only needed when not injected

            producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True,
                                 **(producer_config or {})})
        self._producer = producer

    def open(self) -> None:
        topics = self._producer.list_topics(timeout=5).topics
        missing = [t for t in (self.entry_topic, self.run_topic) if t not in topics]
        if missing:
            raise ConnectionError(f"topic(s) not found: {missing}")

    def _send(self, messages: List[tuple]) -> None:
        errors: List[str] = []
        for topic, key, value in messages:
            self._producer.produce(topic, key=key, value=value,
                                   on_delivery=lambda err, _m: errors.append(str(err)) if err else None)
        remaining = self._producer.flush(self._flush_timeout)
        if errors or remaining:
            raise ConnectionError(f"kafka delivery failed: {errors[:3]} ({remaining} undelivered)")  # -> retried

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        self._send([(self.entry_topic, e.log_id, e.to_json()) for e in entries])

    def write_run(self, run: LogRun) -> None:
        self._send([(self.run_topic, run.run_id, run.to_json())])

    def close(self) -> None:
        if self._owns_producer:
            self._producer.flush(self._flush_timeout)


class PrintingProducer:
    """Stand-in for confluent_kafka.Producer so the example runs without a broker."""

    class _Metadata:
        topics = {"logs.log_entry": None, "logs.log_run": None}

    def list_topics(self, timeout):
        return self._Metadata()

    def produce(self, topic, key, value, on_delivery):
        print(f"  -> {topic:<15} key={key[:8]}…  {value[:80]}…")
        on_delivery(None, None)

    def flush(self, timeout):
        return 0


register_sink("kafka", KafkaSink)


def main() -> None:
    bootstrap = os.environ.get("C66_EXAMPLE_KAFKA_BOOTSTRAP")
    connection = {"bootstrap_servers": bootstrap} if bootstrap else {"producer": PrintingProducer()}
    with AuditLogger(**TENANT, target_type="kafka", connection=connection, mode="sync",
                     logger_name="crm_events") as audit:
        with audit.run("event_forwarding"):
            audit.info({"message": "lead converted", "lead_id": "00Q1"},
                       '{"message": "opportunity won", "amount": 25000}')


if __name__ == "__main__":
    main()

"""
KafkaEventSink: publish events to Kafka. ClickHouse reads the topics into
logs.events (docs/sql/clickhouse_events_schema.sql), so services never write to
the database themselves and a slow or unavailable database doesn't touch them.

    category       topic (default)
    telemetry      c66.events.telemetry
    observability  c66.events.observability
    audit          c66.events.audit
    + criticality in policy.alert_criticalities (P1)   also to c66.events.alerts

Message: key = TenantId (one tenant's events stay in order on one partition),
value = the event as one JSON object (Event.to_json(), the logs.events column
names), headers = category, criticality, event name.

Needs ``confluent-kafka`` (``pip install "c66-logger[kafka]"``) unless you pass
your own producer.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from ..exceptions import ConfigurationError, MissingDependencyError, RejectedRecordsError
from .model import CATEGORIES, Event
from .sinks import BaseEventSink

DEFAULT_TOPICS: Dict[str, str] = {
    "telemetry": "c66.events.telemetry",
    "observability": "c66.events.observability",
    "audit": "c66.events.audit",
}
DEFAULT_ALERT_TOPIC = "c66.events.alerts"

# Producer settings used when the sink creates the producer; producer_config overrides them.
PRODUCER_DEFAULTS: Dict[str, Any] = {
    "acks": "all",                # a message counts as sent once all in-sync replicas have it
    "enable.idempotence": True,   # broker-side retries never duplicate or reorder
    "compression.type": "lz4",
    "linger.ms": 50,              # small wait to build larger batches
}


class KafkaEventSink(BaseEventSink):
    """
    Give exactly one of:

    * ``connection``         a producer the host already has (a confluent_kafka.Producer,
                             or anything with ``produce()`` / ``poll()`` / ``flush()``).
                             Never closed by the sink.
    * ``bootstrap_servers``  (+ ``producer_config``: any librdkafka setting, e.g.
                             security.protocol, sasl.*) — the sink creates the producer
                             and flushes it on ``close()``.

    ``topics`` overrides the category -> topic map; ``alert_topic=None`` turns
    alert routing off. ``alert_criticalities`` is set by EventLogger from its policy.
    ``check_topics`` makes ``open()`` fail when a topic doesn't exist.
    """

    target_type = "kafka"

    def __init__(
        self,
        *,
        connection: Any = None,
        bootstrap_servers: Optional[str] = None,
        producer_config: Optional[Mapping[str, Any]] = None,
        topics: Optional[Mapping[str, str]] = None,
        alert_topic: Optional[str] = DEFAULT_ALERT_TOPIC,
        alert_criticalities: Iterable[str] = ("P1",),
        check_topics: bool = True,
        open_timeout: float = 10.0,
        flush_timeout: float = 30.0,
    ):
        if (connection is None) == (not bootstrap_servers):
            raise ConfigurationError("KafkaEventSink needs exactly one of connection or bootstrap_servers")
        unknown = set(topics or {}) - set(CATEGORIES)
        if unknown:
            raise ConfigurationError(f"topics: unknown categories {', '.join(sorted(unknown))}")
        self.topics = {**DEFAULT_TOPICS, **dict(topics or {})}
        self.alert_topic = alert_topic
        self.alert_criticalities = frozenset(alert_criticalities)
        self._check_topics = check_topics
        self._open_timeout = open_timeout
        self._flush_timeout = flush_timeout
        self._owns_producer = connection is None
        if connection is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise MissingDependencyError("kafka", "kafka") from exc
            config = {**PRODUCER_DEFAULTS, **dict(producer_config or {}), "bootstrap.servers": bootstrap_servers}
            self._producer = Producer(config)
        else:
            if producer_config:
                raise ConfigurationError("producer_config is only used with bootstrap_servers")
            if not all(callable(getattr(connection, m, None)) for m in ("produce", "poll", "flush")):
                raise ConfigurationError(
                    f"don't know how to use a {type(connection).__name__} as a Kafka producer: pass a "
                    "confluent_kafka.Producer (or anything with produce/poll/flush), or bootstrap_servers"
                )
            self._producer = connection
        self._lock = threading.Lock()
        # (event_id, topic) already acknowledged in a batch that then failed: skipped when
        # the writer retries that batch, so a retry doesn't publish them twice
        self._delivered: Set[Tuple[str, str]] = set()

    # ---- BaseEventSink ---------------------------------------------------

    def open(self) -> None:
        if not self._check_topics or not callable(getattr(self._producer, "list_topics", None)):
            return
        metadata = self._producer.list_topics(timeout=self._open_timeout)
        wanted = list(self.topics.values()) + ([self.alert_topic] if self.alert_topic else [])
        missing = sorted(t for t in set(wanted) if t not in metadata.topics)
        if missing:
            raise ConfigurationError(
                f"Kafka topic(s) {', '.join(missing)} not found. Create them (deploy/kafka-topics.sh) "
                "or pass check_topics=False if the broker creates topics on first use."
            )

    def write_batch(self, events: Sequence[Event]) -> None:
        if not events:
            return
        with self._lock:
            self._write(events)

    def close(self) -> None:
        if self._owns_producer:
            self._producer.flush(self._flush_timeout)

    # ---- internals -------------------------------------------------------

    def _write(self, events: Sequence[Event]) -> None:
        failed: List[Tuple[Event, str]] = []       # delivery errors: retried
        rejected: List[Tuple[Event, str]] = []     # can't be produced at all: dropped
        pending = 0
        # keep only this batch's acknowledgements (a batch given up on must not leave entries behind)
        batch_ids = {e.event_id for e in events}
        self._delivered = {d for d in self._delivered if d[0] in batch_ids}

        def on_delivery(event: Event, topic: str):
            def callback(err: Any, _msg: Any) -> None:
                if err is None:
                    self._delivered.add((event.event_id, topic))
                else:
                    failed.append((event, f"{topic}: {err}"))
            return callback

        for event in events:
            value = event.to_json().encode("utf-8")
            headers = [("c66.category", event.category.encode()),
                       ("c66.criticality", event.criticality.encode()),
                       ("c66.event_name", event.event_name.encode())]
            for topic in self._topics_for(event):
                if (event.event_id, topic) in self._delivered:
                    continue
                try:
                    self._produce(topic, event, value, headers, on_delivery(event, topic))
                    pending += 1
                except BufferError:
                    raise  # the local queue stayed full: transient, the writer retries the batch
                except Exception as exc:  # message too large, invalid topic, ...: retrying won't help
                    rejected.append((event, f"{topic}: {exc!r}"))
                    break

        remaining = self._producer.flush(self._flush_timeout) if pending else 0
        if failed or remaining:
            reasons = "; ".join(sorted({reason for _, reason in failed}))[:500]
            raise RuntimeError(f"Kafka delivery failed for {len(failed) + (remaining or 0)} message(s): "
                               f"{reasons or 'flush timed out'}")
        self._delivered.clear()  # the whole batch is in Kafka
        if rejected:
            raise RejectedRecordsError(rejected, written=len(events) - len(rejected))

    def _topics_for(self, event: Event) -> List[str]:
        topics = [self.topics[event.category]]
        if self.alert_topic and event.criticality in self.alert_criticalities:
            topics.append(self.alert_topic)
        return topics

    def _produce(self, topic: str, event: Event, value: bytes, headers: list, callback: Any) -> None:
        deadline = time.monotonic() + self._flush_timeout
        while True:
            try:
                self._producer.produce(topic, value=value, key=event.tenant_id.encode(),
                                       headers=headers, on_delivery=callback)
                self._producer.poll(0)  # serve delivery callbacks as we go
                return
            except BufferError:  # the producer's local queue is full: let it drain, then try again
                if time.monotonic() > deadline:
                    raise
                self._producer.poll(0.5)

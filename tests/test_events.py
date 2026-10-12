"""EventLogger, the event model, the criticality / retention policy and the event sinks."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

import pytest

from c66_logger import EventLogger, EventPolicy, MemoryEventSink, continue_trace
from c66_logger.events import Event, flatten_attributes
from c66_logger.events.clickhouse import ClickHouseEventSink, events_ddl, kafka_ingest_ddl
from c66_logger.events.kafka import KafkaEventSink
from c66_logger.events.model import EVENT_COLUMNS
from c66_logger.exceptions import ConfigurationError, InvalidLogInputError

from .conftest import ENV, TENANT

ROOT = Path(__file__).resolve().parents[1]
OTHER_TENANT = "5a1e5f0c-0000-4000-8000-000000000002"


@pytest.fixture
def events():
    logger = EventLogger("pipeline-service", TENANT, ENV, target_type="memory", mode="sync")
    yield logger
    logger.close()


def sent(logger: EventLogger) -> List[Event]:
    logger.flush()
    return list(logger.sink.events)


# ---- model ---------------------------------------------------------------


def test_attributes_are_flat_string_key_values():
    flat = flatten_attributes({
        "pipeline": {"id": 7, "source": {"type": "s3"}},
        "ok": True, "ratio": 0.5, "tags": ["a", "b"], "skip": None,
    })
    assert flat == {"pipeline.id": "7", "pipeline.source.type": "s3", "ok": "true",
                    "ratio": "0.5", "tags": '["a","b"]'}


def test_event_json_uses_the_table_columns(events):
    event = events.observe("pipeline.created", pipeline_id="p-1", body="created")
    data = json.loads(event.to_json())
    assert list(data) == list(EVENT_COLUMNS)
    assert data["TenantId"] == TENANT and data["EventName"] == "pipeline.created"
    assert data["SeverityText"] == "INFO" and data["SeverityNumber"] == 9
    assert data["LogAttributes"]["pipeline_id"] == "p-1"
    assert data["ResourceAttributes"]["service.name"] == "pipeline-service"
    assert data["ResourceAttributes"]["telemetry.sdk.name"] == "c66_logger"
    assert len(data["Timestamp"]) == len("2026-10-12 10:00:00.000000")  # ClickHouse-parsable UTC


# ---- policy ----------------------------------------------------------------


def test_criticality_explicit_then_rule_then_severity():
    policy = EventPolicy.from_dict({"rules": {"pipeline.run.failed": "p1", "ui.*": "P4"}})
    assert policy.criticality_for("pipeline.run.failed", "INFO") == "P1"      # rule (normalized)
    assert policy.criticality_for("pipeline.run.failed", "INFO", "P3") == "P3"  # explicit wins
    assert policy.criticality_for("ui.crash", "CRITICAL") == "P4"             # rule beats severity
    assert policy.criticality_for("pipeline.created", "ERROR") == "P2"        # severity default
    assert policy.criticality_for("pipeline.created", "INFO") == "P4"


def test_retention_defaults():
    policy = EventPolicy()
    assert policy.retention_for("observability", "ERROR", "P1") == 365
    assert policy.retention_for("observability", "INFO", "P4") == 30
    assert policy.retention_for("telemetry", "INFO", "P4") == 90       # category minimum
    assert policy.retention_for("audit", "INFO", "P4") == 365
    assert policy.retention_for("observability", "DEBUG", "P4") == 7   # debug is short-lived
    assert policy.retention_for("audit", "DEBUG", "P4") == 365         # ... except audit


def test_policy_config_is_validated():
    with pytest.raises(ConfigurationError, match="criticality"):
        EventPolicy.from_dict({"rules": {"x.*": "P9"}})
    with pytest.raises(ConfigurationError, match="unknown policy key"):
        EventPolicy.from_dict({"retention": {}})
    with pytest.raises(ConfigurationError, match="retention"):
        EventPolicy.from_dict({"retention_days": {"P1": -1}})
    assert EventPolicy.from_dict({"retention_days": {"P1": 730}}).retention_days["P2"] == 180  # merged


# ---- EventLogger -------------------------------------------------------------


def test_categories_and_helpers(events):
    events.observe("pipeline.created", pipeline_id="p-1")
    events.track("ui.pipeline.opened", page="/pipelines")
    events.audit("pipeline.config.updated", actor="user-9", field="schedule")
    out = sent(events)
    assert [e.category for e in out] == ["observability", "telemetry", "audit"]
    assert out[2].attributes["user.id"] == "user-9"
    assert out[2].retention_days == 365
    with pytest.raises(InvalidLogInputError, match="actor"):
        events.audit("pipeline.deleted", actor="")


def test_bad_input_raises_before_queueing(events):
    for name in ("Pipeline.Created", "pipeline..run", "", "pipeline run", "x" * 201):
        with pytest.raises(InvalidLogInputError):
            events.observe(name)
    with pytest.raises(InvalidLogInputError, match="category"):
        events.emit("pipeline.created", category="metrics")
    with pytest.raises(InvalidLogInputError, match="level"):
        events.emit("pipeline.created", severity="LOUD")
    with pytest.raises(InvalidLogInputError, match="criticality"):
        events.emit("pipeline.created", criticality="P0")
    assert sent(events) == []


def test_caller_and_exception_attributes(events):
    try:
        raise KeyError("missing column")
    except KeyError:
        events.exception("pipeline.step.error", step="transform")
    event = sent(events)[0]
    assert event.severity_text == "ERROR" and event.criticality == "P2"
    assert event.attributes["exception.type"] == "KeyError"
    assert "missing column" in event.attributes["exception.stacktrace"]
    assert event.attributes["code.function.name"] == "test_caller_and_exception_attributes"
    assert event.attributes["code.file.path"].endswith("test_events.py")


def test_attributes_param_for_reserved_or_dotted_keys(events):
    events.observe("pipeline.created", attributes={"category": "finance", "pipeline.owner": "data-team"})
    attrs = sent(events)[0].attributes
    assert attrs["category"] == "finance" and attrs["pipeline.owner"] == "data-team"


def test_span_success_shares_trace_and_records_duration(events):
    with events.span("pipeline.run", pipeline_id="p-1") as run:
        events.observe("pipeline.step.completed", step="load")
        run.set_attribute("rows", 1200)
    out = sent(events)
    assert [e.event_name for e in out] == ["pipeline.run.started", "pipeline.step.completed",
                                           "pipeline.run.completed"]
    assert {e.trace_id for e in out} == {run.trace_id} and {e.span_id for e in out} == {run.span_id}
    assert all(e.attributes["pipeline_id"] == "p-1" for e in out)
    assert out[2].attributes["rows"] == "1200" and "duration_ms" in out[2].attributes
    assert run.status == "completed"
    events.observe("after.span")
    assert sent(events)[-1].trace_id == ""  # context is gone after the block


def test_span_failure_and_cancellation():
    logger = EventLogger("svc", TENANT, ENV, target_type="memory", mode="sync",
                         policy={"rules": {"pipeline.run.failed": "P1"}})
    with pytest.raises(ValueError):
        with logger.span("pipeline.run"):
            raise ValueError("bad row")
    with pytest.raises(KeyboardInterrupt):
        with logger.span("pipeline.run"):
            raise KeyboardInterrupt
    out = sent(logger)
    failed, cancelled = out[1], out[3]
    assert failed.event_name == "pipeline.run.failed" and failed.criticality == "P1"
    assert failed.attributes["exception.message"] == "bad row"
    assert cancelled.event_name == "pipeline.run.cancelled" and cancelled.severity_text == "WARNING"


def test_nested_spans_continue_the_trace(events):
    with events.span("pipeline.run") as outer:
        with events.span("pipeline.step") as inner:
            pass
    assert inner.trace_id == outer.trace_id and inner.span_id != outer.span_id
    assert inner.parent_span_id == outer.span_id
    step_started = sent(events)[1]
    assert step_started.attributes["span.parent_id"] == outer.span_id


def test_continue_trace_from_traceparent(events):
    header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    with continue_trace(header) as ctx:
        events.observe("pipeline.created")
        with events.span("pipeline.run") as run:
            pass
    assert ctx.traceparent == header
    assert sent(events)[0].trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert run.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736" and run.parent_span_id == "00f067aa0ba902b7"
    with continue_trace("garbage") as ctx:
        assert ctx is None


def test_context_reaches_every_bound_logger(events):
    acme = events.bind(tenant_id=OTHER_TENANT, team="ingest")
    with events.context(request_id="r-1"):
        events.observe("a.b")
        acme.observe("c.d")
    out = sent(events)
    assert out[0].attributes["request_id"] == out[1].attributes["request_id"] == "r-1"
    assert out[1].tenant_id == OTHER_TENANT and out[1].attributes["team"] == "ingest"
    assert "team" not in out[0].attributes


def test_context_is_per_thread(events):
    seen = {}

    def worker():
        events.observe("in.thread")
        seen["attrs"] = events.sink.events[-1].attributes

    with events.context(request_id="r-1"):
        t = threading.Thread(target=worker)
        t.start()
        t.join()
    assert "request_id" not in seen["attrs"]


def test_unbound_root_logger_needs_bind():
    root = EventLogger("svc", target_type="memory", mode="sync")
    with pytest.raises(ConfigurationError, match="bind"):
        root.observe("a.b")
    root.bind(tenant_id=TENANT, environment_id=ENV).observe("a.b")
    assert len(sent(root)) == 1
    with pytest.raises(ConfigurationError, match="both"):
        EventLogger("svc", TENANT, target_type="memory")


def test_aemit_in_both_modes():
    for mode in ("sync", "buffered"):
        logger = EventLogger("svc", TENANT, ENV, target_type="memory", mode=mode)

        async def main():
            await logger.aemit("async.event", category="telemetry", n=1)

        asyncio.run(main())
        assert sent(logger)[0].attributes["n"] == "1"
        logger.close()


def test_buffered_mode_delivers_on_flush_and_close():
    logger = EventLogger("svc", TENANT, ENV, target_type="memory", batch_size=10, flush_interval=60)
    for i in range(25):
        logger.observe("bulk.event", i=i)
    assert logger.flush(5)
    assert len(logger.sink.events) == 25
    logger.close()
    assert logger.closed


def test_client_events_are_sanitized(events):
    event = events.track_client_event(
        {"event_name": "ui.button.clicked", "attributes": {"button": "save", "user.id": "spoofed"},
         "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        user_id="real-user", session_id="s-1",
    )
    assert event.category == "telemetry" and event.attributes["user.id"] == "real-user"
    assert event.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert "code.file.path" not in event.attributes  # server code location is meaningless here
    with pytest.raises(InvalidLogInputError, match="unknown field"):
        events.track_client_event({"event_name": "ui.x", "category": "audit"})
    with pytest.raises(InvalidLogInputError, match="severity"):
        events.track_client_event({"event_name": "ui.x", "severity": "CRITICAL"})
    with pytest.raises(InvalidLogInputError, match="at most"):
        events.track_client_event({"event_name": "ui.x", "attributes": {f"k{i}": i for i in range(65)}})
    long = events.track_client_event({"event_name": "ui.x", "attributes": {"v": "x" * 10_000}})
    assert len(long.attributes["v"]) == 4096


# ---- Kafka sink -----------------------------------------------------------------


class FakeProducer:
    """The subset of confluent_kafka.Producer the sink uses."""

    def __init__(self, topics=("c66.events.telemetry", "c66.events.observability", "c66.events.audit",
                               "c66.events.alerts")):
        self.topics = set(topics)
        self.messages: List[dict] = []
        self.fail_topics: set = set()       # delivery fails (transient)
        self.reject_topics: set = set()     # produce() raises (permanent)
        self.buffer_full = 0                # next N produce() calls raise BufferError
        self._callbacks: List[Any] = []

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics={t: object() for t in self.topics})

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        if self.buffer_full:
            self.buffer_full -= 1
            raise BufferError("Local: Queue full")
        if topic in self.reject_topics:
            raise ValueError("Broker: Message size too large")
        self._callbacks.append((topic, value, key, headers, on_delivery))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        callbacks, self._callbacks = self._callbacks, []
        for topic, value, key, headers, on_delivery in callbacks:
            if topic in self.fail_topics:
                on_delivery("Broker: Not enough in-sync replicas", None)
            else:
                self.messages.append(dict(topic=topic, value=json.loads(value), key=key, headers=dict(headers)))
                on_delivery(None, None)
        return 0


def kafka_logger(producer, **kwargs):
    kwargs.setdefault("policy", {"rules": {"pipeline.run.failed": "P1"}})
    return EventLogger("svc", TENANT, ENV, target_type="kafka", connection=producer, mode="sync",
                       retry_backoff=0.01, **kwargs)


def test_kafka_routes_by_category_and_alerts_p1():
    producer = FakeProducer()
    logger = kafka_logger(producer)
    logger.track("ui.page.viewed")
    logger.observe("pipeline.run.failed", severity="ERROR")
    logger.audit("pipeline.deleted", actor="u-1")
    topics = [m["topic"] for m in producer.messages]
    assert topics == ["c66.events.telemetry", "c66.events.observability", "c66.events.alerts",
                      "c66.events.audit"]
    msg = producer.messages[1]
    assert msg["key"] == TENANT.encode()
    assert msg["headers"]["c66.criticality"] == b"P1"
    assert msg["value"]["EventName"] == "pipeline.run.failed"
    logger.close()


def test_kafka_custom_topics_and_alert_criticalities():
    producer = FakeProducer(topics=("ops.events", "c66.events.telemetry", "c66.events.audit", "c66.events.alerts"))
    logger = EventLogger(
        "svc", TENANT, ENV, target_type="kafka", mode="sync",
        connection={"connection": producer, "topics": {"observability": "ops.events"}},
        policy={"alert_criticalities": ["P1", "P2"]},
    )
    logger.observe("pipeline.step.error", severity="ERROR")  # P2 -> alert too
    assert [m["topic"] for m in producer.messages] == ["ops.events", "c66.events.alerts"]


def test_kafka_missing_topics_fail_fast():
    with pytest.raises(ConfigurationError, match="c66.events.alerts"):
        kafka_logger(FakeProducer(topics=("c66.events.telemetry", "c66.events.observability",
                                          "c66.events.audit")))


def test_kafka_retry_does_not_resend_delivered_messages():
    producer = FakeProducer()
    producer.fail_topics = {"c66.events.alerts"}
    logger = kafka_logger(producer, max_retries=1)
    original_flush = producer.flush

    def heal_after_first(timeout=None):  # the alerts topic recovers after the first attempt
        result = original_flush(timeout)
        producer.fail_topics = set()
        return result

    producer.flush = heal_after_first
    logger.observe("pipeline.run.failed", severity="ERROR")
    assert [m["topic"] for m in producer.messages] == ["c66.events.observability", "c66.events.alerts"]


def test_kafka_permanent_failure_is_dropped_not_retried():
    producer = FakeProducer()
    producer.reject_topics = {"c66.events.audit"}
    dropped = []
    logger = kafka_logger(producer, on_drop=lambda record, reason: dropped.append((record, reason)))
    logger.observe("pipeline.created")
    logger.audit("pipeline.deleted", actor="u-1")
    assert [m["topic"] for m in producer.messages] == ["c66.events.observability"]
    assert len(dropped) == 1 and "too large" in dropped[0][1]


def test_kafka_full_buffer_waits_and_retries():
    producer = FakeProducer()
    producer.buffer_full = 2
    logger = kafka_logger(producer)
    logger.observe("pipeline.created")
    assert len(producer.messages) == 1


def test_kafka_config_errors():
    with pytest.raises(ConfigurationError, match="exactly one"):
        KafkaEventSink()
    with pytest.raises(ConfigurationError, match="producer"):
        KafkaEventSink(connection=object())
    with pytest.raises(ConfigurationError, match="unknown categories"):
        KafkaEventSink(connection=FakeProducer(), topics={"metrics": "x"})


# ---- ClickHouse --------------------------------------------------------------------


def test_docs_events_ddl_matches_the_code():
    text = (ROOT / "docs/sql/clickhouse_events_schema.sql").read_text(encoding="utf-8")
    expected = (";\n\n".join(events_ddl()) + ";\n\n-- Kafka ingestion\n\n"
                + ";\n\n".join(kafka_ingest_ddl()) + ";\n")
    assert expected in text, "docs/sql/clickhouse_events_schema.sql is out of date: regenerate it"


def test_ddl_rejects_bad_identifiers_and_settings():
    with pytest.raises(ConfigurationError):
        events_ddl(table="events; DROP TABLE x")
    with pytest.raises(ConfigurationError):
        kafka_ingest_ddl(broker_list="k:9092' , x='")


class FakeClickHouse:
    """The clickhouse_connect calls the sink makes."""

    def __init__(self):
        self.commands: List[str] = []
        self.inserts: List[dict] = []

    def command(self, sql):
        self.commands.append(sql)
        return 1 if sql.startswith("EXISTS") else ""

    def insert(self, table, rows, column_names=None, database=None, settings=None):
        self.inserts.append(dict(table=f"{database}.{table}", rows=rows, columns=column_names, settings=settings))


def test_clickhouse_event_sink_inserts_rows():
    client = FakeClickHouse()
    logger = EventLogger("svc", TENANT, ENV, target_type="clickhouse", mode="sync",
                         connection={"connection": client, "create_tables": True})
    logger.observe("pipeline.created", pipeline_id="p-1")
    assert any("CREATE TABLE IF NOT EXISTS logs.events" in c for c in client.commands)
    insert = client.inserts[0]
    assert insert["table"] == "logs.events" and insert["columns"] == list(EVENT_COLUMNS)
    row = dict(zip(EVENT_COLUMNS, insert["rows"][0]))
    assert str(row["TenantId"]) == TENANT and row["LogAttributes"]["pipeline_id"] == "p-1"
    assert insert["settings"]["insert_deduplication_token"]


def test_memory_sink_helpers():
    sink = MemoryEventSink()
    logger = EventLogger("svc", TENANT, ENV, sink=sink, mode="sync")
    logger.observe("a.b")
    assert sink.names() == ["a.b"]
    sink.clear()
    assert sink.events == []

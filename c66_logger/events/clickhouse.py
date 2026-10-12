"""
ClickHouse storage for events: the logs.events table, the Kafka ingestion that
fills it, and a sink that writes to it directly.

* ``events_ddl()``         logs.events — one row per event, OpenTelemetry column
                           names, key/value attributes in Map columns, daily
                           partitions, per-row retention (TTL on RetentionDays).
* ``kafka_ingest_ddl()``   a Kafka engine table reading the category topics, a
                           materialized view moving rows into logs.events, and
                           logs.events_ingest_errors for messages that don't parse
                           (so one bad message never stops ingestion).
* ``ClickHouseEventSink``  inserts into logs.events directly, without Kafka.

docs/sql/clickhouse_events_schema.sql is generated from these functions.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any, Dict, List, Optional, Sequence

from ..exceptions import ConfigurationError, RejectedRecordsError
from ..sinks.clickhouse import ClickHouseClient, _command, _exists, _insert
from .kafka import DEFAULT_TOPICS
from .model import EVENT_COLUMNS, Event
from .sinks import BaseEventSink

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# columns as they appear in a Kafka message (InsertedAt is filled in by ClickHouse)
_COLUMN_TYPES = """\
    EventId            UUID,
    Timestamp          DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    ObservedTimestamp  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    TenantId           UUID,
    EnvironmentId      UUID,
    Category           LowCardinality(String),
    EventName          LowCardinality(String),
    SeverityText       LowCardinality(String),
    SeverityNumber     UInt8,
    Criticality        LowCardinality(String),
    RetentionDays      UInt16,
    ServiceName        LowCardinality(String),
    Body               String CODEC(ZSTD(1)),
    TraceId            String CODEC(ZSTD(1)),
    SpanId             String CODEC(ZSTD(1)),
    TraceFlags         UInt8,
    ScopeName          LowCardinality(String),
    ScopeVersion       LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes      Map(LowCardinality(String), String) CODEC(ZSTD(1))"""


def _check(*names: str) -> None:
    for name in names:
        if not _IDENT.match(name):
            raise ConfigurationError(f"invalid identifier {name!r}")


def events_ddl(schema: str = "logs", table: str = "events") -> List[str]:
    """
    logs.events.

    * ORDER BY (TenantId, EnvironmentId, Category, EventName, Timestamp, EventId):
      every query is per tenant, usually per category / event and time window.
    * ReplacingMergeTree: a message delivered twice (Kafka is at-least-once)
      collapses to one row on merge; add FINAL to a query that must not see repeats.
    * PARTITION BY day; TTL deletes each row RetentionDays after its Timestamp.
    * No CHECK constraints: a rejected row would stop Kafka ingestion. EventLogger
      validates before sending.
    """
    _check(schema, table)
    return [
        f"CREATE DATABASE IF NOT EXISTS {schema}",
        f"""CREATE TABLE IF NOT EXISTS {schema}.{table}
(
{_COLUMN_TYPES},
    InsertedAt         DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_attr_keys mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_values mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (TenantId, EnvironmentId, Category, EventName, Timestamp, EventId)
TTL toDateTime(Timestamp) + toIntervalDay(RetentionDays)
SETTINGS non_replicated_deduplication_window = 1000""",
    ]


def kafka_ingest_ddl(
    broker_list: str = "kafka:9092",
    topics: Sequence[str] = tuple(DEFAULT_TOPICS.values()),
    group_name: str = "clickhouse-c66-events",
    num_consumers: int = 1,
    schema: str = "logs",
    table: str = "events",
) -> List[str]:
    """Kafka engine table + materialized views: topics -> logs.events, bad messages -> events_ingest_errors."""
    _check(schema, table)
    for value in (broker_list, group_name, *topics):
        if "'" in value or "\\" in value:
            raise ConfigurationError(f"invalid Kafka setting {value!r}")
    return [
        f"""CREATE TABLE IF NOT EXISTS {schema}.{table}_ingest_errors
(
    InsertedAt DateTime64(3, 'UTC') DEFAULT now64(3),
    Topic      LowCardinality(String),
    Partition  UInt64,
    Offset     UInt64,
    Raw        String,
    Error      String
)
ENGINE = MergeTree
ORDER BY InsertedAt
TTL toDateTime(InsertedAt) + toIntervalDay(30)""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.{table}_kafka
(
{_COLUMN_TYPES}
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '{broker_list}',
         kafka_topic_list = '{",".join(topics)}',
         kafka_group_name = '{group_name}',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = {int(num_consumers)},
         kafka_handle_error_mode = 'stream',
         input_format_skip_unknown_fields = 1""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.{table}_kafka_mv TO {schema}.{table} AS
SELECT {", ".join(EVENT_COLUMNS)}
FROM {schema}.{table}_kafka
WHERE length(_error) = 0""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.{table}_kafka_errors_mv TO {schema}.{table}_ingest_errors AS
SELECT _topic AS Topic, _partition AS Partition, _offset AS Offset, _raw_message AS Raw, _error AS Error
FROM {schema}.{table}_kafka
WHERE length(_error) > 0""",
    ]


class ClickHouseEventSink(BaseEventSink):
    """
    Inserts events into logs.events directly — for development and small
    installs without Kafka. Takes the same ``connection`` / ``host`` options as
    the log_entry ClickHouseSink (a clickhouse_connect client, an
    enterprise_connectors connector, a factory, ...). ``create_tables=True``
    runs events_ddl() in ``open()``.
    """

    target_type = "clickhouse"

    def __init__(self, *, connection: Any = None, host: Optional[str] = None, port: Optional[int] = None,
                 username: str = "default", password: str = "", secure: bool = False,
                 client_options: Optional[Dict[str, Any]] = None, schema: str = "logs",
                 table: str = "events", create_tables: bool = False):
        self._ddl = events_ddl(schema, table)  # validates the identifiers
        self.schema, self.table = schema, table
        self._create_tables = create_tables
        self._source = ClickHouseClient(connection=connection, host=host, port=port, username=username,
                                        password=password, secure=secure, client_options=client_options,
                                        owner="ClickHouseEventSink")

    def open(self) -> None:
        with self._source.use() as client:
            if self._create_tables:
                for statement in self._ddl:
                    _command(client, statement)
            if not _exists(client, self.schema, self.table):
                raise ConfigurationError(
                    f"table {self.schema}.{self.table} not found. Apply docs/sql/clickhouse_events_schema.sql "
                    "or pass create_tables=True."
                )

    def write_batch(self, events: Sequence[Event]) -> None:
        if not events:
            return
        rows = []
        for event in events:
            row = event.as_row()
            row["EventId"] = uuid.UUID(row["EventId"])
            row["TenantId"] = uuid.UUID(row["TenantId"])
            row["EnvironmentId"] = uuid.UUID(row["EnvironmentId"])
            rows.append([row[c] for c in EVENT_COLUMNS])
        token = hashlib.sha256(",".join(e.event_id for e in events).encode()).hexdigest()
        try:
            with self._source.use() as client:
                _insert(client, self.schema, self.table, EVENT_COLUMNS, rows, {"insert_deduplication_token": token})
        except Exception as exc:
            if "cannot parse" in str(exc).lower():  # a bad value (Code 27 / 376 ...): retrying won't help
                raise RejectedRecordsError([(e, str(exc).splitlines()[0][:500]) for e in events]) from exc
            raise

    def close(self) -> None:
        self._source.close()

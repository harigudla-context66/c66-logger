-- ClickHouse storage for c66_logger events (EventLogger): logs.events.
--
-- Flow:  service -> EventLogger -> Kafka topics -> logs.events_kafka (Kafka engine)
--                -> logs.events_kafka_mv -> logs.events
--        messages that don't parse -> logs.events_ingest_errors (kept 30 days)
--
-- * Columns follow the OpenTelemetry ClickHouse exporter (otel_logs) plus the c66
--   columns EventId, TenantId, EnvironmentId, Category, Criticality, RetentionDays.
-- * Attributes are key/value pairs in Map columns:
--       SELECT LogAttributes['pipeline_id'] FROM logs.events WHERE EventName = 'pipeline.run.failed'
-- * Each row is deleted RetentionDays after its Timestamp (TTL); EventLogger's
--   policy sets RetentionDays, so retention changes need no DDL change.
-- * Kafka is at-least-once and logs.events is a ReplacingMergeTree: a repeated
--   message collapses to one row on merge (use FINAL where exact counts matter).
--
-- Generated from c66_logger.events.clickhouse.events_ddl() and kafka_ingest_ddl();
-- a test keeps them in sync. Change kafka_broker_list / kafka_group_name for your
-- environment. Without Kafka, apply only the first two statements and use
-- EventLogger(target_type="clickhouse").

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.events
(
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
    LogAttributes      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
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
SETTINGS non_replicated_deduplication_window = 1000;

-- Kafka ingestion

CREATE TABLE IF NOT EXISTS logs.events_ingest_errors
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
TTL toDateTime(InsertedAt) + toIntervalDay(30);

CREATE TABLE IF NOT EXISTS logs.events_kafka
(
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
    LogAttributes      Map(LowCardinality(String), String) CODEC(ZSTD(1))
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'c66.events.telemetry,c66.events.observability,c66.events.audit',
         kafka_group_name = 'clickhouse-c66-events',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream',
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.events_kafka_mv TO logs.events AS
SELECT EventId, Timestamp, ObservedTimestamp, TenantId, EnvironmentId, Category, EventName, SeverityText, SeverityNumber, Criticality, RetentionDays, ServiceName, Body, TraceId, SpanId, TraceFlags, ScopeName, ScopeVersion, ResourceAttributes, LogAttributes
FROM logs.events_kafka
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.events_kafka_errors_mv TO logs.events_ingest_errors AS
SELECT _topic AS Topic, _partition AS Partition, _offset AS Offset, _raw_message AS Raw, _error AS Error
FROM logs.events_kafka
WHERE length(_error) > 0;

# Events: telemetry, observability and audit (`EventLogger`)

`EventLogger` is the generic logger for the platform. Every service uses it:
data pipelines, the chatbot, platform services, and the UI through its backend.
It sends one kind of record, an **event**:

- shaped like an **OpenTelemetry log record**
- with **key/value attributes** instead of fixed columns
- published **asynchronously** to **Kafka**
- stored in **ClickHouse**, which is built for time-ordered events

`AuditLogger` (`logs.log_entry` / `logs.log_run`) is unchanged and still works.
New code should use `EventLogger`.

## Why

From the architecture review (Palvinder Singh, with Rahul Neekhra, Prajakta Sangore and Hari Krishna Gudla):

| Requirement | How it is met |
| --- | --- |
| Generic | One `emit(event_name, ...)` for every service. Event types are names (`pipeline.run.failed`), not tables or columns. |
| Scalable | Services only publish to Kafka. ClickHouse ingests from Kafka at its own pace; topics are partitioned by tenant. |
| Asynchronous | Calls queue the event and return. A background thread sends batches. `await events.aemit(...)` works in asyncio code. |
| Key-value model | A small fixed envelope plus `LogAttributes Map(String, String)`. New data never needs a new column. |
| Time-event database | ClickHouse `logs.events`: daily partitions, ordered by tenant, category, event and time, with per-row TTL. |
| Message queue | Kafka, with one topic per category plus an alerts topic. |
| Criticality | P1–P4, separate from severity. It drives alert routing and retention. |
| Telemetry and observability | `category` is `telemetry` (what users do), `observability` (what the system does) or `audit` (who changed what). UI events come in through `track_client_event()`. |

## Flow

```
service code ── EventLogger.emit() ──► in-process queue ──► background thread
                                                               │ batches, retries
                                                               ▼
                                     Kafka  c66.events.telemetry | .observability | .audit
                                            c66.events.alerts  (P1 copies, for alerting)
                                                               │
                         ClickHouse  logs.events_kafka (Kafka engine)
                                     └─ logs.events_kafka_mv ──► logs.events
                                     └─ bad messages ─────────► logs.events_ingest_errors
```

For development or small installs without Kafka, `target_type="clickhouse"`
writes to `logs.events` directly. Unit tests use `target_type="memory"`.

## The event

The fields follow the OpenTelemetry Logs data model and the column names of the
OpenTelemetry ClickHouse exporter (`otel_logs`), plus the c66 columns:

| Column | Meaning |
| --- | --- |
| `EventId` | UUID; repeats of the same event collapse into one row |
| `Timestamp`, `ObservedTimestamp` | when it happened / when it was recorded (UTC, ns) |
| `TenantId`, `EnvironmentId` | whose event it is; every query filters on these |
| `Category` | `telemetry` \| `observability` \| `audit` |
| `EventName` | dotted lower case: `pipeline.run.failed`, `ui.button.clicked` |
| `SeverityText`, `SeverityNumber` | DEBUG 5, INFO 9, WARNING 13, ERROR 17, CRITICAL 21 (OTel numbers) |
| `Criticality`, `RetentionDays` | from the policy (below) |
| `ServiceName`, `ResourceAttributes` | the sending process: service, version, host, pid, SDK |
| `Body` | optional human-readable text |
| `TraceId`, `SpanId`, `TraceFlags` | W3C trace context, which links the UI, services and pipeline steps |
| `ScopeName`, `ScopeVersion` | `c66_logger` and its version |
| `LogAttributes` | **everything else, as key/value pairs** |

Attribute keys follow OpenTelemetry semantic conventions where one exists:
`exception.type`, `exception.message`, `exception.stacktrace`,
`code.file.path`, `code.function.name`, `code.line.number`, `thread.id`,
`user.id`, `session.id`. Nested data is flattened:
`{"source": {"type": "s3"}}` becomes `source.type = "s3"`.

## Criticality and retention (proposed defaults)

**These are proposals.** Every value can be changed in config without changing
code or DDL, and they are to be confirmed with Palvinder.

Criticality is decided in this order:
1. `criticality=` on the call
2. the first matching rule (event-name globs)
3. the severity default

| Severity | Default criticality |
| --- | --- |
| CRITICAL | P1 |
| ERROR | P2 |
| WARNING | P3 |
| INFO, DEBUG | P4 |

| Criticality | Meaning | Kept |
| --- | --- | --- |
| P1 | needs someone now; also sent to `c66.events.alerts` | 365 days |
| P2 | needs attention today | 180 days |
| P3 | worth reviewing | 90 days |
| P4 | routine | 30 days |

Category minimums: audit 365 days, telemetry 90 days. DEBUG events are kept 7
days, except audit. The days are stored per row (`RetentionDays`) and the
table's TTL reads them, so a policy change applies to new events with no DDL
change.

```python
policy = {
    "rules": {"pipeline.run.failed": "P1", "pipeline.step.error": "P2", "ui.*": "P4"},
    "retention_days": {"P1": 730},          # merged over the defaults
    "alert_criticalities": ["P1"],
}
```

## Using it

```bash
pip install "c66-logger[kafka] @ git+https://github.com/harigudla-context66/c66-logger.git"
```

```python
from c66_logger import EventLogger

events = EventLogger(
    "pipeline-service", TENANT_ID, ENVIRONMENT_ID,
    target_type="kafka", connection={"bootstrap_servers": "kafka:9092"},
    policy=policy, service_version="1.4.0",
)

events.observe("pipeline.created", pipeline_id=pid, source={"type": "s3"})
events.audit("pipeline.config.updated", actor=user_id, pipeline_id=pid, field="schedule")
events.track("ui.export.downloaded", user_id=user_id)        # telemetry from server code

with events.span("pipeline.run", pipeline_id=pid) as run:     # pipeline.run.started
    events.observe("pipeline.step.completed", step="load", rows=1200)
    run.set_attribute("rows_loaded", 1200)
                                                              # pipeline.run.completed / .failed / .cancelled
try:
    ...
except Exception:
    events.exception("pipeline.step.error", step="transform")  # ERROR + exception.* attributes

events.close()     # at shutdown: delivers what's queued
```

Other features:

- **Multi-tenant:** create one logger without tenant ids, then call `events.bind(tenant_id=..., environment_id=...)` per tenant. Bound loggers share the producer and the background thread.
- **Request context:** `with events.context(request_id=rid): ...` adds attributes to every event in the block, from any logger. Context is kept per thread or asyncio task.
- **asyncio:** `await events.aemit("x.y", ...)`.
- **Short-lived processes:** use `mode="sync"` (CLIs, serverless).
- **Lost events:** `on_drop=callback(event, reason)` receives events that could not be delivered after retries. Use it to write a dead-letter file.
- **Existing OpenTelemetry tracing:** if a service already uses the OpenTelemetry SDK, events pick up the active span's ids.

### UI telemetry

The browser posts events to a backend endpoint. That endpoint takes tenant,
environment and user from the **authenticated session**, never from the payload:

```python
@app.post("/telemetry")
def telemetry(payload: dict, session=Depends(current_session)):
    events.bind(tenant_id=session.tenant_id, environment_id=session.environment_id) \
          .track_client_event(payload, user_id=session.user_id, session_id=session.id)
```

The payload looks like this:

```json
{"event_name": "ui.button.clicked", "timestamp": "...", "severity": "INFO",
 "attributes": {...}, "traceparent": "00-..."}
```

The category is always telemetry. The client can't set criticality. Attributes are limited to 64 and values to 4,096 characters.

To link a click to the backend work it triggers, send `traceparent` with the
request and wrap the handler in `with continue_trace(request.headers.get("traceparent")):`.

## Setting up Kafka and ClickHouse

1. **Topics:** `deploy/kafka-topics.sh` creates `c66.events.{telemetry,observability,audit,alerts}`. Set `BOOTSTRAP`, `PARTITIONS` and `REPLICATION_FACTOR` as needed. Kafka keeps messages 7 days, which is only a buffer.
2. **ClickHouse:** apply [sql/clickhouse_events_schema.sql](sql/clickhouse_events_schema.sql). Set `kafka_broker_list` and `kafka_group_name` for your environment first.
3. **Local stack:** run `docker compose -f deploy/docker-compose.events.yml up -d`, then `python examples/21_events_pipeline.py --kafka localhost:29092`.

Example queries:

```sql
-- failed pipeline runs this week, with the error
SELECT Timestamp, LogAttributes['pipeline_id'] AS pipeline, LogAttributes['exception.message'] AS error
FROM logs.events
WHERE TenantId = {tenant:UUID} AND EventName = 'pipeline.run.failed' AND Timestamp > now() - INTERVAL 7 DAY
ORDER BY Timestamp DESC;

-- everything that happened in one trace (UI click -> API -> pipeline steps)
SELECT Timestamp, ServiceName, EventName, SeverityText FROM logs.events
WHERE TenantId = {tenant:UUID} AND TraceId = {trace:String} ORDER BY Timestamp;

-- P1/P2 counts per day and category
SELECT toDate(Timestamp) AS day, Category, Criticality, count() FROM logs.events
WHERE TenantId = {tenant:UUID} AND Criticality IN ('P1', 'P2') GROUP BY ALL ORDER BY day;
```

## Delivery guarantees

- **At-least-once.** A retried batch can repeat a message. On retry, the Kafka sink skips messages already acknowledged. `logs.events` is a ReplacingMergeTree keyed on `EventId`, so any repeats collapse on merge; use `FINAL` where exact counts matter.
- **The producer is set up for safety:** `acks=all`, idempotence on, lz4 compression.
- **Retries and drops:** a batch that keeps failing is retried 5 times with backoff, then handed to `on_drop`. A message Kafka refuses outright (too large) is dropped straight away. When the in-process queue is full (100,000 events by default), new events are dropped through `on_drop`.
- **Bad messages:** a message ClickHouse can't parse goes to `logs.events_ingest_errors` and never stops ingestion.

## Not done yet

- A JavaScript client for the UI. The backend endpoint above is ready for it.
- Spill to local disk when the queue is full or Kafka is down for a long time. Today those events go to `on_drop`.
- Alert consumers (paging, Slack) on `c66.events.alerts`.
- Moving `AuditLogger` and the chatbot `Telemetry` class onto events.
- Getting Kafka connections from c66-data-connection-layer once it has a Kafka connector. Today you pass a `confluent_kafka.Producer` or `bootstrap_servers`.

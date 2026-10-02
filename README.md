# c66_logger

Multi-tenant logging for Python packages. It writes log entries to
`log_entry` and run lifecycles to `log_run`, in **Postgres or ClickHouse**
(MongoDB too). It uses the connections your application already has: pass
the connection object, pool or factory your connection library hands out
(c66-data-connection-layer's `manager.get("logs_db")`, `get_pg_conn()`,
`get_ch_client()`). c66_logger never looks up
credentials and never closes what it didn't open.

| Target | Tables | Pass as `connection` | Install |
| --- | --- | --- | --- |
| `postgres` | existing `logs.log_entry` / `logs.log_run` ([DDL](docs/sql/logs_schema.sql)) | enterprise_connectors connector (`manager.get("logs_db")`), psycopg2/psycopg connection, pool, factory (`get_pg_conn`), SQLAlchemy Engine, or `{"dsn": ...}` | `pip install "c66-logger[postgres]"` |
| `clickhouse` | `logs.log_entry` / `logs.log_run`, same columns ([DDL](docs/sql/clickhouse_logs_schema.sql)) | clickhouse_connect client, factory (`get_ch_client`), clickhouse_driver Client, or `{"host": ...}` | `pip install "c66-logger[clickhouse]"` |
| `mongodb` | collections `log_entry` / `log_run`, same fields | `{"client": MongoClient}` or `{"uri": ...}` | `pip install "c66-logger[mongodb]"` |
| `memory` | in-memory, for unit tests of host packages | — | (no extra) |
| `s3`, `kafka` | planned — plug in via `register_sink()` (examples 11, 12) | — | — |

The core package has **no runtime dependencies**. Importing it loads no
database driver and configures no logging.

c66-chatbot's current logging calls (`log_event`, `log_llm_call`, … `log_audit_event`)
all have a same-signature replacement in `c66_logger.Telemetry`. See
[docs/chatbot-log-mapping.md](docs/chatbot-log-mapping.md).

## Documentation

- [Implementation guide](docs/implementation-guide.md) — step-by-step setup with the `salesforce` tenant
- [Architecture](docs/architecture.md) — design decisions, component map, column mapping, open items
- [Data-flow diagram](docs/architecture-diagram.svg) ([interactive page](docs/architecture-diagram.html))
- [c66-chatbot log mapping](docs/chatbot-log-mapping.md) — every current log call, where it goes now, dashboard queries
- [Table DDL](docs/sql/logs_schema.sql) — Postgres `logs.log_entry` / `logs.log_run`; [ClickHouse DDL](docs/sql/clickhouse_logs_schema.sql)
- [Examples](examples/README.md) — 17 runnable use cases

## Quick start

The tables must already exist ([docs/sql/logs_schema.sql](docs/sql/logs_schema.sql)).
c66_logger doesn't create or alter them.

```python
from c66_logger import AuditLogger

audit = AuditLogger(
    tenant_id="5a1e5f0c-0000-4000-8000-000000000001",       # must exist in app.tenant
    environment_id="e0000000-0000-4000-8000-000000000001",  # must exist in app.tenant_environment
    target_type="postgres",
    connection={"dsn": "postgresql://user:pass@host:5432/appdb"},
    logger_name="order_service",
)

with audit.run("order_import", metadata={"file": "orders.csv"}):     # 1 logs.log_run row
    audit.info("import started")                                       # logs.log_entry rows,
    audit.info({"message": "order created", "order_id": 123})          # run_id filled in
    audit.warning({"message": "payment retry", "order_id": 123, "attempt": 2})

audit.close()   # or `with AuditLogger(...) as audit:`
```

With the connections your app already has:

```python
audit = AuditLogger(tenant_id=..., environment_id=..., target_type="postgres", connection=get_pg_conn)
audit = AuditLogger(tenant_id=..., environment_id=..., target_type="clickhouse", connection=get_ch_client())
```

MongoDB: `target_type="mongodb", connection={"uri": "mongodb://...", "database": "logs"}`.

One process serving many tenants: create one logger without tenant ids and
`bind()` per tenant. They share one connection and one writer thread:

```python
root = AuditLogger(target_type="clickhouse", connection=get_ch_client)
acme = root.bind(tenant_id=ACME_ID, environment_id=ACME_PROD)
```

## How calls map onto the tables

**`logs.log_entry`: one row per logged item.**

| Column | Filled from |
| --- | --- |
| `log_id` | generated UUID (or the item's `log_id`) |
| `tenant_id`, `environment_id` | the logger |
| `run_id`, `accelerator_id`, `use_case_id`, `tenant_user_id`, `correlation_id` | logger defaults < `audit.run()` / `audit.context()` < `log(..., run_id=...)` < a key in the item |
| `logger_name` | the logger's `logger_name` (or the item's) |
| `level`, `level_no` | the method (`info`, `error`, …) or the item's `level`; only the 5 levels the CHECK allows |
| `message` | the item's `message`, else `message=` on the call, else a plain one-line string. **Required.** |
| `module`, `func_name`, `pathname`, `line_no` | where `log()` was called (or the `logging` call site) |
| `process_id`, `process_name`, `thread_id`, `thread_name` | the calling process and thread |
| `exception_type`, `exception_message`, `exception_traceback` | `audit.exception(...)`, `exc_info=`, or a failed `audit.run()` |
| `metadata_json` | every other key in the item |
| `logged_at` | now (UTC), or the item's `logged_at` for imports |
| `created_at` | database default |

**`logs.log_run`: one row per run.** Inserted as `Running` when the run starts, then updated with `status`, `ended_at`, `duration_ms`, `error_summary`, `metadata_json` and `updated_at` when it ends:

```python
with audit.run("nightly_sync") as run:   # finishes -> Completed
    ...                                  # raises -> ERROR entry + Failed (re-raised)
                                         # Ctrl-C / cancelled -> Cancelled
run = audit.start_run("export")          # or explicitly
audit.end_run(run, "Completed", metadata={"rows": 5000})
```

## Input formats: one or many items per call

```python
audit.info("sync started")                                          # plain message
audit.info({"message": "order created", "order_id": 1})             # dict
audit.info({"message": "a"}, {"message": "b"})                      # several items
audit.info([{"message": "a"}, {"message": "b"}])                    # list of dicts
audit.info('[{"message": "a"}, {"message": "b"}]')                  # JSON array (or object, or JSON Lines)
audit.info("message,order_id\ncreated,1\npaid,1")                   # CSV with a header row
audit.info({"order_id": 1}, {"order_id": 2}, message="bulk update") # message= as default
```

`log()` returns how many rows it created. Invalid input raises
`InvalidLogInputError` **before** anything is queued. That covers a missing
message, a level the table doesn't allow, and an id that isn't a UUID.

## Connections

`connection=` takes either a dict of options or **the object your connection library hands out**.

**postgres** (plain DB-API: psycopg2 or psycopg 3):

| You pass | c66_logger does |
| --- | --- |
| a c66-data-connection-layer connector, `manager.get("logs_db")` | `with connector.connection() as conn` per write: borrows from the library's pool and gives it back. **Recommended** |
| a factory, e.g. `get_pg_conn` | calls it per write and `close()`s the result. A pooled proxy's `close()` returns it to its pool |
| a pool with `getconn()`/`putconn()` (psycopg2.pool, psycopg_pool) | borrows per write, gives it back |
| a SQLAlchemy `Engine` | `raw_connection()` per write, returned to its pool |
| one connection object | uses it for its lifetime, one write at a time, and commits after each write. Give the logger its own connection, not one in the middle of your transaction |
| `{"dsn": ...}` or `{"host", "database", "user", "password", "port"}` | opens its own small pool (`pool_size`, default 4); closes it on `close()` |

Options: `schema` (default `logs`), `entry_table` (`log_entry`), `run_table` (`log_run`).

With [c66-data-connection-layer](https://github.com/context66/c66-data-connection-layer)
(`enterprise_connectors`, Postgres only so far), the app owns the login and the pool;
c66_logger only borrows:

```python
from enterprise_connectors import ConnectorManager

manager = ConnectorManager.from_yaml("connections.yaml")      # once, at startup
audit = AuditLogger(target_type="postgres", connection=manager.get("logs_db"))
...
audit.close()      # flushes; leaves the connector open
manager.close()    # the app closes its connections
```

Size the connection's pool for the logger too: buffered mode borrows one
connection at a time for entries, plus one per run start/end. If the pool is
exhausted, a write waits up to the pool's `checkout_timeout` and is then retried.
See [example 17](examples/17_data_connection_layer.py).

**clickhouse**:

| You pass | c66_logger does |
| --- | --- |
| a clickhouse_connect client | shares it, one call at a time. Create it with `autogenerate_session_id=False` if your threads use it too |
| a factory, e.g. `get_ch_client` | calls it once, reuses the client |
| a clickhouse_driver `Client` | uses `execute()` |
| `{"host", "port", "username", "password", "secure"}` | creates its own client; closes it on `close()` |

Options: `schema` (ClickHouse database, default `logs`), `entry_table`, `run_table`,
and `create_tables=True` to run the DDL at startup (development).

**mongodb**: exactly one of `uri` or `client` (an existing `MongoClient`),
plus `database`. Optional: `entry_collection`, `run_collection`,
`create_indexes`, `client_options`.

Nothing you pass in is ever closed by c66_logger. What it created itself is closed by `audit.close()`.

## c66-chatbot's logging calls

```python
from c66_logger import AuditLogger, Telemetry

telemetry = Telemetry(
    AuditLogger(target_type="clickhouse", connection=get_ch_client),
    resolve_tenant=lookup_tenant_ids,           # client_name -> (tenant_id, environment_id)
    resolve_user=lookup_tenant_user_id,         # optional
    system_tenant=(PLATFORM_TENANT_ID, PLATFORM_ENV_ID),
    cost_fn=_calc_cost_split, model_fn=PROVIDER_MODEL.get,
    environment=APP_ENVIRONMENT, prompt_version=PROMPT_VERSION,
)
telemetry.log_llm_call(request_id, client, 1, "routing", "anthropic", 1200, 40)   # same arguments as today
telemetry.log_event(client, "/api/chat", question, retrieval_mode, request_id=request_id, latency_ms=5300)
```

Each `/api/chat` request becomes one `log_run` (`chat_request`), and its LLM
calls, graph queries and retrievals become `log_entry` rows inside it.
Ingestion jobs are runs too. Feedback, eval runs, deletions and audit events
are entries. Every old column is kept in `metadata_json` under its old name.
The full map is in [docs/chatbot-log-mapping.md](docs/chatbot-log-mapping.md).

## Delivery and failures

| Setting | Default | Meaning |
| --- | --- | --- |
| `mode` | `"buffered"` | `log()` enqueues and returns; one background thread writes entries. `"sync"` writes before returning |
| `batch_size` / `flush_interval` | 500 / 2.0 s | write when either is reached, or on `flush()` / `close()` / `end_run()` |
| `max_queue_size` | 50,000 | beyond this, new entries are dropped (`reason="queue_full"`) |
| `max_retries` / `retry_backoff` | 3 / 0.5 s | transient failures retried with exponential backoff (capped at 5 s) |
| `on_drop` | log a warning | `callable(entry, reason)` for entries that could not be written |

- **Retries never duplicate.** Postgres: entries use `ON CONFLICT (log_id) DO NOTHING` and runs use `ON CONFLICT (run_id) DO UPDATE`. ClickHouse: a per-batch `insert_deduplication_token` plus ReplacingMergeTree. Runs are row versions there; read them `FINAL`.
- **Rows Postgres rejects are dropped, not retried.** This covers an FK to an unknown run or user, or a CHECK violation. Only the offending rows go to `on_drop`; the rest of the batch is written.
- **Run writes are synchronous.** Entries that reference a run can never arrive before it. If a run can't be written after retries, `start_run()` raises `RunWriteError`.

## Using it inside another package

- Create loggers once at startup and reuse them; they're thread-safe. For many tenants, `bind()` per tenant off one root logger (example 13).
- Loggers share no module-level state.
- Under a forking server (gunicorn, uWSGI), create the logger in each worker, after the fork.
- `audit.context(correlation_id=...)` covers the current thread or asyncio task. Pass `run_id=run.run_id` explicitly to work you hand to other threads.
- Short-lived processes (CLI, serverless) can use `mode="sync"`.
- Unit-test with `target_type="memory", mode="sync"`, then assert on `audit.sink.entries` and `audit.sink.runs`.
- Existing `logging` calls: `logging.getLogger("my_pkg").addHandler(AuditLogHandler(audit))`.
- c66_logger's own diagnostics go to the `c66_logger` logger, which has only a `NullHandler` by default.

## Adding a target (S3, Kafka, …)

Subclass `BaseSink` and implement `write_batch(entries)` and `write_run(run)`,
plus `open()` and `close()` if you need them. Then call
`register_sink("kafka", KafkaSink)`. `LogEntry.to_json()` and `LogRun.to_json()`
give ready-to-ship JSON with the table's field names. See examples 11 and 12.

## Tests

```bash
pip install -e ".[dev]"
pytest                                                          # unit tests, embedded ClickHouse (chdb), mongomock
C66_TEST_POSTGRES_DSN=postgresql://u:p@localhost/postgres pytest  # + real Postgres (needs CREATE DATABASE;
                                                                #   applies the exact DDL to a throwaway DB)
C66_TEST_CLICKHOUSE_HOST=localhost pytest                       # + a real ClickHouse server
C66_TEST_MONGO_URI=mongodb://localhost:27017 pytest             # Mongo tests on a real server
```

Without a server, the ClickHouse tests run on chdb, an embedded ClickHouse
engine. Rows are serialized by clickhouse_connect's own insert code.

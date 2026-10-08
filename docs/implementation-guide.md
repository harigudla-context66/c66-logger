# c66_logger v0.4 Implementation Guide — Salesforce Example

_Last updated 2026-10-01 · See also: [architecture.md](architecture.md) · [chatbot-log-mapping.md](chatbot-log-mapping.md) · [data-flow diagram](architecture-diagram.html) · [examples](../examples/README.md)_

## Overview

c66_logger writes two things for a tenant. A **run** is one row in `logs.log_run`: a job, a sync or an import, from start to finish. **Log entries** are rows in `logs.log_entry`, linked to the run by `run_id`. The code that creates the logger passes the tenant, the environment and the connection. The package never looks up credentials and never creates the tables. This guide uses the **salesforce** tenant on Postgres. ClickHouse and MongoDB differ only in `target_type` and `connection`; the differences are noted where they matter.

## Step 0 — the tables

Both tables must already exist with the DDL in [sql/logs_schema.sql](sql/logs_schema.sql) (ClickHouse: [sql/clickhouse_logs_schema.sql](sql/clickhouse_logs_schema.sql), which has the same columns and no foreign keys). They have foreign keys to `app.tenant`, `app.tenant_environment`, `app.accelerator`, `app.business_use_case` and `app.tenant_user`. Every id you log must exist there, or Postgres rejects the row.

| You need | From |
| --- | --- |
| `tenant_id` | `app.tenant` (salesforce's row) |
| `environment_id` | `app.tenant_environment` (e.g. prod) |
| `use_case_id`, `accelerator_id`, `tenant_user_id` (optional) | `app.business_use_case`, `app.accelerator`, `app.tenant_user` |

The database user needs `INSERT` on `logs.log_entry`, and `INSERT` and `UPDATE` on `logs.log_run`. On ClickHouse, `INSERT` and `SELECT` on both tables.

## Step 1 — install

```bash
pip install -e "/path/to/c66_logger[postgres]"     # psycopg2 (psycopg 3 also works)
pip install -e "/path/to/c66_logger[clickhouse]"   # clickhouse-connect
pip install -e "/path/to/c66_logger[mongodb]"      # pymongo 4
```

## Step 2 — create the logger

```python
from c66_logger import AuditLogger

audit = AuditLogger(
    tenant_id="5a1e5f0c-0000-4000-8000-000000000001",       # salesforce, app.tenant
    environment_id="e0000000-0000-4000-8000-000000000001",  # prod, app.tenant_environment
    target_type="postgres",
    connection={"dsn": "postgresql://sf_user:sf_pass@db-host:5432/appdb"},
    logger_name="order_service",                            # log_entry.logger_name
    use_case_id="0c0c0c0c-0000-4000-8000-000000000001",     # optional default for every row
)
```

Construction connects and checks that both tables exist. Bad credentials or a missing table raise here, not on the first log call. Create the logger once at startup and reuse it.

**Using the connection library.** Pass whatever it returns as `connection=`. c66_logger uses it and never closes it:

```python
from enterprise_connectors import ConnectorManager  # c66-data-connection-layer

manager = ConnectorManager.from_yaml("connections.yaml")   # the app's connections, once at startup
audit = AuditLogger(tenant_id=..., environment_id=..., target_type="postgres", connection=manager.get("logs_db"))
audit = AuditLogger(tenant_id=..., environment_id=..., target_type="clickhouse", connection=manager.get("logs_ch"))
```

Install the connection library from its repository (dev branch for now):
`pip install "enterprise-connectors[postgres,clickhouse,yaml] @ git+https://github.com/context66/c66-data-connection-layer@dev"`.

| Target | `connection=` (one of) | Other options (in a dict, with `"connection": obj`) |
| --- | --- | --- |
| `postgres` | c66-data-connection-layer connector (`manager.get(name)`) · DB-API connection · pool (`getconn`/`putconn`) · SQLAlchemy Engine · zero-arg factory · `{"dsn": ...}` · `{"host", "database", "user", "password", "port"}` | `schema` (`logs`), `entry_table`, `run_table`, `pool_size` |
| `clickhouse` | c66-data-connection-layer connector (`manager.get(name)`) · `clickhouse_connect` client · `clickhouse_driver` Client · zero-arg factory · `{"host", "port", "username", "password", "secure"}` | `schema` (`logs`), `entry_table`, `run_table`, `create_tables` (dev only) |
| `mongodb` | `{"uri": ...}` · `{"client": ...}`, plus `database` | `entry_collection`, `run_collection`, `create_indexes` |

A plain DB-API connection is shared behind a lock and committed after every write, so give the logger its own connection, not one that carries your app's transaction. A factory or a pool is the better fit for a web app.

**Many tenants in one process.** Create one logger without ids and `bind()` one per tenant. They share the connection and the writer thread:

```python
root = AuditLogger(target_type="postgres", connection=get_pg_conn, logger_name="order_service")
salesforce = root.bind(tenant_id=SF_TENANT_ID, environment_id=SF_PROD_ENV_ID)
root.close()   # at shutdown; flushes for every bound logger
```

## Step 3 — record a run

```python
with audit.run("order_import", metadata={"file": "orders_0923.csv"}) as run:
    audit.info("import started")
    audit.info({"message": "order created", "order_id": 1001})
# -> log_run: Running when the block starts; Completed, ended_at and duration_ms when it ends
```

| Block outcome | `log_run.status` | Also |
| --- | --- | --- |
| finishes | Completed | — |
| raises an Exception | Failed | an ERROR entry with the traceback; `error_summary` = "Type: message"; the exception is re-raised |
| Ctrl-C, task cancelled, SystemExit | Cancelled | re-raised |

Without a `with` block, call `run = audit.start_run("export")` and then `audit.end_run(run, "Completed", metadata={"rows": 5000})`. Entries logged inside the block get `run_id` automatically. That works for the current thread and asyncio task only; for work handed to other threads, pass `run_id=run.run_id`.

## Step 4 — log entries

```python
audit.info("sync started")                                          # plain one-line message
audit.info({"message": "order created", "order_id": 1001})          # other keys -> metadata_json
audit.warning({"message": "a"}, {"message": "b"})                   # several items, one row each
audit.info('[{"message": "a"}, {"message": "b"}]')                  # JSON array / object / JSON Lines
audit.info("message,order_id\ncreated,1\npaid,1")                   # CSV with a header row
audit.info({"order_id": 1}, {"order_id": 2}, message="bulk update") # message= as the default
try:
    charge(order)
except PaymentError:
    audit.exception({"message": "payment failed", "order_id": 1001})  # exception_* columns
```

| `log_entry` column | Filled from |
| --- | --- |
| `message` | the item's `message`, else `message=`, else a plain string. **Required** |
| `level`, `level_no` | the method (`info`, `error`, …) or the item's `level`: DEBUG, INFO, WARNING, ERROR or CRITICAL only |
| `logger_name` | the logger's `logger_name`, or the item's |
| `module`, `func_name`, `pathname`, `line_no` | where the log call was made |
| `process_*`, `thread_*` | the calling process and thread |
| `exception_*` | `audit.exception()`, `exc_info=`, or a failed run |
| `metadata_json` | every other key in the item |
| `logged_at` | now (UTC), or the item's `logged_at` for imports |

If any item is invalid, nothing from that call is written and `InvalidLogInputError` names the bad record.

## Step 5 — ids and correlation

```python
with audit.context(correlation_id=request_id, tenant_user_id=user_id):
    audit.info("request received")               # both ids on every row in the block
audit.info("scoring started", accelerator_id=accel_id)             # per call
audit.info({"message": "event", "correlation_id": "..."})          # per item
```

When the same id is set at more than one level, the more specific one wins: logger default, then `run()` / `context()`, then the call, then the item.

## Step 6 — embed it in your package

```python
# order_service/audit.py
def build_audit_logger(tenant_id, environment_id, log_target) -> AuditLogger:
    return AuditLogger(tenant_id=tenant_id, environment_id=environment_id,
                       target_type=log_target["target_type"], connection=log_target["connection"],
                       logger_name="order_service")
```

- **Close on shutdown.** `audit.close()` writes what's queued and releases the connection pool.
- **Forking servers.** Under gunicorn or uWSGI, create the logger in each worker, after the fork.
- **Short-lived processes.** Use `mode="sync"` so nothing waits on a background thread.
- **Existing `logging` calls.** `logging.getLogger("order_service").addHandler(AuditLogHandler(audit))` fills the location, process, thread and exception columns from the `LogRecord`. Pass fields with `extra={"audit": {...}}`.
- **Unit tests.** Use `AuditLogger(..., target_type="memory", mode="sync")`, then assert on `audit.sink.entries` and `audit.sink.runs`.
- **Work that's already finished.** `audit.record_run("nightly_export", status="Completed", started_at=..., ended_at=..., metadata={...})` writes a run in one call.

## Step 7 — c66-chatbot's logging calls

`Telemetry` has the chatbot's nine helpers with the same names and arguments, so `app_common.py` swaps them in and no call site changes:

```python
from c66_logger import AuditLogger, Telemetry

telemetry = Telemetry(
    AuditLogger(target_type="clickhouse", connection=get_ch_client),   # or "postgres", get_pg_conn
    resolve_tenant=lookup_tenant_ids,           # client_name -> (tenant_id, environment_id)
    system_tenant=(PLATFORM_TENANT_ID, PLATFORM_ENV_ID),
    cost_fn=_calc_cost_split, model_fn=PROVIDER_MODEL.get,
    environment=APP_ENVIRONMENT, prompt_version=PROMPT_VERSION,
)
log_event = telemetry.log_event
log_llm_call = telemetry.log_llm_call
# ... and the other seven
```

A chat request becomes one `log_run` (`run_type = chat_request`, `run_id` = `request_id`) with its LLM calls, graph queries and retrieval as `log_entry` rows in it. The full field map is in [chatbot-log-mapping.md](chatbot-log-mapping.md); [example 16](../examples/16_chatbot_telemetry.py) runs it end to end.

## Verify it worked

```sql
SELECT run_type, status, duration_ms, error_summary FROM logs.log_run
WHERE tenant_id = '5a1e5f0c-0000-4000-8000-000000000001' ORDER BY started_at DESC LIMIT 5;

SELECT level, message, func_name, line_no, metadata_json FROM logs.log_entry
WHERE run_id = '<run_id>' ORDER BY logged_at;

-- ClickHouse: read runs with FINAL (one row version per status change)
SELECT run_type, status, duration_ms FROM logs.log_run FINAL WHERE run_id = '<run_id>';
```

The package's tests apply this exact DDL to a throwaway Postgres 16 database. Set `C66_TEST_POSTGRES_DSN` to run them there. They cover runs, entries, FK rejections, retries, upserts and every connection style. The ClickHouse tests run on embedded ClickHouse (chdb), or on a server when `C66_TEST_CLICKHOUSE_HOST` is set. 149 pass.

## Troubleshooting

| Error / symptom | Cause | Fix |
| --- | --- | --- |
| `ConfigurationError: table(s) logs.log_run ... not found` | tables missing, or a different schema | apply `sql/logs_schema.sql`, or pass `schema=` |
| `ConfigurationError: tenant_id must be a UUID` | a name was passed instead of the id | use the UUID from `app.tenant` |
| `InvalidLogInputError: record 1: 'message' is required` | item without `message` | add it, or pass `message=` |
| `InvalidLogInputError: level must be one of ...` | e.g. `AUDIT` | use one of the five levels |
| Warning `target rejected N record(s)`, rows missing | FK violation: `run_id`, `tenant_user_id`, `use_case_id` etc. not in its parent table | fix the id. Only those rows were dropped; capture them with `on_drop=` |
| `RunWriteError` from `start_run()` / `audit.run()` | database unreachable after retries, or the tenant/environment isn't in `app.*` | check the connection and ids |
| Rows not visible yet | buffered: written every 2 s or 500 rows | `audit.flush()`, or `mode="sync"` |
| `ConfigurationError: don't know how to use a ... as a Postgres/ClickHouse connection` | `connection=` is something c66_logger can't use (e.g. a cursor, or a client for the other database) | pass the connection, pool, engine, client or factory itself |
| `ConfigurationError: this AuditLogger has no tenant_id/environment_id` | logging on a root logger created without ids | log on `root.bind(tenant_id=..., environment_id=...)` |
| ClickHouse: several `log_run` rows per run | one version per status change; merged later | query with `FINAL` |
| ClickHouse: `target rejected N record(s)` | a level or status outside the CHECK | fix the value; ClickHouse has no FKs, so unknown ids are accepted |

## Complete flow

![c66_logger v0.4 data flow](architecture-diagram.svg)

```mermaid
sequenceDiagram
    participant Host as Host package
    participant Audit as AuditLogger
    participant Writer as BufferedWriter<br/>(bg thread)
    participant Sink as PostgresSink
    participant Run as logs.log_run
    participant Entry as logs.log_entry

    Host->>Audit: AuditLogger(tenant_id, environment_id, target_type, connection)
    Audit->>Sink: open(): connect, check both tables exist

    Host->>Audit: with audit.run("order_import")
    Audit->>Sink: write_run(status=Running)
    Sink->>Run: INSERT ... ON CONFLICT (run_id) DO UPDATE

    Host->>Audit: info(dict, JSON, CSV, ...)
    Audit->>Audit: one LogEntry per item (ids, caller, thread, exception)
    Audit->>Writer: submit(entries)
    Audit-->>Host: returns immediately

    loop batch of 500 OR 2s OR flush()
        Writer->>Sink: write_batch(entries)
        Sink->>Entry: INSERT ... ON CONFLICT (log_id) DO NOTHING
        alt FK / CHECK violation
            Sink->>Entry: retry row by row, reject only the bad rows (on_drop)
        else connection error
            Writer->>Writer: retry with backoff, then on_drop
        end
    end

    Host->>Audit: block ends (or raises)
    Audit->>Writer: flush()
    Audit->>Sink: write_run(Completed / Failed / Cancelled, ended_at, duration_ms)
    Sink->>Run: UPDATE via upsert
```

# c66_logger User Guide

c66_logger writes audit logs for a tenant to two tables, `logs.log_run` and `logs.log_entry`, in Postgres, ClickHouse or MongoDB. It uses the connections your application already has, and it is built to be embedded in other Python packages.

This guide goes from installation to every feature, with code you can copy. Read Parts 1 to 4 in order to get started. The later parts can be read as you need them.

**Contents**

- Part 1. Concepts
- Part 2. Installation
- Part 3. Preparing the database tables
- Part 4. Quick start
- Part 5. Connecting through c66-data-connection-layer (recommended)
- Part 6. Other ways to connect
- Part 7. Creating and closing a logger
- Part 8. The logging functions
- Part 9. What you can pass to a logging function
- Part 10. Ids and context
- Part 11. Runs
- Part 12. Many tenants in one process
- Part 13. Using Python's standard `logging`
- Part 14. Delivery, retries and failures
- Part 15. Telemetry (c66-chatbot's logging calls)
- Part 16. Testing code that logs
- Part 17. Embedding c66_logger in your own package
- Part 18. Adding a new target
- Part 19. Querying the logs
- Part 20. Errors and troubleshooting
- Part 21. API reference

---

> **New in 0.5:** for telemetry, observability and audit events (OpenTelemetry-shaped,
> key/value attributes, sent through Kafka to ClickHouse) use `EventLogger`. See
> [events.md](events.md). This guide covers `AuditLogger`, which is unchanged.

## Part 1. Concepts

Five ideas cover the whole library.

| Concept | What it is | Where it is stored |
| --- | --- | --- |
| **Log entry** | One event: a message, a level, and any extra fields | One row in `logs.log_entry` |
| **Run** | One piece of work from start to finish: a job, a sync, an import, a chat request | One row in `logs.log_run`, with status `Running`, then `Completed`, `Failed` or `Cancelled` |
| **Tenant** | The customer the log belongs to, as `tenant_id` + `environment_id` (UUIDs) | On every row |
| **Target** | Where logs are written: `postgres`, `clickhouse`, `mongodb`, or `memory` for tests | Chosen with `target_type` |
| **Logger** | An `AuditLogger` object for one tenant and one target | In your code |

Entries logged inside a run carry the run's `run_id`, so you can see everything that happened in one job.

```mermaid
flowchart LR
    app["Your code<br/>audit.info(...)"] --> logger["AuditLogger<br/>validates, queues"]
    logger -- "background thread,<br/>batches" --> target["Target<br/>Postgres / ClickHouse / MongoDB"]
    conn["Your connection<br/>(c66-data-connection-layer,<br/>pool, engine, ...)"] -. "borrowed per write" .-> target
    subgraph target_tables["logs schema"]
        run["log_run"]
        entry["log_entry"]
    end
    target --> target_tables
```

What c66_logger does **not** do:

- It never creates or changes your tables. They must exist first (Part 3).
- It never reads credentials or environment variables. Your application passes the connection.
- It never closes a connection you gave it.
- It never configures Python logging or prints anything unless you ask.

---

## Part 2. Installation

c66_logger needs Python 3.9 or later. The core has no dependencies. Each target's database driver is an optional extra, so you install only what you use.

| Extra | Installs | For target |
| --- | --- | --- |
| `postgres` | `psycopg2-binary` (psycopg 3 also works) | `postgres` |
| `clickhouse` | `clickhouse-connect` | `clickhouse` |
| `mongodb` | `pymongo` | `mongodb` |
| `all` | all three | all |

### 2.1 From Git

```bash
pip install "c66-logger[postgres] @ git+https://github.com/harigudla-context66/c66-logger.git"
```

### 2.2 From a local checkout

```bash
pip install -e "/path/to/c66_logger[postgres,clickhouse]"
```

Keep the quotes. In zsh (the macOS default shell), square brackets without quotes cause `zsh: no matches found`.

### 2.3 With c66-data-connection-layer

Part 5 shows how to use c66_logger with c66-data-connection-layer (`enterprise_connectors`). Install it next to c66_logger, with the extras for your databases, plus `python-dotenv` if you keep passwords in a `.env` file:

```bash
pip install "enterprise-connectors[postgres,clickhouse,yaml] @ git+https://github.com/context66/c66-data-connection-layer@dev"
pip install python-dotenv
```

When c66_logger gets its connections from this library, it doesn't need its own driver extras; the connection layer brings them.

### 2.4 Check the installation

```bash
python -c "import c66_logger; print(c66_logger.__version__, c66_logger.available_targets())"
```

Expected: `0.4.2 ['clickhouse', 'memory', 'mongodb', 'postgres']`.

Importing c66_logger loads no database drivers. A driver is imported only when a logger for that target is created. If it's missing, you get `MissingDependencyError` with the `pip install` command to run.

---

## Part 3. Preparing the database tables

c66_logger only inserts rows (and updates runs). Whoever owns the database creates the tables once.

### 3.1 Postgres

Apply [docs/sql/logs_schema.sql](sql/logs_schema.sql). It creates `logs.log_run` and `logs.log_entry` with indexes, CHECK constraints and foreign keys to the platform's `app.*` tables:

| Column | Foreign key to |
| --- | --- |
| `tenant_id` | `app.tenant` |
| `environment_id` | `app.tenant_environment` |
| `accelerator_id` | `app.accelerator` |
| `use_case_id` | `app.business_use_case` |
| `tenant_user_id` | `app.tenant_user` |
| `log_entry.run_id` | `logs.log_run` |

Every id you log must exist in those tables, or Postgres rejects the row.

```bash
psql "$DSN" -v ON_ERROR_STOP=1 -f docs/sql/logs_schema.sql
```

For local development without the real `app.*` tables, apply the stub first. It creates minimal `app.*` tables with demo tenants. Never run it against a real database.

```bash
psql "$DSN" -v ON_ERROR_STOP=1 -f tests/sql/app_stub.sql -f docs/sql/logs_schema.sql
```

Permissions for the logging user: `INSERT` on `logs.log_entry`; `INSERT` and `UPDATE` on `logs.log_run`.

### 3.2 ClickHouse

Apply [docs/sql/clickhouse_logs_schema.sql](sql/clickhouse_logs_schema.sql):

```bash
clickhouse-client --multiquery < docs/sql/clickhouse_logs_schema.sql
```

The columns have the same names as in Postgres. The differences come from how ClickHouse works:

- There are no foreign keys, so ids aren't checked against `app.*`.
- The level and status CHECK constraints are kept.
- Both tables are `ReplacingMergeTree`, so a retried batch isn't stored twice.
- Starting and ending a run writes two row versions. Read runs with `FINAL` to get the latest (Part 19).
- `metadata_json` is a string holding JSON.

For local development only, the logger can create the tables itself with `create_tables=True` (Part 6.2).

Permissions: `INSERT` and `SELECT` on both tables.

### 3.3 MongoDB

Nothing to prepare. c66_logger writes to collections named `log_entry` and `log_run` (field names match the table columns) and creates their indexes by default.

---

## Part 4. Quick start

### 4.1 Without a database

The `memory` target keeps logs in memory. It's the fastest way to see how the library behaves:

```python
from c66_logger import AuditLogger

TENANT_ID = "5a1e5f0c-0000-4000-8000-000000000001"
ENVIRONMENT_ID = "e0000000-0000-4000-8000-000000000001"

with AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="memory", mode="sync",
                 logger_name="order_service") as audit:
    with audit.run("order_import", metadata={"file": "orders.csv"}) as run:
        audit.info("import started")
        audit.info({"message": "order created", "order_id": 1001})

    for entry in audit.sink.entries:
        print(entry.level, entry.message, entry.metadata_json, entry.run_id == run.run_id)
    print(audit.sink.runs[run.run_id].status)
```

Output:

```
INFO import started {} True
INFO order created {'order_id': 1001} True
Completed
```

### 4.2 With Postgres

The same code, writing to Postgres. Only `target_type` and `connection` change:

```python
from c66_logger import AuditLogger

with AuditLogger(
    tenant_id="5a1e5f0c-0000-4000-8000-000000000001",
    environment_id="e0000000-0000-4000-8000-000000000001",
    target_type="postgres",
    connection={"dsn": "postgresql://postgres@localhost:5432/c66_demo"},
    logger_name="order_service",
) as audit:
    with audit.run("order_import", metadata={"file": "orders.csv"}):
        audit.info("import started")
        audit.info({"message": "order created", "order_id": 1001})
```

This leaves one row in `logs.log_run` (status `Completed`) and two in `logs.log_entry`. In an application, get the connection from c66-data-connection-layer instead of a DSN (Part 5).

---

## Part 5. Connecting through c66-data-connection-layer (recommended)

c66-data-connection-layer (`enterprise_connectors`) is the platform's connection library. It reads connection settings from a YAML file, keeps passwords out of code, and manages connection pools. c66_logger takes one of its connectors and borrows a pooled connection for each write.

Why this is the recommended setup:

- **No passwords in the logger.** The connection layer holds them; c66_logger only gets the connector.
- **One pool per database for the whole app.** The logger shares the app's pool instead of opening its own.
- **Borrow and return.** Each write takes a connection from the pool and gives it back.
- **The app stays in control.** The app opens and closes connections; c66_logger never closes the connector.

### 5.1 Step 1: describe the databases in connections.yaml

Each entry is a named connection. Values come from environment variables through `env://` references. Secrets **must** be references; the library refuses a plain password.

```yaml
# connections.yaml
connections:
  logs_db:                                     # Postgres
    type: postgres
    config:
      host: env://LOGS_PG_HOST
      port: env://LOGS_PG_PORT
      database: env://LOGS_PG_DATABASE
      application_name: order_service-logs     # shows up in pg_stat_activity
    credentials:
      username: env://LOGS_PG_USER
      password: env://LOGS_PG_PASSWORD
    pool:
      max_size: 5

  logs_ch:                                     # ClickHouse
    type: clickhouse
    config:
      host: env://LOGS_CH_HOST
      port: env://LOGS_CH_PORT                 # 8123 = HTTP, 8443 = HTTPS / ClickHouse Cloud
      secure: env://LOGS_CH_SECURE
      client_name: order_service-logs          # shows up in system.query_log
    credentials:
      username: env://LOGS_CH_USER
      password: env://LOGS_CH_PASSWORD
    pool:
      max_size: 4
```

Besides `env://NAME`, a secret can be `awssm://SECRET_ID#json_key` (AWS Secrets Manager, with the library's `aws` extra).

### 5.2 Step 2: put the values in .env

```dotenv
# .env  (never commit this file; add ".env" to .gitignore)
LOGS_PG_HOST=localhost
LOGS_PG_PORT=5432
LOGS_PG_DATABASE=c66_demo
LOGS_PG_USER=logs_writer
LOGS_PG_PASSWORD=...

LOGS_CH_HOST=localhost
LOGS_CH_PORT=8123
LOGS_CH_SECURE=false
LOGS_CH_USER=logs_writer
LOGS_CH_PASSWORD=...
```

A password must not be empty: the library treats an empty value as unset. If your local server doesn't check passwords, any placeholder works.

### 5.3 Step 3: load the connections and create the logger

```python
from dotenv import load_dotenv
from enterprise_connectors import ConnectorManager
from c66_logger import AuditLogger

load_dotenv()                                              # .env -> environment variables
manager = ConnectorManager.from_yaml("connections.yaml")   # once, at app startup

audit = AuditLogger(
    tenant_id=TENANT_ID,
    environment_id=ENVIRONMENT_ID,
    target_type="postgres",
    connection=manager.get("logs_db"),                     # the connector, not a password
    logger_name="order_service",
)

with audit.run("order_import"):
    audit.info("import started")

# at app shutdown, in this order:
audit.close()      # writes what's queued; leaves the connector open
manager.close()    # the app closes its own connections
```

For ClickHouse, change two words:

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="clickhouse",
                    connection=manager.get("logs_ch"), logger_name="order_service")
```

### 5.4 What happens behind the scenes

1. `ConnectorManager.from_yaml(...)` checks each entry but opens no connection and reads no secret yet.
2. `manager.get("logs_db")` returns the connector. Every call returns the same object, so the whole app shares one pool.
3. When the `AuditLogger` is created, it borrows a connection once to check that the log tables exist. If they don't, it raises `ConfigurationError` straight away.
4. Each write runs `with connector.connection() as conn:`, so the connection goes back to the pool when the write ends.
5. `env://` values are read when a connection is opened, so a rotated password is picked up.

### 5.5 Sizing the pool

The logger shares the pool with the rest of your app. In the default buffered mode it uses at most one connection at a time for entries, plus one briefly when a run starts or ends. Add 1 or 2 to the pool size you'd need without logging. If the pool is busy, a write waits for the pool's `checkout_timeout` and is then retried (Part 14).

### 5.6 Checking the pool after logging

```python
for stats in manager.pool_manager.stats().values():
    print(f"{stats.total} open, {stats.in_use} in use")   # in_use is 0 when everything was returned
```

Working examples: [examples/19_connections_yaml.py](../examples/19_connections_yaml.py) (Postgres and ClickHouse) and [examples/connections.yaml](../examples/connections.yaml).

---

## Part 6. Other ways to connect

`connection=` takes either a dict of options, or the object your connection library hands out. c66_logger works out which kind of object it is.

### 6.1 Postgres

| You pass | What c66_logger does |
| --- | --- |
| A c66-data-connection-layer connector, `manager.get("logs_db")` | Borrows a connection per write and returns it (recommended, Part 5) |
| A factory function, such as `get_pg_conn` | Calls it per write and closes the result. For a pooled proxy, `close()` returns it to its pool |
| A pool with `getconn()` / `putconn()` (`psycopg2.pool`, `psycopg_pool`) | Borrows per write and returns it |
| A SQLAlchemy `Engine` | Uses `raw_connection()` per write, returned to the engine's pool |
| One connection object | Uses it for the logger's lifetime, one write at a time, and commits after each write |
| `{"dsn": "postgresql://..."}` | Opens its own small pool (`pool_size`, default 4) and closes it on `close()` |
| `{"host": ..., "database": ..., "user": ..., "password": ..., "port": 5432}` | The same, from parts |

```python
# a DSN: the logger owns a small pool
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres",
                    connection={"dsn": "postgresql://user:pass@host:5432/db", "pool_size": 2})

# a psycopg2 pool the app already has
from psycopg2.pool import ThreadedConnectionPool
pool = ThreadedConnectionPool(1, 10, dsn="postgresql://user:pass@host:5432/db")
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=pool)

# a SQLAlchemy engine
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://user:pass@host:5432/db")
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=engine)

# a factory function
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=get_pg_conn)
```

If you pass a single connection object, give the logger its own. It commits after every write, so it must not share a connection that is in the middle of one of your transactions.

Other Postgres options, passed in the dict: `schema` (default `logs`), `entry_table` (`log_entry`), `run_table` (`log_run`).

```python
connection={"connection": manager.get("logs_db"), "schema": "audit"}
```

When you need an option together with a connection object, pass the object under the `connection` key, as above.

### 6.2 ClickHouse

| You pass | What c66_logger does |
| --- | --- |
| A c66-data-connection-layer connector, `manager.get("logs_ch")` | Borrows a client per write and returns it (recommended) |
| A `clickhouse_connect` client | Shares it, one call at a time |
| A factory function, such as `get_ch_client` | Calls it once and reuses the client |
| A `clickhouse_driver` `Client` | Uses its `execute()` |
| `{"host": ..., "port": 8123, "username": ..., "password": ..., "secure": False}` | Creates its own client and closes it on `close()` |

```python
import clickhouse_connect
client = clickhouse_connect.get_client(host="localhost", username="logs_writer", password="...",
                                       autogenerate_session_id=False)
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="clickhouse", connection=client)
```

Create a shared `clickhouse_connect` client with `autogenerate_session_id=False` if your own threads also use it: a ClickHouse session allows only one query at a time.

Options: `schema` (the ClickHouse database, default `logs`), `entry_table`, `run_table`, `client_options` (extra arguments for the client it creates), and `create_tables`:

```python
# development only: create logs.log_entry / logs.log_run if they're missing
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="clickhouse",
                    connection={"connection": client, "create_tables": True})
```

### 6.3 MongoDB

Give exactly one of `uri` or `client` (an existing `MongoClient`), plus `database`:

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="mongodb",
                    connection={"uri": "mongodb://localhost:27017", "database": "observability"})

from pymongo import MongoClient
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="mongodb",
                    connection={"client": MongoClient("mongodb://localhost:27017"), "database": "observability"})
```

Options: `entry_collection` (default `log_entry`), `run_collection` (`log_run`), `create_indexes` (`True`), `client_options`.

### 6.4 Target names

`target_type` is not case-sensitive and accepts these aliases: `postgresql` and `pg` for `postgres`, `mongo` for `mongodb`, `ch` for `clickhouse`.

### 6.5 Who closes what

| Created by | Closed by |
| --- | --- |
| Your app (connector, pool, engine, client) | Your app. c66_logger never closes it |
| c66_logger, from a dict such as `{"dsn": ...}` | `audit.close()` |

---

## Part 7. Creating and closing a logger

### 7.1 The constructor

```python
AuditLogger(
    tenant_id=None, environment_id=None,       # UUIDs; both or neither (then use bind(), Part 12)
    *,
    target_type=None,                          # "postgres" | "clickhouse" | "mongodb" | "memory" | your own
    connection=None,                           # Part 5 and 6
    sink=None,                                 # a ready-made sink instead of target_type (Part 18)
    logger_name="c66_logger",                  # stored in log_entry.logger_name
    accelerator_id=None, use_case_id=None,     # default ids for every entry (Part 10)
    tenant_user_id=None, correlation_id=None,
    mode="buffered",                           # or "sync" (Part 14)
    batch_size=500, flush_interval=2.0,        # buffered mode: when to write
    max_queue_size=50_000,                     # buffered mode: queue limit
    max_retries=3, retry_backoff=0.5,          # retries for failed writes
    on_drop=None,                              # called for entries that couldn't be written
)
```

| Parameter | Default | Meaning |
| --- | --- | --- |
| `tenant_id`, `environment_id` | none | The tenant every row belongs to. Must be UUIDs; pass both or neither |
| `target_type` | none | Where to write. Pass this **or** `sink`, not both |
| `connection` | none | A dict of options or a connection object |
| `sink` | none | An already-built sink object |
| `logger_name` | `"c66_logger"` | Name stored on every entry; up to 200 characters |
| `accelerator_id`, `use_case_id`, `tenant_user_id`, `correlation_id` | none | Ids added to every entry from this logger |
| `mode` | `"buffered"` | `"buffered"` writes in the background; `"sync"` writes before the call returns |
| `batch_size` | 500 | Buffered: write when this many entries are queued |
| `flush_interval` | 2.0 s | Buffered: write at least this often |
| `max_queue_size` | 50,000 | Buffered: beyond this, new entries are dropped |
| `max_retries` | 3 | Retries for a failed write |
| `retry_backoff` | 0.5 s | First wait between retries; doubles each time, capped at 5 s |
| `on_drop` | log a warning | `callable(entry, reason)` for entries that couldn't be written |

Creating the logger checks the connection and that both tables exist. A problem raises `ConfigurationError` straight away, not later at the first write.

### 7.2 From a config dict

`AuditLogger.from_config(...)` takes the same keys as the constructor. Handy when settings come from a file:

```python
config = {
    "tenant_id": TENANT_ID,
    "environment_id": ENVIRONMENT_ID,
    "target_type": "postgres",
    "connection": {"dsn": "postgresql://postgres@localhost:5432/c66_demo"},
    "logger_name": "order_service",
    "mode": "sync",
}
audit = AuditLogger.from_config(config)
```

### 7.3 Closing

`close()` writes everything still queued, stops the background thread, and closes connections the logger opened itself. Use the logger as a context manager, or call `close()` at shutdown:

```python
with AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=connector) as audit:
    audit.info("hello")
# closed here

audit = AuditLogger(...)
try:
    ...
finally:
    audit.close(timeout=10.0)     # waits up to 10 s for queued entries
```

Calling `close()` twice is safe. After closing, starting a run raises `RuntimeError`, and `audit.closed` is `True`.

If the program exits without closing a buffered logger, an exit hook still tries to write what's queued (for up to 5 s). Don't rely on it: close the logger yourself.

### 7.4 One logger per process

Create the logger once at startup and reuse it everywhere. It is thread-safe. Creating one per request would open a new background thread (and maybe a new pool) each time.

---

## Part 8. The logging functions

### 8.1 One function per level

The tables allow five levels. Each has a method:

| Method | Level | `level_no` | Use for |
| --- | --- | --- | --- |
| `audit.debug(...)` | DEBUG | 10 | Detail for troubleshooting |
| `audit.info(...)` | INFO | 20 | Normal events |
| `audit.warning(...)` | WARNING | 30 | Something unexpected that was handled |
| `audit.error(...)` | ERROR | 40 | A failure in one piece of work |
| `audit.critical(...)` | CRITICAL | 50 | A failure that affects the whole service |
| `audit.exception(...)` | ERROR | 40 | Like `error`, plus the traceback of the exception being handled |

`level_no` uses the same numbers as Python's `logging` module, so you can filter with `level_no >= 40` for errors and above.

```python
audit.debug({"message": "parsed file", "rows": 3, "delimiter": ","})
audit.info("import started")
audit.warning({"message": "order missing ship date, defaulted to today", "order_id": 1002})
audit.error({"message": "order rejected", "order_id": 1003, "reason": "bad amount"})
audit.critical({"message": "payment gateway unreachable", "queued": 1})
```

### 8.2 Logging an exception

Call `audit.exception(...)` inside an `except` block. It fills `exception_type`, `exception_message` and `exception_traceback`:

```python
try:
    amount = float("12,50")
except ValueError:
    audit.exception({"message": "order rejected: bad amount", "order_id": 1003})
```

Stored: `level = ERROR`, `exception_type = ValueError`, `exception_message = could not convert string to float: '12,50'`, and the full traceback.

Any level can carry an exception with `exc_info`:

```python
except TimeoutError as exc:
    audit.warning("retrying after timeout", exc_info=exc)    # an exception object
    audit.critical("giving up", exc_info=True)               # the exception being handled
```

### 8.3 `log()`: the general form

All the level methods call `log()`. Use it when the level is a variable:

```python
audit.log({"message": "status changed"}, level="WARNING")
audit.log("plain message", level=severity)                 # severity = "DEBUG" ... "CRITICAL"
```

```python
audit.log(*items, level="INFO", message=None, fmt="auto", csv_fieldnames=None, exc_info=None, **ids)
```

| Argument | Meaning |
| --- | --- |
| `*items` | What to log (Part 9). Each item becomes one or more rows |
| `level` | One of the five levels, case-insensitive |
| `message` | Default message for items that don't have one |
| `fmt` | `"auto"` (default), `"dict"`, `"json"` or `"csv"`: force how strings are read |
| `csv_fieldnames` | Column names for CSV text without a header row |
| `exc_info` | `True` or an exception: fill the exception columns |
| `**ids` | `run_id`, `correlation_id`, `tenant_user_id`, `use_case_id`, `accelerator_id` for every row of this call (Part 10) |

### 8.4 Return value

Every logging function returns the number of rows it created:

```python
n = audit.info([{"message": "a"}, {"message": "b"}])
print(n)    # 2
```

### 8.5 What is filled in automatically

You never pass these; c66_logger fills them on every entry:

| Column | Value |
| --- | --- |
| `log_id` | A new UUID (or the item's own `log_id`) |
| `tenant_id`, `environment_id` | From the logger |
| `logger_name` | From the logger (or the item's own `logger_name`) |
| `module`, `func_name`, `pathname`, `line_no` | Where in your code the call was made |
| `process_id`, `process_name`, `thread_id`, `thread_name` | The calling process and thread |
| `logged_at` | The current time in UTC (or the item's own `logged_at`) |
| `created_at` | Set by the database |

Values longer than a column allows (for example `logger_name` over 200 characters) are cut to fit instead of failing the insert.

### 8.6 Invalid input

Mistakes raise `InvalidLogInputError` **before** anything is queued, so a bad call never writes half its rows. Examples:

```python
audit.info({"order_id": 1})                    # no message
audit.log("hello", level="NOTICE")             # not one of the five levels
audit.info("hello", run_id="not-a-uuid")       # an id that isn't a UUID
```

Write failures (the database is down, for example) never raise from a logging call. They are retried and then reported through `on_drop` (Part 14).

---

## Part 9. What you can pass to a logging function

### 9.1 Accepted input

| You pass | Rows created |
| --- | --- |
| A one-line string | 1, with the string as `message` |
| A dict | 1 |
| Several items, comma-separated | 1 per item (or more, for lists and CSV) |
| A list or tuple of dicts | 1 per dict |
| A JSON string: an object, an array of objects, or JSON Lines | 1 per object |
| CSV text with a header row | 1 per data row |
| CSV text without a header, plus `csv_fieldnames=[...]` | 1 per row |
| `bytes` of any string form (UTF-8) | as for the string |

```python
audit.info("sync started")                                           # plain message
audit.info({"message": "order created", "order_id": 1})              # dict
audit.info({"message": "a"}, {"message": "b"})                       # two items
audit.info([{"message": "a"}, {"message": "b"}])                     # list of dicts
audit.info('{"message": "order paid", "order_id": 1}')               # JSON object
audit.info('[{"message": "a"}, {"message": "b"}]')                   # JSON array
audit.info('{"message": "a"}\n{"message": "b"}')                     # JSON Lines
audit.info("message,order_id\ncreated,1\npaid,1")                    # CSV with a header
audit.info("created,1\npaid,1", csv_fieldnames=["message", "order_id"])   # CSV without a header
audit.info(b'{"message": "from bytes"}')                             # bytes
```

### 9.2 A shared message for many items

`message=` is the default for items without a `message` key:

```python
audit.info({"order_id": 1}, {"order_id": 2}, message="bulk update")   # 2 rows, both "bulk update"
audit.info(message="sync started")                                     # 1 row, no other fields
```

### 9.3 How keys become columns

Keys that match a column set that column. **Every other key goes into `metadata_json`.**

| Key in the item | Sets |
| --- | --- |
| `message` | `message` (required, here or as `message=`) |
| `level` | `level`, overriding the method's level for this item |
| `logger_name` | `logger_name` for this item |
| `logged_at` | `logged_at` (a `datetime`, an ISO string or a Unix timestamp), for importing past events |
| `log_id` | `log_id` (a UUID you choose, for idempotent imports) |
| `run_id`, `correlation_id`, `tenant_user_id`, `use_case_id`, `accelerator_id` | Those id columns (UUIDs) |
| `metadata_json` | Merged into `metadata_json` (a dict or a JSON string) |
| `tenant_id`, `environment_id` | Checked only: must match the logger's, or the call is rejected |
| anything else | A key in `metadata_json` |

```python
audit.info({
    "message": "order created",
    "order_id": 1001,                        # -> metadata_json
    "total": 49.90,                          # -> metadata_json
    "customer": {"id": 7, "tier": "gold"},   # -> metadata_json (nested values are fine)
    "correlation_id": request_id,            # -> correlation_id column
})
```

Stored `metadata_json`: `{"order_id": 1001, "total": 49.9, "customer": {"id": 7, "tier": "gold"}}`.

Values that aren't JSON types, such as `datetime`, `Decimal` or `UUID`, are converted to strings.

### 9.4 Mixed levels in one call

An item's `level` key overrides the method's level, so one call can write several levels:

```python
audit.log([
    {"message": "row 1 ok"},
    {"message": "row 2 missing field", "level": "WARNING"},
    {"message": "row 3 rejected", "level": "ERROR"},
])
```

### 9.5 Importing past events

Set `logged_at` to keep each event's original time, and `log_id` to make the import safe to re-run (Postgres ignores a `log_id` it already has):

```python
audit.info([
    {"message": "legacy event", "logged_at": "2026-09-01T08:15:00Z", "log_id": "3f1c2a7e-0000-4000-8000-000000000001"},
    {"message": "legacy event", "logged_at": "2026-09-01T08:16:30Z", "log_id": "3f1c2a7e-0000-4000-8000-000000000002"},
])
```

---

## Part 10. Ids and context

### 10.1 The id columns

Besides the tenant, every entry has five optional id columns. All are UUIDs, and in Postgres each has a foreign key (Part 3.1).

| Column | Links the entry to |
| --- | --- |
| `run_id` | The run it happened in (`logs.log_run`) |
| `correlation_id` | Anything you want to tie together across services, such as a request id |
| `tenant_user_id` | The user who caused it (`app.tenant_user`) |
| `use_case_id` | The business use case (`app.business_use_case`) |
| `accelerator_id` | The accelerator (`app.accelerator`) |

### 10.2 Five places to set them

You can set ids at five levels. A more specific level wins over a more general one:

| Level | How | Applies to |
| --- | --- | --- |
| 1. Logger | `AuditLogger(..., use_case_id=...)` | Every entry from this logger |
| 2. Run | `audit.run(..., tenant_user_id=...)` | Every entry inside the run (and `run_id` itself) |
| 3. Context block | `with audit.context(correlation_id=...):` | Every entry inside the block |
| 4. Call | `audit.info(..., correlation_id=...)` | Every row of this call |
| 5. Item | `{"message": ..., "correlation_id": ...}` | This row only |

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=connector,
                    use_case_id=USE_CASE_ID)                          # 1. logger

with audit.run("nightly_sync", tenant_user_id=USER_ID):               # 2. run (adds run_id too)
    with audit.context(correlation_id=request_id):                    # 3. block
        audit.info("fetching")                                        # gets all of the above
        audit.info("one-off", correlation_id=other_id)                # 4. call
        audit.info({"message": "per row", "correlation_id": row_id})  # 5. item
```

### 10.3 `context()` for request handlers

`audit.context(...)` attaches ids to everything logged inside the block, without passing them to each call. It's the natural fit for web requests:

```python
def handle_request(request):
    with audit.context(correlation_id=request.id, tenant_user_id=request.user_id):
        audit.info("request received")
        process(request)              # anything process() logs gets the same ids
        audit.info("request done")
```

The context applies to the current thread or asyncio task only, so concurrent requests don't mix their ids. Work you hand to another thread doesn't inherit it; pass the ids explicitly there:

```python
with audit.run("export") as run:
    executor.submit(worker, run.run_id)

def worker(run_id):
    audit.info("chunk written", run_id=run_id)
```

---

## Part 11. Runs

A run is one row in `logs.log_run` that follows a piece of work from start to end.

| Column | Meaning |
| --- | --- |
| `run_id` | UUID; entries in the run carry it |
| `run_type` | What kind of work, up to 50 characters: `"nightly_sync"`, `"order_import"` |
| `status` | `Running`, then `Completed`, `Failed` or `Cancelled` |
| `started_at`, `ended_at`, `duration_ms` | Timing |
| `error_summary` | Why it failed or was cancelled |
| `metadata_json` | Your fields about the run |
| `accelerator_id`, `use_case_id`, `tenant_user_id` | Optional ids |

### 11.1 `audit.run()`: the usual way

`with audit.run(...)` starts a run, attaches its `run_id` to every entry logged in the block, and ends the run when the block exits:

```python
with audit.run("order_import", metadata={"file": "orders.csv"}) as run:
    audit.info("import started")
    import_orders()
print(run.run_id, run.status, run.duration_ms)
```

How the block ends decides the status:

| The block... | Status | Also |
| --- | --- | --- |
| finishes normally | `Completed` | |
| raises an exception | `Failed` | An ERROR entry with the traceback is logged, and `error_summary` is set. The exception is re-raised |
| is interrupted (Ctrl-C, task cancellation, `SystemExit`) | `Cancelled` | `error_summary` is set. The exception is re-raised |

c66_logger never swallows your exception:

```python
try:
    with audit.run("payment_batch"):
        charge_cards()            # raises PaymentError("gateway timeout")
except PaymentError:
    pass
# log_run:   status Failed, error_summary "PaymentError: gateway timeout"
# log_entry: ERROR "run failed: PaymentError: gateway timeout", with the traceback
```

`run()` also takes `accelerator_id`, `use_case_id` and `tenant_user_id`; entries inside the run inherit them.

### 11.2 `start_run()` and `end_run()`: explicit control

Use these when start and end happen in different places, such as a job started by one request and finished by another:

```python
run = audit.start_run("export", metadata={"format": "parquet"})
audit.info("export started", run_id=run.run_id)
...
audit.end_run(run, "Completed", metadata={"rows": 5000})
```

`end_run(run, status, error_summary=None, metadata=None, flush_timeout=10.0)`:

- `status` is `Completed`, `Failed` or `Cancelled`.
- `metadata` is merged into the run's existing `metadata_json`.
- It writes the run's queued entries first, so they're stored before the status changes.
- Ending a run twice is ignored, with a warning.

```python
audit.end_run(run, "Failed", error_summary="upstream API returned 503")
```

Entries logged between `start_run` and `end_run` aren't linked to the run automatically. Pass `run_id=run.run_id`, or wrap the code in `audit.context(run_id=run.run_id)`.

Inside a `with audit.run()` block you may end the run early yourself; the block then leaves it alone:

```python
with audit.run("sync") as run:
    if nothing_to_do():
        audit.end_run(run, "Cancelled", error_summary="no new data")
```

### 11.3 `record_run()`: report finished work in one call

For work that is reported after the fact, such as a request that already finished, write the whole run at once:

```python
audit.record_run(
    "document_ingestion",
    status="Completed",
    duration_ms=8200,                         # started_at is computed as now - 8.2 s
    metadata={"filename": "contracts.pdf", "chunks": 42},
)
```

`record_run(run_type, *, status="Completed", run_id=None, started_at=None, ended_at=None, duration_ms=None, error_summary=None, metadata=None, accelerator_id=None, use_case_id=None, tenant_user_id=None)`.

It upserts on `run_id`, so it can also overwrite a run started earlier with `start_run()`.

### 11.4 Choosing your own run_id

`start_run(..., run_id=...)` and `record_run(..., run_id=...)` accept a UUID you choose, such as an incoming request id. That makes the run easy to find later:

```python
run = audit.start_run("chat_request", run_id=request_id)
```

### 11.5 Runs are written immediately

Run rows are written straight away, not queued. So a run always exists before any entry that points to it, and the foreign key from `log_entry.run_id` never fails. If a run can't be written after the retries, `start_run()` (and `run()`) raises `RunWriteError`.

### 11.6 The `LogRun` object

`start_run()`, `record_run()` and `with audit.run() as run` give you a `LogRun` with the same fields as the table: `run.run_id`, `run.status`, `run.started_at`, `run.ended_at`, `run.duration_ms`, `run.error_summary`, `run.metadata_json`, and `run.is_running`.

---

## Part 12. Many tenants in one process

A web service that serves every customer needs one logger per tenant. Create **one** logger without tenant ids, then call `bind()` per tenant. Bound loggers share the root's connection and background thread, so a new tenant costs no new pool and no new thread.

```python
root = AuditLogger(target_type="postgres", connection=manager.get("logs_db"),
                   logger_name="integration_hub")             # no tenant ids

acme = root.bind(tenant_id=ACME_ID, environment_id=ACME_PROD)
globex = root.bind(tenant_id=GLOBEX_ID, environment_id=GLOBEX_PROD, logger_name="billing")

with acme.run("hourly_sync"):
    acme.info({"message": "sync finished", "rows": 100})
globex.info("invoice sent")

root.close()      # closes everything; closing a bound logger does nothing
```

`bind(tenant_id=None, environment_id=None, *, logger_name=None, accelerator_id=..., use_case_id=..., tenant_user_id=..., correlation_id=...)`. Anything you don't pass is inherited.

A root logger without tenant ids can't log by itself. Calling `root.info(...)` raises `ConfigurationError` that tells you to use `bind()`.

A typical pattern caches one bound logger per tenant:

```python
_loggers = {}

def logger_for(tenant_name):
    if tenant_name not in _loggers:
        tenant_id, environment_id = lookup_tenant(tenant_name)    # from app.tenant / app.tenant_environment
        _loggers[tenant_name] = root.bind(tenant_id=tenant_id, environment_id=environment_id)
    return _loggers[tenant_name]
```

See [examples/13_multi_tenant_config.py](../examples/13_multi_tenant_config.py).

---

## Part 13. Using Python's standard `logging`

If your code already uses `logging.getLogger(...)`, attach an `AuditLogHandler` and those calls go to `logs.log_entry` too, with no other changes:

```python
import logging
from c66_logger import AuditLogHandler

log = logging.getLogger("order_service")
log.setLevel(logging.INFO)
log.addHandler(AuditLogHandler(audit))

log.info("order created")
log.warning("stock low for %s", "SKU-42")
try:
    1 / 0
except ZeroDivisionError:
    log.exception("payment failed")          # ERROR, with the exception columns filled
```

The record's fields map onto the columns: logger name, level, message, module, function, path, line, process, thread, and the exception columns.

### 13.1 Extra fields and ids

Put extra fields under `extra={"audit": {...}}`. Id keys set their columns; everything else goes to `metadata_json`:

```python
log.info("order created", extra={"audit": {"order_id": 1001, "correlation_id": request_id}})
```

Ids can also be passed directly in `extra`, such as `extra={"run_id": run.run_id}`. Entries logged inside `audit.run()` or `audit.context()` pick up those ids automatically.

### 13.2 Levels

`AuditLogHandler(audit, level=logging.NOTSET)` takes a minimum level like any handler. Custom levels are rounded down to one of the five the table allows: level 25 becomes INFO, 45 becomes ERROR, anything below 20 becomes DEBUG.

The handler doesn't own the `AuditLogger`; close the logger yourself.

See [examples/09_logging_bridge.py](../examples/09_logging_bridge.py).

---

## Part 14. Delivery, retries and failures

### 14.1 Buffered mode (default)

A logging call only validates the input and puts the entries in a queue, then returns. One background thread writes them in batches when either limit is reached:

- `batch_size` entries are queued (default 500), or
- `flush_interval` seconds have passed (default 2.0).

Your code never waits for the database. Queued entries are also written on `flush()`, `close()` and `end_run()`.

```python
audit = AuditLogger(..., batch_size=1000, flush_interval=5.0)
```

### 14.2 Sync mode

`mode="sync"` writes before the logging call returns, with no background thread. Use it for short-lived processes such as CLI tools, serverless functions and tests:

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="postgres", connection=connector, mode="sync")
```

Sync mode still retries, and still never raises a write failure from a logging call.

### 14.3 `flush()`

`flush(timeout=10.0)` waits until everything logged so far is written (or dropped). It returns `False` if it timed out:

```python
audit.info("checkpoint")
if not audit.flush(timeout=5.0):
    print("logs are still being written")
```

### 14.4 Retries

A failed write is retried up to `max_retries` times (default 3). The wait starts at `retry_backoff` (0.5 s) and doubles each time, up to 5 s. Retries never create duplicates:

| Target | How duplicates are prevented |
| --- | --- |
| Postgres | Entries: `ON CONFLICT (log_id) DO NOTHING`. Runs: `ON CONFLICT (run_id) DO UPDATE` |
| ClickHouse | A deduplication token per batch, and `ReplacingMergeTree` tables |
| MongoDB | `_id` is the `log_id` / `run_id` |

### 14.5 Rows the database rejects

Some rows can never be written: a tenant id not in `app.tenant`, a `run_id` that doesn't exist, a CHECK violation. Retrying can't fix those, so in Postgres only the offending rows are dropped (with the database error as the reason). The rest of the batch is still written.

### 14.6 `on_drop`: knowing what was lost

Entries that couldn't be written are passed to `on_drop(entry, reason)`. By default it logs a warning on the `c66_logger` logger. Supply your own to count, alert, or save them:

```python
import json

def save_dropped(entry, reason):
    with open("dropped-logs.jsonl", "a") as f:
        f.write(json.dumps({"reason": reason, "entry": json.loads(entry.to_json())}) + "\n")

audit = AuditLogger(..., on_drop=save_dropped)
```

Reasons include `queue_full`, `logger_closed`, the database's error after the last retry, or the rejection reason from 14.5.

### 14.7 A full queue

In buffered mode the queue holds up to `max_queue_size` entries (default 50,000). If the database is slow or down for long enough to fill it, new entries are dropped with reason `queue_full`, so memory stays bounded and your app keeps running.

### 14.8 Seeing c66_logger's own messages

c66_logger reports its own problems (drops, retries, telemetry failures) on the standard logger named `c66_logger`. It has only a `NullHandler` by default, so nothing is printed. To see them:

```python
import logging
logging.getLogger("c66_logger").addHandler(logging.StreamHandler())
logging.getLogger("c66_logger").setLevel(logging.INFO)
```

Never attach an `AuditLogHandler` to the `c66_logger` logger: that would log c66_logger's messages through itself.

---

## Part 15. Telemetry (c66-chatbot's logging calls)

### 15.1 What Telemetry is

c66-chatbot logs through helper functions in its `app_common.py`: `log_event`, `log_llm_call`, `log_feedback` and others, which wrote to its own ClickHouse and Postgres tables. `Telemetry` is a drop-in replacement: it has the **same functions with the same arguments and defaults**, but writes to `logs.log_run` and `logs.log_entry` through c66_logger.

So moving the chatbot over means changing where those functions come from. The call sites stay as they are.

```python
# before
from app_common import log_llm_call, log_event
log_llm_call(request_id, client, 1, "routing", "anthropic", 1200, 40)

# after
log_llm_call = telemetry.log_llm_call
log_event = telemetry.log_event
log_llm_call(request_id, client, 1, "routing", "anthropic", 1200, 40)    # unchanged
```

### 15.2 How chatbot calls map onto the tables

Each `/api/chat` request becomes one run; everything that happens while answering it becomes entries in that run.

| Chatbot call | Writes | `logger_name` / `run_type` |
| --- | --- | --- |
| `log_event` (one per request) | The request's `log_run` | `chat_request` (other routes get their own run type) |
| `log_llm_call` | `log_entry` in the request's run | `c66.llm` |
| `log_retrieval_event` | `log_entry` in the request's run | `c66.retrieval` |
| `log_graph_query` | `log_entry` in the request's run | `c66.graph` |
| `log_feedback` | `log_entry`, linked to the request by `run_id` and `correlation_id` | `c66.feedback` |
| `log_ingestion_event` | A `log_run` plus a `log_entry` | `document_ingestion`, `c66.ingestion` |
| `log_eval_run` | `log_entry` on the system tenant | `c66.eval` |
| `log_deletion_receipt` | `log_entry` | `c66.governance` |
| `log_audit_event` | `log_entry` | `c66.audit` |
| `print(...)` warnings | `log_entry` through `logging` | `c66.app` |

Every column the old chatbot tables had is kept, under the same name, in `metadata_json`. So `events.total_cost_usd` becomes `metadata_json->>'total_cost_usd'` in Postgres, or `JSONExtractFloat(metadata_json, 'total_cost_usd')` in ClickHouse. The full field-by-field map is in [docs/chatbot-log-mapping.md](chatbot-log-mapping.md).

### 15.3 Setting it up

The chatbot names tenants by client (such as `"meta_telephony"`), but the tables need `tenant_id` and `environment_id` UUIDs. You give Telemetry a function that maps one to the other:

```python
from c66_logger import AuditLogger, Telemetry

root = AuditLogger(target_type="clickhouse", connection=manager.get("logs_ch"))   # no tenant ids

telemetry = Telemetry(
    root,
    resolve_tenant=lookup_tenant_ids,           # client name -> (tenant_id, environment_id), or None to skip
    resolve_user=lookup_tenant_user_id,         # username -> app.tenant_user id (optional)
    system_tenant=(PLATFORM_TENANT_ID, PLATFORM_ENV_ID),   # for events with no client
    cost_fn=_calc_cost_split,                   # (provider, input_tokens, output_tokens) -> (input $, output $)
    model_fn=PROVIDER_MODEL.get,                # provider -> model name
    environment=APP_ENVIRONMENT,                # stored on every request run
    prompt_version=PROMPT_VERSION,              # default for log_event(prompt_version=None)
)
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `audit` | yes | A root `AuditLogger`, usually without tenant ids. Telemetry binds one logger per client, all sharing its connection |
| `resolve_tenant` | yes | `client -> (tenant_id, environment_id)`. Return `None` to skip a client; its events are dropped and reported once |
| `resolve_user` | no | `username -> tenant_user_id` UUID, stored in `tenant_user_id` |
| `system_tenant` | no | `(tenant_id, environment_id)` for `log_eval_run` and the logging handler. Without it, those events are skipped |
| `cost_fn` | no | Computes costs. Without it, the cost fields are left out |
| `model_fn` | no | `provider -> model name`, stored as `model` |
| `environment` | no | Stored as `environment` on request runs |
| `prompt_version` | no | Default prompt version for `log_event` |
| `request_cache_size` | no (10,000) | How many open requests Telemetry remembers to link their entries |

A simple `resolve_tenant` is a dict lookup:

```python
CLIENTS = {
    "meta_telephony": ("5a1e5f0c-0000-4000-8000-000000000001", "e0000000-0000-4000-8000-000000000001"),
    "acme":           ("4b0b5907-0000-4000-8000-000000000002", "e0000000-0000-4000-8000-000000000002"),
}
telemetry = Telemetry(root, resolve_tenant=CLIENTS.get)
```

In production, look the ids up in `app.tenant` and `app.tenant_environment` (and cache them).

### 15.4 Telemetry never raises

Like the original helpers, every Telemetry method catches its own errors. A logging problem must never break a chat answer, an upload or a deletion. Problems are reported as warnings on the `c66_logger` logger (Part 14.8).

### 15.5 Logging a chat request

A request is identified by its `request_id` (a UUID string). It becomes the run's `run_id`, so the request is easy to find later. The run is opened automatically by the first call that carries the request id:

```python
request_id = str(uuid.uuid4())

telemetry.log_llm_call(request_id, "meta_telephony", 1, "routing", "anthropic", 1200, 40,
                       max_tokens=200, username="alice", latency_ms=310, finish_reason="end_turn")
telemetry.log_retrieval_event(request_id, "meta_telephony", 12, True, username="alice")
telemetry.log_graph_query(request_id, "meta_telephony", "graph_traversal",
                          cypher_text="MATCH (c:Customer) ...", anchor_count=3,
                          nodes_returned=40, relationships_returned=55, username="alice")
telemetry.log_llm_call(request_id, "meta_telephony", 2, "answer", "anthropic", 5000, 700,
                       max_tokens=4096, username="alice", latency_ms=4200)

# at the end of the request: the run's final state
telemetry.log_event("meta_telephony", "/api/chat", "Why did churn rise in Q3?", "graph_traversal",
                    provider="anthropic", llm_call_count=2, input_tokens=6200, output_tokens=740,
                    latency_ms=5300, request_id=request_id, username="alice",
                    groundedness_score=4, groundedness_reason="supported")

# later, when the user rates the answer
telemetry.log_feedback(request_id, "meta_telephony", 1, comment="spot on", username="alice")
```

Result: one `log_run` row (`chat_request`, `Completed`, `duration_ms` 5300, the question, tokens and cost in `metadata_json`), and one `log_entry` row per LLM call, retrieval, graph query and feedback, all with that `run_id`.

If `log_event` is called with `success=False, error="..."`, the run is marked `Failed` and an ERROR entry is added.

To open the run before anything else happens, call `start_request` at the beginning of the request. It's optional:

```python
telemetry.start_request(request_id, "meta_telephony", route="/api/chat", username="alice")
```

### 15.6 The other events

```python
# a document ingestion job: its own run plus a summary entry
telemetry.log_ingestion_event("meta_telephony", "doc-9", "contracts.pdf", "pdf", uploaded_by="alice",
                              latency_ms=8200, total_chunks=42, llm_cost_usd=0.03, embedding_cost_usd=0.001)

# an evaluation result (goes to system_tenant)
telemetry.log_eval_run("hallucination_v3", 17, "numeric", "What was Q3 revenue?",
                       True, [], [], "$4.2M")

# a tenant data deletion
telemetry.log_deletion_receipt("acme", "alice", nodes_deleted=120, chunks_deleted=900)

# an admin action
telemetry.log_audit_event("alice", "acme", "purge_tenant", "requested from admin console")
```

### 15.7 Replacing `print()` with logging

The chatbot reports non-fatal problems with `print(...)`. `telemetry.logging_handler()` returns a standard logging handler that writes to the system tenant (or to a client's tenant with `client="..."`):

```python
import logging

log = logging.getLogger("c66.app")
log.setLevel(logging.INFO)
log.addHandler(telemetry.logging_handler())

log.warning("ClickHouse schema init error (non-fatal, logging disabled): %s", err)   # was print(...)
```

`logging_handler(level=logging.INFO, client=None)` raises `ValueError` if there's no tenant to write to (no `client` and no `system_tenant`).

### 15.8 Flushing and closing

Telemetry uses the root logger's background writer. Call `telemetry.flush()` to wait for pending writes, and close the root logger at shutdown:

```python
telemetry.flush(timeout=10.0)
root.close()
```

### 15.9 Querying telemetry

```sql
-- Postgres: one request and its cost
SELECT run_type, status, duration_ms,
       metadata_json->>'question'       AS question,
       metadata_json->>'total_cost_usd' AS total_cost_usd
FROM logs.log_run WHERE run_id = '<request id>';

-- Postgres: LLM cost by purpose
SELECT metadata_json->>'purpose' AS purpose,
       sum((metadata_json->>'total_cost_usd')::numeric) AS cost_usd
FROM logs.log_entry WHERE logger_name = 'c66.llm'
GROUP BY 1 ORDER BY 2 DESC;
```

```sql
-- ClickHouse: the same cost report
SELECT JSONExtractString(metadata_json, 'purpose') AS purpose,
       round(sum(JSONExtractFloat(metadata_json, 'total_cost_usd')), 6) AS cost_usd
FROM logs.log_entry WHERE logger_name = 'c66.llm'
GROUP BY purpose ORDER BY cost_usd DESC;
```

Working examples: [examples/16_chatbot_telemetry.py](../examples/16_chatbot_telemetry.py) and [examples/20_telemetry_connections_yaml.py](../examples/20_telemetry_connections_yaml.py) (through `connections.yaml`, to Postgres and ClickHouse).

---

## Part 16. Testing code that logs

Use the `memory` target with `mode="sync"` in unit tests. Nothing touches a database, and everything is written by the time the call returns:

```python
from c66_logger import AuditLogger

def test_order_import_logs_a_completed_run():
    audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="memory", mode="sync")

    import_orders(audit)                       # your code under test

    runs = list(audit.sink.runs.values())
    assert runs[0].run_type == "order_import"
    assert runs[0].status == "Completed"
    messages = [e.message for e in audit.sink.entries]
    assert "import started" in messages
    assert all(e.run_id == runs[0].run_id for e in audit.sink.entries)
```

| Attribute | Contains |
| --- | --- |
| `audit.sink.entries` | A list of `LogEntry` objects, in order |
| `audit.sink.runs` | A dict of `run_id` -> `LogRun`, each in its latest state |
| `audit.sink.clear()` | Empties both |

A `LogEntry` has the same fields as the table: `entry.message`, `entry.level`, `entry.level_no`, `entry.metadata_json`, `entry.run_id`, `entry.exception_type`, and so on.

If your package creates the logger itself, let tests pass one in (or pass `target_type="memory"` through your config). See [examples/14_testing_your_package](../examples/14_testing_your_package).

---

## Part 17. Embedding c66_logger in your own package

c66_logger is built to live inside other packages. Guidelines:

1. **Let the application pass the connection.** Your package shouldn't read credentials. Accept a connector, or a config dict, from the app that uses your package.
2. **Create loggers once and reuse them.** They're thread-safe. For many tenants, `bind()` off one root logger.
3. **Under forking servers** (gunicorn, uWSGI), create the logger in each worker, after the fork. A background thread doesn't survive a fork.
4. **Short-lived processes** (CLI tools, serverless) can use `mode="sync"`, or call `flush()` before exit.
5. **Don't configure logging.** c66_logger adds only a `NullHandler`. Leave handler setup to the application.
6. **Loggers share no global state.** Two packages in the same process can each have their own loggers without interfering.
7. **Close what you create.** Close the logger at shutdown; let the app close its connector.

A small wrapper keeps your package's code clean:

```python
# my_package/logging.py
from c66_logger import AuditLogger

_audit = None

def init_audit(tenant_id, environment_id, connection, target_type="postgres"):
    global _audit
    _audit = AuditLogger(tenant_id, environment_id, target_type=target_type,
                         connection=connection, logger_name="my_package")
    return _audit

def audit():
    if _audit is None:
        raise RuntimeError("call init_audit() at startup")
    return _audit

def shutdown_audit():
    if _audit is not None:
        _audit.close()
```

See [examples/07_embed_in_host_package](../examples/07_embed_in_host_package).

---

## Part 18. Adding a new target

To write somewhere c66_logger doesn't support yet (S3, Kafka, a file), write a sink class and register it.

### 18.1 Write a sink

Subclass `BaseSink` and implement two methods:

```python
from typing import Sequence
from c66_logger import BaseSink, LogEntry, LogRun, register_sink

class JsonlFileSink(BaseSink):
    target_type = "jsonl"

    def __init__(self, *, path: str):
        self.path = path

    def open(self) -> None:                      # optional: fail fast on bad settings
        open(self.path, "a").close()

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        with open(self.path, "a") as f:
            for entry in entries:
                f.write(entry.to_json() + "\n")

    def write_run(self, run: LogRun) -> None:   # called when a run starts and when it ends
        with open(self.path, "a") as f:
            f.write(run.to_json() + "\n")

    def close(self) -> None:                     # optional: release what you created
        pass

register_sink("jsonl", JsonlFileSink)
```

| Method | Called | Must |
| --- | --- | --- |
| `open()` | Once, when the logger is created | Raise if the settings are wrong |
| `write_batch(entries)` | From the background thread | Raise on a temporary failure (it's retried). Be safe to repeat for the same `log_id`. Raise `RejectedRecordsError` for records that can never be written |
| `write_run(run)` | From the caller's thread, at run start and end | Upsert on `run.run_id` |
| `close()` | On `audit.close()` | Release only what the sink created |

`entry.as_row()` and `entry.to_json()` (and the same on `LogRun`) give the record as a dict or JSON with the table's column names.

### 18.2 Use it

The `connection` dict becomes the sink's keyword arguments:

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, target_type="jsonl", connection={"path": "audit.jsonl"})
```

Or build the sink yourself and pass it with `sink=`:

```python
audit = AuditLogger(TENANT_ID, ENVIRONMENT_ID, sink=JsonlFileSink(path="audit.jsonl"))
```

A sink you pass with `sink=` is not closed by `audit.close()`.

### 18.3 Registry functions

| Function | Does |
| --- | --- |
| `register_sink(name, factory, replace=False)` | Adds a target. Raises `ValueError` if the name exists, unless `replace=True` |
| `available_targets()` | Lists registered target names |
| `create_sink(name, **options)` | Builds a sink by name, as `AuditLogger` does |

Full examples: [examples/11_custom_target_s3.py](../examples/11_custom_target_s3.py) and [examples/12_custom_target_kafka.py](../examples/12_custom_target_kafka.py).

---

## Part 19. Querying the logs

### 19.1 Postgres

```sql
-- latest runs for a tenant
SELECT run_id, run_type, status, started_at, duration_ms, error_summary
FROM logs.log_run
WHERE tenant_id = '<tenant id>'
ORDER BY started_at DESC LIMIT 20;

-- everything in one run
SELECT logged_at, level, logger_name, message, metadata_json
FROM logs.log_entry WHERE run_id = '<run id>' ORDER BY logged_at;

-- recent errors for a tenant (uses the partial index ix_log_entry_errors)
SELECT logged_at, logger_name, message, exception_type
FROM logs.log_entry
WHERE tenant_id = '<tenant id>' AND level_no >= 40
ORDER BY logged_at DESC LIMIT 50;

-- a field inside metadata_json
SELECT message, metadata_json->>'order_id' AS order_id
FROM logs.log_entry WHERE metadata_json ? 'order_id';

-- failed runs with tenant names
SELECT t.name AS tenant, r.run_type, r.error_summary, r.started_at
FROM logs.log_run r JOIN app.tenant t USING (tenant_id)
WHERE r.status = 'Failed' ORDER BY r.started_at DESC;
```

### 19.2 ClickHouse

Read runs with `FINAL`: starting and ending a run are two row versions, and `FINAL` returns the latest.

```sql
SELECT run_id, run_type, status, duration_ms
FROM logs.log_run FINAL
WHERE tenant_id = '<tenant id>'
ORDER BY started_at DESC LIMIT 20;

SELECT logged_at, level, message, JSONExtractInt(metadata_json, 'order_id') AS order_id
FROM logs.log_entry
WHERE run_id = '<run id>' AND JSONHas(metadata_json, 'order_id')
ORDER BY logged_at;

SELECT level, count() FROM logs.log_entry
WHERE tenant_id = '<tenant id>' AND logged_at > now() - INTERVAL 1 DAY
GROUP BY level, level_no ORDER BY level_no;
```

ClickHouse stores timestamps in UTC and `metadata_json` as a JSON string without spaces.

---

## Part 20. Errors and troubleshooting

### 20.1 Exceptions

All of them inherit from `C66LoggerError`.

| Exception | Raised when | Typical fix |
| --- | --- | --- |
| `ConfigurationError` | Missing or conflicting options; tables missing; a root logger without tenant used directly | Read the message; it names the problem |
| `UnknownTargetError` | `target_type` isn't registered | Check spelling, or `register_sink()` |
| `MissingDependencyError` | The target's driver isn't installed | `pip install "c66-logger[postgres]"` (or the extra it names) |
| `InvalidLogInputError` | A logging call got bad input: no message, unknown level, an id that isn't a UUID | Fix the call; nothing was queued |
| `RunWriteError` | A run couldn't be written after all retries | Check the database; the error is chained |
| `RejectedRecordsError` | Raised by a sink for records that can never be written | Handled for you: those records go to `on_drop` |

`ConfigurationError` and `InvalidLogInputError` are also `ValueError`s; `MissingDependencyError` is also an `ImportError`.

### 20.2 Common problems

| Symptom | Cause | Fix |
| --- | --- | --- |
| `table(s) "logs"."log_run", "logs"."log_entry" not found` | The tables don't exist in the database you connected to | Apply the DDL (Part 3), or check the database name |
| Rows missing in Postgres, warning `dropped record ... foreign key` | A tenant, user, use case or accelerator id isn't in `app.*` | Log ids that exist there |
| Nothing written yet | Buffered mode writes every 2 s or 500 entries | `audit.flush()`, or `mode="sync"` for scripts |
| Entries lost at exit | The logger wasn't closed | Call `audit.close()` at shutdown |
| `this AuditLogger has no tenant_id/environment_id` | Logging on a root logger | Use `root.bind(tenant_id=..., environment_id=...)` |
| ClickHouse shows a run twice, `Running` and `Completed` | Two row versions of the same run | Query with `FINAL` |
| `environment variable ... is not set` (from enterprise_connectors) | A `.env` value is missing or empty | Set it; passwords can't be empty |
| `zsh: no matches found: ...[postgres]` | zsh reads brackets as a pattern | Quote the package: `"c66-logger[postgres]"` |
| Telemetry events silently missing | `resolve_tenant` returned `None` for the client, or no `system_tenant` | See the warning on the `c66_logger` logger (Part 14.8) |

---

## Part 21. API reference

### `AuditLogger`

| Member | Summary |
| --- | --- |
| `AuditLogger(tenant_id, environment_id, *, target_type, connection, ...)` | Create a logger (Part 7.1) |
| `AuditLogger.from_config(dict)` | Create from a dict |
| `debug / info / warning / error / critical(*items, **kwargs)` | Log at a level; returns the row count |
| `exception(*items, **kwargs)` | ERROR plus the current exception |
| `log(*items, level, message, fmt, csv_fieldnames, exc_info, **ids)` | The general form |
| `context(**ids)` | Context manager: ids for every entry in the block |
| `run(run_type, *, metadata, accelerator_id, use_case_id, tenant_user_id)` | Context manager: start and end a run |
| `start_run(run_type, *, metadata, ..., run_id)` | Start a run; returns a `LogRun` |
| `end_run(run, status, *, error_summary, metadata, flush_timeout)` | End a run |
| `record_run(run_type, *, status, run_id, started_at, ended_at, duration_ms, ...)` | Write a finished run in one call |
| `bind(tenant_id, environment_id, *, logger_name, **ids)` | A logger for another tenant, sharing this one's connection |
| `log_record(record)` | Write a `logging.LogRecord` (used by `AuditLogHandler`) |
| `flush(timeout=10.0)` | Wait for queued entries; `False` on timeout |
| `close(timeout=10.0)` | Flush and release; safe to call twice |
| `closed` | `True` after `close()` |
| `sink` | The sink object (for tests: `entries`, `runs`) |
| `tenant_id`, `environment_id`, `logger_name` | The logger's settings |

### Other classes and functions

| Name | Summary |
| --- | --- |
| `Telemetry(audit, *, resolve_tenant, ...)` | c66-chatbot's logging calls (Part 15) |
| `AuditLogHandler(audit, level=NOTSET)` | A `logging.Handler` that writes to an `AuditLogger` (Part 13) |
| `LogEntry`, `LogRun` | Records shaped like the tables; `as_row()`, `to_json()` |
| `LEVELS` | `{"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}` |
| `RUN_STATUSES` | `("Running", "Completed", "Failed", "Cancelled")` |
| `BaseSink`, `register_sink`, `create_sink`, `available_targets` | Custom targets (Part 18) |
| `parse_items(items, fmt="auto", csv_fieldnames=None)` | The input parser `log()` uses, returns a list of dicts |
| `__version__` | The package version |

### Telemetry methods

| Method | Writes |
| --- | --- |
| `start_request(request_id, client, route="/api/chat", username="")` | Opens the request's run (optional) |
| `log_event(client, route, question, retrieval_mode, ...)` | The request's run, final state |
| `log_llm_call(request_id, client, call_index, purpose, provider, input_tokens, output_tokens, ...)` | `c66.llm` entry |
| `log_retrieval_event(request_id, client, records_matched, aggregate_computed, username="")` | `c66.retrieval` entry |
| `log_graph_query(request_id, client, query_type, ...)` | `c66.graph` entry |
| `log_feedback(request_id, client, rating, comment="", username="")` | `c66.feedback` entry |
| `log_ingestion_event(client, doc_id, filename, doc_type, ...)` | `document_ingestion` run plus `c66.ingestion` entry |
| `log_eval_run(eval_set_name, question_id, category, question, passed, missing_terms, forbidden_found, answer)` | `c66.eval` entry on the system tenant |
| `log_deletion_receipt(client, requested_by, ...)` | `c66.governance` entry |
| `log_audit_event(username, client_name, action, detail="")` | `c66.audit` entry |
| `logging_handler(level=INFO, client=None)` | A logging handler for `print()`-style messages |
| `flush(timeout=10.0)` | Wait for pending writes |

### More documentation

- [README.md](../README.md): overview and reference tables
- [docs/architecture.md](architecture.md): how the parts fit together
- [docs/implementation-guide.md](implementation-guide.md): a worked Salesforce example
- [docs/chatbot-log-mapping.md](chatbot-log-mapping.md): every chatbot field and where it lands
- [examples/README.md](../examples/README.md): 20 runnable examples
- [DEMO_GUIDE.md](../DEMO_GUIDE.md): a step-by-step demo from database setup to logged rows

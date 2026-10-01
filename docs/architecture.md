# c66_logger — architecture (v0.4)

_Last updated 2026-10-01 · See also: [implementation-guide.md](implementation-guide.md) · [chatbot-log-mapping.md](chatbot-log-mapping.md) · [data-flow diagram](architecture-diagram.html) · DDL: [Postgres](sql/logs_schema.sql), [ClickHouse](sql/clickhouse_logs_schema.sql)_

Multi-tenant logging package, embedded inside other Python packages. It writes
**log entries** to `logs.log_entry` and **run lifecycles** to `logs.log_run`,
in Postgres or ClickHouse (MongoDB for the POC too). The code that creates the
logger supplies the tenant, the environment and the **connection object** it
got from its connection library. c66_logger never looks up or stores
credentials, and never alters the tables.

![c66_logger v0.4 data flow](architecture-diagram.svg)

## Decisions so far

1. **Architect (2026-09-23).** The caller supplies the target type and connection details. POC targets are Postgres and MongoDB; S3 and Kafka come later. `log()` takes multiple items as dict, JSON or CSV. The package is used inside other Python packages.
2. **Target tables (2026-09-23).** Write to the existing `logs.log_entry` and `logs.log_run` ([sql/logs_schema.sql](sql/logs_schema.sql)). Both have foreign keys into `app.*`.
3. **Palvinder (2026-10-01).** Support Postgres **and ClickHouse**. c66_logger must cover every logging call c66-chatbot makes today. Connections come from a separate connection library; c66_logger takes the object it returns and logs through it.
4. **Destination for the chatbot's calls (2026-10-01).** Everything goes into `log_run` / `log_entry`. The old per-event tables (`events`, `llm_calls`, `audit_log`, …) are not written; their columns go to `metadata_json`. Field map: [chatbot-log-mapping.md](chatbot-log-mapping.md).

## What changed in v0.4 (from v0.3)

- **Connection objects.** `connection=` takes what the connection library returns, not only a dict of options. Postgres: a DB-API connection, a pool (`getconn`/`putconn`), a SQLAlchemy Engine, or a factory such as `get_pg_conn`. ClickHouse: a `clickhouse_connect` client, a `clickhouse_driver` Client, or a factory such as `get_ch_client`. Injected objects are never closed. See [Connections](#connections).
- **ClickHouse target.** `target_type="clickhouse"` (alias `ch`). Same two tables, same columns and CHECKs, no foreign keys ([sql/clickhouse_logs_schema.sql](sql/clickhouse_logs_schema.sql)). Both tables are `ReplacingMergeTree`, so retries don't duplicate. `log_run` keeps a row version per status change; read it with `FINAL`.
- **Postgres sink without SQLAlchemy.** It talks plain DB-API (psycopg2 or psycopg 3), so it works on the connection the app already has. SQLAlchemy engines are still accepted.
- **`bind()`.** One root logger (no tenant needed) on one connection; `root.bind(tenant_id, environment_id)` per tenant. Bound loggers share the sink and the writer thread.
- **`record_run()`.** Writes a run's final state in one call, for work that is already finished (e.g. a request that is logged at the end).
- **`Telemetry`.** One method per c66-chatbot logging helper, with the same names, parameters and defaults as in `app_common.py`. It maps each call onto `log_run` / `log_entry` and never raises.

## Component map

```
connection library (c66_clients / get_pg_conn / get_ch_client)
   |  connection object
   v
host package / c66-chatbot
   |  AuditLogger(target_type, connection=obj)  ->  .bind(tenant_id, environment_id)  per tenant
   |  Telemetry(audit, resolve_tenant=...)       ->  log_event / log_llm_call / ... (same signatures as app_common.py)
   |
   |-- run(), start_run(), end_run(), record_run() --- sync --> sink.write_run(LogRun)  --> log_run  (upsert / new version)
   |
   |-- audit.info(*items) --parse_items()--> LogEntry per item (ids, caller, process/thread, exception)
   |                                              |
   |                          BufferedWriter (queue + 1 thread, batch 500 / 2 s, retries, on_drop)
   |                          or SyncWriter (mode="sync")
   |                                              |
   |                                    sink.write_batch(entries)  --> log_entry (idempotent insert)
   |
   '-- logging.getLogger(...) + AuditLogHandler --> audit.log_record(LogRecord) --> same entry path
```

| Module | Responsibility |
| --- | --- |
| `c66_logger/logger.py` | `AuditLogger`: config, `bind()`, ids and context, runs, turning items into `LogEntry` |
| `c66_logger/telemetry.py` | `Telemetry`: c66-chatbot's nine helpers → runs and entries |
| `c66_logger/record.py` | `LogEntry`, `LogRun` (the table shapes), allowed levels and statuses, varchar limits |
| `c66_logger/_build.py` | UUID, level and timestamp validation; caller, process, thread and exception capture |
| `c66_logger/parsing.py` | dict / list / JSON / JSON Lines / CSV / plain string → list of dicts |
| `c66_logger/writers.py` | `BufferedWriter`, `SyncWriter`: batching, retries, rejected-row handling, `on_drop` |
| `c66_logger/sinks/_connections.py` | Turns a Postgres connection object into "borrow / give back" |
| `c66_logger/sinks/` | `BaseSink` contract, registry, `postgres`, `clickhouse`, `mongodb`, `memory` |
| `c66_logger/handler.py` | `AuditLogHandler`: `logging.LogRecord` → `log_entry` row |

**Sink contract:**

- `open()` connects and checks the tables exist. It raises at init.
- `write_batch(entries)` raises to get a retry, or raises `RejectedRecordsError` for permanent per-row failures.
- `write_run(run)` upserts on `run_id`.
- `close()` closes only what the sink created.

## Connections

There's no singleton and no global cache. Each root `AuditLogger` owns one sink; bound loggers reuse it. Entries are written by one background thread, so a buffered logger needs about one connection at a time, plus run writes from the caller's thread.

| Postgres `connection=` | What c66_logger does with it |
| --- | --- |
| DB-API connection (psycopg2 / psycopg 3) | Shares it behind a lock and commits after each write. It must be the logger's own connection, not one with the app's open transaction |
| Pool with `getconn()` / `putconn()` | Borrows per write and gives it back (`close=True` if it broke) |
| SQLAlchemy Engine | `raw_connection()` per write, then `close()` (back to the pool) |
| Zero-arg factory, e.g. `get_pg_conn` | Calls it per write, then `close()`; c66-chatbot's pooled proxy returns the connection to its pool on `close()` |
| `{"dsn": ...}` or host options | Its own small pool (`pool_size`, default 4), closed by `audit.close()` |

| ClickHouse `connection=` | What c66_logger does with it |
| --- | --- |
| `clickhouse_connect` client | Uses it for every insert |
| Zero-arg factory, e.g. `get_ch_client` | Calls it once and reuses the client |
| `clickhouse_driver` Client | `execute("INSERT ... VALUES", rows)` |
| `{"host": ...}` options | Its own client, closed by `audit.close()` |

## Status

- **Tests:** 138 pass, 4 skipped (the real-ClickHouse-server variants; they run when `C66_TEST_CLICKHOUSE_HOST` is set). Postgres tests run against Postgres 16 with the exact DDL, through every connection style, including a pooled proxy like the chatbot's `get_pg_conn`. ClickHouse tests run against embedded ClickHouse 26.9 (chdb), with rows encoded by `clickhouse_connect` itself. The `Telemetry` tests check that every signature matches `app_common.py` and replay a chat request on both databases.
- **Examples:** all 16 run.
- **MongoDB:** tested with mongomock only.

## Open items

- **ClickHouse server:** run the 4 skipped tests against a real server (`C66_TEST_CLICKHOUSE_HOST`).
- **c66_clients:** its `postgres.py` and `clickhouse.py` are stubs. The design assumes they return what `get_pg_conn` (psycopg2 pooled proxy) and `get_ch_client` (clickhouse_connect client) return today.
- **For Pal** (details in [chatbot-log-mapping.md](chatbot-log-mapping.md#open-points-for-pal)):
  - Where `resolve_tenant` gets `app.tenant` UUIDs from (the chatbot only has client names).
  - Which system tenant owns platform-level events (eval runs, startup messages).
  - Re-pointing the dashboards to `log_run` / `log_entry`.
  - Cut-over: dual-write the old tables for a while, or switch in one go.
- **MongoDB:** run the tests and examples against a real server.
- **S3 and Kafka sinks:** examples 11 and 12 show the shape. They aren't built in yet.
- **Forking servers:** nothing detects a fork yet. The logger must be created after the fork, in each worker.
- **Confirm with the architect:**
  - Should a failed `start_run()` raise (current behaviour) or degrade quietly? (`Telemetry` never raises either way.)
  - Is dropping after retries, with a dead-letter via `on_drop`, acceptable for these logs?
  - Should the package check `tenant_id` / `environment_id` against `app.*` at startup? Today an unknown id is only caught per row by the Postgres FKs; ClickHouse has no FKs and accepts it.

# c66_logger examples

Each file covers one use case and runs on its own. Together they write to
`logs.log_run` and `logs.log_entry` (see [Postgres DDL](../docs/sql/logs_schema.sql) ·
[ClickHouse DDL](../docs/sql/clickhouse_logs_schema.sql)).

| # | Example | Case | Needs |
| --- | --- | --- | --- |
| 01 | [01_postgres_quickstart.py](01_postgres_quickstart.py) | One run plus its entries in Postgres | Postgres |
| 02 | [02_mongodb_quickstart.py](02_mongodb_quickstart.py) | Same, in MongoDB (collections `log_entry` / `log_run`) | MongoDB |
| 03 | [03_input_formats.py](03_input_formats.py) | Every input `log()` accepts, and how item keys map to columns vs `metadata_json` | nothing |
| 04 | [04_runs.py](04_runs.py) | The `log_run` lifecycle: Completed, Failed, Cancelled, explicit start/end, runs across threads | Postgres (or `memory` arg) |
| 05 | [05_context_ids.py](05_context_ids.py) | Filling `correlation_id`, `tenant_user_id`, `use_case_id`, `accelerator_id` at each level | nothing |
| 06 | [06_existing_connection.py](06_existing_connection.py) | Pass the connection library's object: `get_pg_conn` factory (pooled proxy), a pool, a dedicated connection, `get_ch_client` | Postgres (ClickHouse embedded) |
| 07 | [07_embed_in_host_package/](07_embed_in_host_package/) | A package (`order_service`) embedding c66_logger; the app passes tenant, environment, target | Postgres |
| 08 | [08_sync_mode_serverless.py](08_sync_mode_serverless.py) | `mode="sync"` for CLIs, cron jobs, serverless handlers | nothing (`postgres` arg optional) |
| 09 | [09_logging_bridge.py](09_logging_bridge.py) | Existing `logging` calls → `log_entry` rows, including exception columns | nothing |
| 10 | [10_error_handling.py](10_error_handling.py) | Config errors, bad input, FK-rejected rows, outage → dead-letter file, `RunWriteError` | Postgres for part 3 |
| 11 | [11_custom_target_s3.py](11_custom_target_s3.py) | Future target: `S3Sink` (JSON Lines per batch, one object per run) | nothing (real bucket optional) |
| 12 | [12_custom_target_kafka.py](12_custom_target_kafka.py) | Future target: `KafkaSink` (entry topic + run topic) | nothing (real broker optional) |
| 13 | [13_multi_tenant_config.py](13_multi_tenant_config.py) | Several tenants in one process: one root logger, `bind()` per tenant, one shared connection and writer | Postgres |
| 14 | [14_testing_your_package/](14_testing_your_package/) | Unit-test a host package with the `memory` target | pytest |
| 15 | [15_clickhouse_quickstart.py](15_clickhouse_quickstart.py) | One run plus its entries in ClickHouse; reading `log_run` with `FINAL` | ClickHouse (embedded if unset) |
| 16 | [16_chatbot_telemetry.py](16_chatbot_telemetry.py) | Every c66-chatbot logging call through `Telemetry`: a chat request, ingestion, feedback, eval, deletion, audit, `print` → logging | ClickHouse (embedded) or `postgres` arg |
| 17 | [17_data_connection_layer.py](17_data_connection_layer.py) | Log through c66-data-connection-layer: pass `manager.get("logs_db")`; c66_logger borrows from its pool | Postgres + `enterprise_connectors` |

## Setup

```bash
pip install -e ".[all]"            # from the repo root

# Where the logs go, and which tenant/environment to log as. These are read by
# examples/_settings.py, which plays the calling application; c66_logger reads no env vars.
export C66_EXAMPLE_POSTGRES_DSN="postgresql://user:pass@localhost:5432/appdb"
export C66_EXAMPLE_TENANT_ID="<a tenant_id in app.tenant>"
export C66_EXAMPLE_ENVIRONMENT_ID="<an environment_id in app.tenant_environment>"
export C66_EXAMPLE_MONGO_URI="mongodb://localhost:27017"      # optional
export C66_EXAMPLE_CLICKHOUSE_HOST="localhost"                # optional; also _PORT, _USER, _PASSWORD
```

Without `C66_EXAMPLE_CLICKHOUSE_HOST`, the ClickHouse examples (06, 15, 16) use
embedded ClickHouse ([chdb](https://github.com/chdb-io/chdb), `pip install chdb`)
and create the tables in it.

`logs.log_entry` and `logs.log_run` have foreign keys to `app.*`. The tenant,
environment and any `use_case_id` / `accelerator_id` / `tenant_user_id` you log
must exist there. Postgres rejects rows that don't match (example 10 shows what
happens).

**Local sandbox without the real app schema.** For a throwaway database only,
[tests/sql/app_stub.sql](../tests/sql/app_stub.sql) creates minimal `app.*`
tables and the demo rows that `_settings.py` defaults to:

```bash
createdb c66_sandbox
psql -d c66_sandbox -f tests/sql/app_stub.sql -f docs/sql/logs_schema.sql
export C66_EXAMPLE_POSTGRES_DSN="postgresql://localhost/c66_sandbox"   # tenant/env vars can stay unset
```

## Run

```bash
cd examples
python 01_postgres_quickstart.py
python 04_runs.py                 # or: python 04_runs.py memory
(cd 07_embed_in_host_package && python run.py)
python -m pytest 14_testing_your_package -q
python 15_clickhouse_quickstart.py
python 16_chatbot_telemetry.py            # or: python 16_chatbot_telemetry.py postgres
python 17_data_connection_layer.py        # needs c66-data-connection-layer installed
```

All 17 were run on 2026-10-02 (17 with c66-data-connection-layer's dev branch). The Postgres ones ran against Postgres 16 with
the exact DDL. The ClickHouse ones ran against embedded ClickHouse 26.9 (chdb),
not a server. MongoDB (02) ran against mongomock, a MongoDB stand-in.

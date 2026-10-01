"""
Shared settings for the examples — this file plays the *caller*: the
application that knows which tenant it's running for and where its logs go.
c66_logger itself reads no environment variables.

Point these at rows that exist in your app.* tables (logs.log_entry and
logs.log_run have foreign keys to them). The defaults match the demo rows in
tests/sql/app_stub.sql, for a local database set up with:

    psql "$DSN" -f tests/sql/app_stub.sql -f docs/sql/logs_schema.sql
"""

import os

POSTGRES_DSN = os.environ.get("C66_EXAMPLE_POSTGRES_DSN", "postgresql://postgres@localhost:5432/postgres")
MONGO_URI = os.environ.get("C66_EXAMPLE_MONGO_URI")  # optional
CLICKHOUSE_HOST = os.environ.get("C66_EXAMPLE_CLICKHOUSE_HOST")  # optional; examples fall back to embedded chdb
CLICKHOUSE_PORT = int(os.environ.get("C66_EXAMPLE_CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("C66_EXAMPLE_CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("C66_EXAMPLE_CLICKHOUSE_PASSWORD", "")

TENANT_ID = os.environ.get("C66_EXAMPLE_TENANT_ID", "5a1e5f0c-0000-4000-8000-000000000001")          # salesforce
ENVIRONMENT_ID = os.environ.get("C66_EXAMPLE_ENVIRONMENT_ID", "e0000000-0000-4000-8000-000000000001")  # prod
ACCELERATOR_ID = os.environ.get("C66_EXAMPLE_ACCELERATOR_ID", "acce1e7a-0000-4000-8000-000000000001")
USE_CASE_ID = os.environ.get("C66_EXAMPLE_USE_CASE_ID", "0c0c0c0c-0000-4000-8000-000000000001")
TENANT_USER_ID = os.environ.get("C66_EXAMPLE_TENANT_USER_ID", "00000000-0000-4000-8000-00000000a11c")

TENANT = {"tenant_id": TENANT_ID, "environment_id": ENVIRONMENT_ID}


def postgres_target() -> dict:
    """target_type + connection for AuditLogger(**TENANT, **postgres_target())."""
    return {"target_type": "postgres", "connection": {"dsn": POSTGRES_DSN}}


def clickhouse_client():
    """
    A clickhouse_connect client, like the one c66_clients / get_ch_client() hands out.
    Without C66_EXAMPLE_CLICKHOUSE_HOST, an embedded ClickHouse (chdb) stand-in is
    used so the examples run anywhere (pip install chdb).
    """
    if CLICKHOUSE_HOST:
        import clickhouse_connect

        return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USER,
                                             password=CLICKHOUSE_PASSWORD, autogenerate_session_id=False)
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
    from chdb_client import ChdbClient  # test helper: real ClickHouse engine, clickhouse_connect serialization

    return ChdbClient()

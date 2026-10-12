"""
Case 19 — the app's real setup: .env + connections.yaml + enterprise_connectors,
logging the same run to Postgres and ClickHouse.

Same as cases 17 and 18, but the connectors come from a connections file instead
of being built in code. This is how an app wires it:

    .env               LOGS_PG_* and LOGS_CH_* values, passwords included (never committed)
    connections.yaml   logs_db (postgres) and logs_ch (clickhouse), as env:// references
    app startup        manager = ConnectorManager.from_yaml("connections.yaml")
                       audit = AuditLogger(..., connection=manager.get("logs_db"))

    pip install -e "/path/to/c66-data-connection-layer[postgres,clickhouse,yaml]" python-dotenv
    cp examples/.env.example examples/.env          # then edit it
    python examples/19_connections_yaml.py                  # both targets
    python examples/19_connections_yaml.py postgres         # or just one: postgres | clickhouse

Both targets need the log tables first (docs/sql/logs_schema.sql,
docs/sql/clickhouse_logs_schema.sql). Logs as C66_EXAMPLE_TENANT_ID /
C66_EXAMPLE_ENVIRONMENT_ID (see _settings.py); in Postgres those must exist in
app.tenant / app.tenant_environment.
"""

import sys
from pathlib import Path

from _settings import TENANT

from c66_logger import AuditLogger

try:
    from dotenv import load_dotenv
    from enterprise_connectors import ConnectorManager
except ImportError:
    raise SystemExit('needs c66-data-connection-layer and python-dotenv: pip install -e "<its folder>[postgres,clickhouse,yaml]" python-dotenv') from None

HERE = Path(__file__).resolve().parent


TARGETS = {"postgres": "logs_db", "clickhouse": "logs_ch"}       # target_type -> connection name in connections.yaml


def order_import(audit: AuditLogger):
    """The app's work: one run with an entry at every level the tables allow."""
    with audit.run("order_import", metadata={"file": "orders_1002.csv"}) as run:
        audit.debug({"message": "parsed file", "rows": 3, "delimiter": ","})
        audit.info("import started")
        audit.info({"message": "order created", "order_id": 1001})
        audit.warning({"message": "order missing ship date, defaulted to today", "order_id": 1002})
        try:
            float("12,50")                                          # a bad amount in row 3
        except ValueError:
            audit.exception({"message": "order rejected: bad amount", "order_id": 1003})  # ERROR + traceback
        audit.critical({"message": "payment gateway unreachable, orders queued", "queued": 1})
    return run


def read_back(target: str, connector, run_id: str) -> str:
    """The app keeps using the same connector after logging."""
    with connector.connection() as conn:
        if target == "postgres":                                    # conn: psycopg connection
            status, n = conn.execute(
                "SELECT status, (SELECT count(*) FROM logs.log_entry WHERE run_id = %s) FROM logs.log_run WHERE run_id = %s",
                (run_id, run_id)).fetchone()
        else:                                                       # conn: clickhouse_connect client
            status = conn.command(f"SELECT status FROM logs.log_run FINAL WHERE run_id = '{run_id}'")
            n = conn.command(f"SELECT count() FROM logs.log_entry WHERE run_id = '{run_id}'")
    return f"status {status}, {n} entries"


def main() -> None:
    targets = sys.argv[1:] or list(TARGETS)
    if unknown := set(targets) - set(TARGETS):
        raise SystemExit(f"unknown target(s) {sorted(unknown)}; choose from {list(TARGETS)}")
    if not load_dotenv(HERE / ".env"):                              # puts LOGS_PG_* / LOGS_CH_* into the environment
        raise SystemExit(f"no {HERE / '.env'}: copy .env.example to .env and fill it in")
    manager = ConnectorManager.from_yaml(HERE / "connections.yaml")  # app startup, once
    try:
        for target in targets:
            name = TARGETS[target]
            connector = manager.get(name)                           # the same object every time: one shared pool
            with AuditLogger(**TENANT, target_type=target, connection=connector, logger_name="order_service") as audit:
                run = order_import(audit)
            print(f"{target:<10} run {run.run_id} written through connections.yaml -> {name}: "
                  f"{read_back(target, connector, run.run_id)}")

        for stats in manager.pool_manager.stats().values():
            print(f"pool {stats.total} open, {stats.in_use} in use")  # 0 in use: every borrowed connection came back
    finally:
        manager.close()                                             # app shutdown: the app closes its connections


if __name__ == "__main__":
    main()

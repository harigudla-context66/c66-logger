"""
Case 18 — log to ClickHouse through c66-data-connection-layer (enterprise_connectors).

Same idea as case 17, for ClickHouse: the data connection layer owns the login
and a pool of clickhouse_connect clients; c66_logger borrows one client per
write with `connector.connection()` and hands it back. It never sees the
password and never closes the connector.

    pip install -e "/path/to/c66-data-connection-layer[clickhouse,yaml]"
    export C66_EXAMPLE_CLICKHOUSE_HOST=localhost          # + _PORT (8123), _USER, _PASSWORD, _SECURE
    python examples/18_data_connection_layer_clickhouse.py

Without C66_EXAMPLE_CLICKHOUSE_HOST it runs against an embedded ClickHouse
(chdb, `pip install chdb`): the library's connector still does the login,
pooling and leasing, only its clients talk to the embedded engine instead of
a server over HTTP.

In an app the connector usually comes from connections.yaml:

    connections:
      logs_ch:
        type: clickhouse
        config: {host: env://LOGS_CH_HOST, port: env://LOGS_CH_PORT, secure: env://LOGS_CH_SECURE}
        credentials: {username: env://LOGS_CH_USER, password: env://LOGS_CH_PASSWORD}
        pool: {max_size: 4}

    manager = ConnectorManager.from_yaml("connections.yaml")
    audit = AuditLogger(..., target_type="clickhouse", connection=manager.get("logs_ch"))
"""

import os
import sys
from pathlib import Path

from _settings import CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD, CLICKHOUSE_PORT, CLICKHOUSE_USER, TENANT

from c66_logger import AuditLogger

try:
    from enterprise_connectors import ConnectorManager, UsernamePassword
except ImportError:
    raise SystemExit('needs c66-data-connection-layer: pip install -e "<its folder>[clickhouse,yaml]"') from None


def use_embedded_clickhouse() -> None:
    """Point clickhouse_connect.get_client at the embedded engine (demo only)."""
    import itertools

    import clickhouse_connect

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
    from chdb_client import ChdbClient

    ids = itertools.count(1)

    class EmbeddedClient(ChdbClient):
        def __init__(self, **_params):
            super().__init__(reset_databases=())
            self.params = {"session_id": f"embedded-{next(ids)}"}

    clickhouse_connect.get_client = lambda **params: EmbeddedClient(**params)


def main() -> None:
    if not CLICKHOUSE_HOST:
        use_embedded_clickhouse()
        print("no C66_EXAMPLE_CLICKHOUSE_HOST: using an embedded ClickHouse")

    manager = ConnectorManager()                                     # app startup, once
    logs_ch = manager.create(
        "clickhouse",
        {"host": CLICKHOUSE_HOST or "localhost", "port": CLICKHOUSE_PORT,
         "secure": os.environ.get("C66_EXAMPLE_CLICKHOUSE_SECURE", "false").lower() == "true",
         "client_name": "order_service-logs"},                       # shown in system.processes / query_log
        UsernamePassword(username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD),
        pool_config={"max_size": 4},
    )
    try:
        with AuditLogger(**TENANT, target_type="clickhouse", logger_name="order_service",
                         connection={"connection": logs_ch, "create_tables": True}) as audit:   # create_tables: dev only
            with audit.run("order_import", metadata={"file": "orders_1002.csv"}) as run:
                audit.info("import started")
                audit.info({"message": "order created", "order_id": 1001})
        print("run", run.run_id, "written through enterprise_connectors (ClickHouse)")

        (stats,) = manager.pool_manager.stats().values()
        print(f"pool after logging: {stats.total} open, {stats.in_use} in use (everything was returned)")

        with logs_ch.connection() as client:                          # the app keeps using the same connector
            status = client.command(f"SELECT status FROM logs.log_run FINAL WHERE run_id = '{run.run_id}'")
            n = client.command(f"SELECT count() FROM logs.log_entry WHERE run_id = '{run.run_id}'")
        print(f"log_run status: {status}; entries in that run: {n}")
    finally:
        manager.close()                                               # app shutdown


if __name__ == "__main__":
    main()

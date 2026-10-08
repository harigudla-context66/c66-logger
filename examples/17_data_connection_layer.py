"""
Case 17 — log through c66-data-connection-layer (enterprise_connectors).

The data connection layer owns the database login and the pool. The app loads
its connections once, picks the one for the log tables by name, and passes
that connector to c66_logger. Each write borrows a connection with
`connector.connection()` and hands it back; c66_logger never sees a password
and never closes the connector.

    pip install -e "/path/to/c66-data-connection-layer[postgres,yaml]"   # dev branch, Postgres only for now
    python examples/17_data_connection_layer.py

In an app it's normally a connections.yaml entry (see the library's
HOW_TO_CONNECT_AND_FETCH_DATA.md):

    connections:
      logs_db:
        type: postgres
        config: {host: env://LOGS_PG_HOST, port: env://LOGS_PG_PORT, database: env://LOGS_PG_DATABASE}
        credentials: {username: env://LOGS_PG_USER, password: env://LOGS_PG_PASSWORD}
        pool: {max_size: 5}

    manager = ConnectorManager.from_yaml("connections.yaml")
    audit = AuditLogger(..., target_type="postgres", connection=manager.get("logs_db"))

This example builds the same connector in code from C66_EXAMPLE_POSTGRES_DSN
(URL-encode special characters in the password, e.g. @ as %40), and logs as
C66_EXAMPLE_TENANT_ID / C66_EXAMPLE_ENVIRONMENT_ID. Against a real database
those must be ids that exist in app.tenant / app.tenant_environment.
"""

from urllib.parse import unquote, urlparse

from _settings import POSTGRES_DSN, TENANT

from c66_logger import AuditLogger

try:
    from enterprise_connectors import ConnectorManager, UsernamePassword
except ImportError:
    raise SystemExit('needs c66-data-connection-layer: pip install -e "<its folder>[postgres,yaml]"') from None


def main() -> None:
    url = urlparse(POSTGRES_DSN)
    manager = ConnectorManager()                                   # app startup, once
    logs_db = manager.create(
        "postgres",
        {"host": url.hostname, "port": url.port or 5432, "database": url.path.lstrip("/"),
         "application_name": "order_service-logs"},                # shows up in pg_stat_activity
        UsernamePassword(username=unquote(url.username or "postgres"), password=unquote(url.password or "")),
        pool_config={"max_size": 5},
    )
    try:
        with AuditLogger(**TENANT, target_type="postgres", connection=logs_db, logger_name="order_service") as audit:
            with audit.run("order_import", metadata={"file": "orders_1002.csv"}) as run:
                audit.info("import started")
                audit.info({"message": "order created", "order_id": 1001})
        print("run", run.run_id, "written through enterprise_connectors")

        (stats,) = manager.pool_manager.stats().values()
        print(f"pool after logging: {stats.total} open, {stats.in_use} in use (everything was returned)")

        with logs_db.connection() as conn:                          # the app keeps using the same connector
            n = conn.execute("SELECT count(*) FROM logs.log_entry WHERE run_id = %s", (run.run_id,)).fetchone()[0]
        print("entries in that run:", n)
    finally:
        manager.close()                                             # app shutdown: the app closes its connections


if __name__ == "__main__":
    main()

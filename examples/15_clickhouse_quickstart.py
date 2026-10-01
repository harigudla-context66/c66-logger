"""
Case 15 — the same runs and entries, in ClickHouse.

ClickHouse gets the same two tables (docs/sql/clickhouse_logs_schema.sql):
same columns, same level/status checks, no foreign keys. log_run keeps one
row version per status change; read it with FINAL.

    pip install -e ".[clickhouse]"
    export C66_EXAMPLE_CLICKHOUSE_HOST=localhost        # unset: embedded ClickHouse (pip install chdb)
    python examples/15_clickhouse_quickstart.py
"""

from _settings import TENANT, clickhouse_client

from c66_logger import AuditLogger


def main() -> None:
    client = clickhouse_client()          # in the app: c66_clients / get_ch_client()
    with AuditLogger(
        **TENANT,
        target_type="clickhouse",
        connection={"connection": client, "create_tables": True},   # create_tables: dev only
        logger_name="order_service",
    ) as audit:
        with audit.run("order_import", metadata={"file": "orders_0923.csv"}) as run:
            audit.info("import started")
            audit.info("message,order_id,amount\norder created,1001,49.99\norder created,1002,15.00")
            audit.warning({"message": "payment retry", "order_id": 1001, "attempt": 2})

    query = client.rows if hasattr(client, "rows") else (lambda sql: client.query(sql).named_results())
    print("log_run (FINAL):", list(query(
        f"SELECT run_type, status, duration_ms FROM logs.log_run FINAL WHERE run_id = '{run.run_id}'")))
    for row in query(f"SELECT level, message, JSONExtractString(metadata_json, 'order_id') AS order_id "
                     f"FROM logs.log_entry WHERE run_id = '{run.run_id}' ORDER BY logged_at"):
        print("log_entry:", dict(row))


if __name__ == "__main__":
    main()

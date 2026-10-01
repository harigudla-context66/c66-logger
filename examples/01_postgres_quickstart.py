"""
Case 1 — write a run and its log entries to Postgres (logs.log_run / logs.log_entry).

The caller supplies the tenant, the environment and the connection. The
tables must already exist (docs/sql/logs_schema.sql); c66_logger only writes.

    pip install -e ".[postgres]"
    export C66_EXAMPLE_POSTGRES_DSN="postgresql://user:pass@localhost:5432/appdb"
    python examples/01_postgres_quickstart.py
"""

from _settings import POSTGRES_DSN, TENANT, USE_CASE_ID

from c66_logger import AuditLogger


def main() -> None:
    with AuditLogger(
        **TENANT,                                   # tenant_id + environment_id (UUIDs)
        target_type="postgres",
        connection={"dsn": POSTGRES_DSN},           # schema "logs", tables log_entry / log_run
        logger_name="order_service",
        use_case_id=USE_CASE_ID,                    # default for every entry and run
    ) as audit:
        # One logs.log_run row: inserted as 'Running', updated to 'Completed'
        # (or 'Failed' if the block raises) with ended_at and duration_ms.
        with audit.run("order_import", metadata={"source": "sftp", "file": "orders_0923.csv"}) as run:
            audit.info("import started")                                   # plain message
            audit.info({"message": "order created", "order_id": 1001, "amount": 49.99})
            audit.warning({"message": "payment retry", "order_id": 1001, "attempt": 2})
            audit.error({"message": "payment failed", "order_id": 1002})
            # every entry above gets run_id = run.run_id automatically

        print(f"run {run.run_id}: {run.status}, {run.duration_ms} ms")
    # leaving the outer block writes anything still queued and closes the pool


if __name__ == "__main__":
    main()

# Check:
#   SELECT run_type, status, duration_ms, metadata_json FROM logs.log_run ORDER BY started_at DESC LIMIT 1;
#   SELECT level, message, metadata_json, func_name, line_no
#   FROM logs.log_entry WHERE run_id = '<run id printed above>' ORDER BY logged_at;

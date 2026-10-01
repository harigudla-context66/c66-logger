"""
Case 2 — the same tenant, writing to MongoDB.

Two collections mirror the two tables with identical field names:
log_entry (_id = log_id) and log_run (_id = run_id). Only the target type and
connection change; every log()/run() call stays the same.

    pip install -e ".[mongodb]"
    export C66_EXAMPLE_MONGO_URI="mongodb://user:pass@localhost:27017"
    python examples/02_mongodb_quickstart.py
"""

from _settings import MONGO_URI, TENANT

from c66_logger import AuditLogger


def main() -> None:
    with AuditLogger(
        **TENANT,
        target_type="mongodb",
        connection={
            "uri": MONGO_URI or "mongodb://localhost:27017",
            "database": "logs",                       # collections: log_entry, log_run
            "client_options": {"serverSelectionTimeoutMS": 5000},
        },
        logger_name="order_service",
    ) as audit:
        with audit.run("order_import") as run:
            audit.info({"message": "order created", "order_id": 2001, "items": [{"sku": "A1", "qty": 2}]})
            audit.info({"message": "order shipped", "order_id": 2001, "carrier": "DHL"})
        print(f"run {run.run_id}: {run.status}")


if __name__ == "__main__":
    main()

# mongosh:
#   use logs
#   db.log_run.find().sort({started_at: -1}).limit(1)
#   db.log_entry.find({run_id: "<run id>"}).sort({logged_at: 1})

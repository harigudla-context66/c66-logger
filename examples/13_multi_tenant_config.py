"""
Case 13 — many tenants in one process (a web app serving every client).

Create ONE logger without tenant ids on your connection, then bind() one per
tenant. Bound loggers share the connection and the background writer: no new
pool and no new thread per tenant. Closing the root closes everything.

    pip install -e ".[postgres]"
    python examples/13_multi_tenant_config.py
"""

from _settings import POSTGRES_DSN

from c66_logger import AuditLogger

# client name -> ids. In a real app: looked up from app.tenant / app.tenant_environment.
# These are the demo rows from tests/sql/app_stub.sql.
TENANTS = {
    "salesforce": ("5a1e5f0c-0000-4000-8000-000000000001", "e0000000-0000-4000-8000-000000000001"),
    "hubspot":    ("4b0b5907-0000-4000-8000-000000000002", "e0000000-0000-4000-8000-000000000002"),
    "zendesk":    ("2e4de5c0-0000-4000-8000-000000000003", "e0000000-0000-4000-8000-000000000003"),
}


def main() -> None:
    root = AuditLogger(target_type="postgres", connection={"dsn": POSTGRES_DSN}, logger_name="integration_hub")
    loggers = {name: root.bind(tenant_id=t, environment_id=e) for name, (t, e) in TENANTS.items()}
    try:
        for name, audit in loggers.items():
            with audit.run("hourly_sync"):
                audit.info({"message": "sync finished", "tenant": name, "rows": 100})
        print("one writer thread shared:", len({id(a._writer) for a in loggers.values()}) == 1)
        for name, audit in loggers.items():
            print(f"{name:<11} -> tenant {audit.tenant_id}")
    finally:
        root.close()  # flushes and closes for every bound logger


if __name__ == "__main__":
    main()

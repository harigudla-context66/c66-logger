"""
Case 7 — c66_logger embedded inside another package.

This script is the application that *uses* order_service. It knows the
tenant, the environment and where their logs go, and passes all three in;
order_service contains no connection details.

    pip install -e ".[postgres]"
    cd examples/07_embed_in_host_package && python run.py
"""

import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # examples/_settings.py
from _settings import POSTGRES_DSN, TENANT_ID, ENVIRONMENT_ID, USE_CASE_ID  # noqa: E402

from order_service import OrderService  # noqa: E402

# In a real app this comes from the app's own config / secrets manager.
LOG_TARGET = {
    "target_type": "postgres",
    "connection": {"dsn": POSTGRES_DSN},
    "use_case_id": USE_CASE_ID,
}


def main() -> None:
    service = OrderService(TENANT_ID, ENVIRONMENT_ID, log_target=LOG_TARGET)
    try:
        paid = service.process_batch(
            [
                {"order_id": 4001, "amount": 120.0, "customer": "acme", "card_ok": True},
                {"order_id": 4002, "amount": 15.0, "customer": "globex", "card_ok": False},
                {"order_id": 4003, "amount": 99.0, "customer": "initech", "card_ok": True},
            ],
            correlation_id=str(uuid.uuid4()),
        )
        imported = service.import_csv("order_id,amount\n4004,15\n4005,99")
        print(f"order_service: 1 run, {paid}/3 orders paid, {imported} orders imported from CSV")
    finally:
        service.close()  # writes what's queued, releases the connection pool


if __name__ == "__main__":
    main()

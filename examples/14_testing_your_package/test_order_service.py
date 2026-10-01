"""
Case 14 — unit-testing a package that embeds c66_logger.

Inject an AuditLogger on the "memory" target in sync mode, run your code,
then assert on sink.entries (log_entry rows) and sink.runs (log_run rows).
No database, no background thread, no sleeps.

    cd examples && python -m pytest 14_testing_your_package -q
"""

import sys
from pathlib import Path

import pytest

from c66_logger import AuditLogger

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1] / "07_embed_in_host_package"))  # the example host package
from order_service import OrderService  # noqa: E402

TENANT = "5a1e5f0c-0000-4000-8000-000000000001"
ENV = "e0000000-0000-4000-8000-000000000001"
CORR = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def audit():
    logger = AuditLogger(tenant_id=TENANT, environment_id=ENV, target_type="memory", mode="sync")
    yield logger
    logger.close()


@pytest.fixture
def service(audit):
    return OrderService(audit_logger=audit)


ORDERS = [
    {"order_id": 1, "amount": 10.0, "customer": "acme", "card_ok": True},
    {"order_id": 2, "amount": 20.0, "customer": "globex", "card_ok": False},
]


def test_batch_is_one_completed_run(service, audit):
    service.process_batch(ORDERS, correlation_id=CORR)
    (run,) = audit.sink.runs.values()
    assert (run.run_type, run.status, run.metadata_json) == ("order_batch", "Completed", {"orders": 2})
    assert all(e.run_id == run.run_id and e.correlation_id == CORR for e in audit.sink.entries)


def test_declined_payment_is_an_error_entry_with_the_exception(service, audit):
    paid = service.process_batch(ORDERS)
    assert paid == 1
    failed = [e for e in audit.sink.entries if e.level == "ERROR"]
    assert len(failed) == 1
    assert failed[0].metadata_json == {"order_id": 2}
    assert failed[0].exception_type.endswith("PaymentDeclined")


def test_csv_import_logs_one_entry_per_row(service, audit):
    assert service.import_csv("order_id,amount\n3,15\n4,99") == 2
    assert [e.metadata_json["order_id"] for e in audit.sink.entries] == ["3", "4"]  # CSV values are strings
    assert {e.message for e in audit.sink.entries} == {"order imported"}


def test_service_does_not_close_an_injected_logger(service, audit):
    service.close()
    assert not audit.closed

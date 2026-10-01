"""
Case 9 — a package that already logs through Python's `logging` module.

logs.log_entry's columns are a logging.LogRecord: attach AuditLogHandler and
existing logging calls fill logger_name, level, message, module, func_name,
pathname, line_no, process/thread — and the exception columns when you use
log.exception() or exc_info=True.

Runs with no database (memory target); swap in postgres_target() for real use.

    python examples/09_logging_bridge.py
"""

import logging

from _settings import TENANT

from c66_logger import AuditLogger, AuditLogHandler

log = logging.getLogger("order_service.payments")


def charge(order_id: int) -> None:
    log.info("charging order %s", order_id, extra={"audit": {"order_id": order_id}})
    try:
        raise TimeoutError("gateway did not answer in 30s")
    except TimeoutError:
        log.exception("charge failed", extra={"audit": {"order_id": order_id, "gateway": "stripe"}})


def main() -> None:
    audit = AuditLogger(**TENANT, target_type="memory", mode="sync")

    handler = AuditLogHandler(audit, level=logging.INFO)  # DEBUG stays out of the table
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)

    with audit.run("payment_batch"):   # entries from logging calls get the run_id too
        charge(501)
        log.debug("not stored: below the handler's level")

    for e in audit.sink.entries:
        print(f"{e.level:<7} {e.logger_name:<24} {e.func_name}:{e.line_no:<4} {e.message!r:<24} "
              f"{e.metadata_json} exception={e.exception_type}")

    log.removeHandler(handler)
    audit.close()  # the handler doesn't own the AuditLogger


if __name__ == "__main__":
    main()

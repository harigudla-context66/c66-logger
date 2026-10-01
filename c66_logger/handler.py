from __future__ import annotations

import logging

from .logger import AuditLogger


class AuditLogHandler(logging.Handler):
    """
    Bridges the standard ``logging`` module into logs.log_entry. The table's
    columns map one-to-one onto a logging.LogRecord: logger_name, level,
    message, module, func_name, pathname, line_no, process/thread, and the
    exception columns when you log with exc_info / logger.exception().

        audit = AuditLogger(tenant_id=..., environment_id=..., target_type="postgres", connection={...})
        logging.getLogger("order_service").addHandler(AuditLogHandler(audit))

        log = logging.getLogger("order_service")
        log.info("order created", extra={"audit": {"order_id": 1, "correlation_id": req_id}})
        try: ...
        except Exception: log.exception("payment failed")

    Under ``extra={"audit": {...}}``, id keys (run_id, correlation_id,
    tenant_user_id, use_case_id, accelerator_id) set their columns and all
    other keys go to metadata_json. Entries logged inside ``audit.run()`` /
    ``audit.context()`` pick up those ids too. Custom levels are mapped onto
    the five the table allows. The handler doesn't own the AuditLogger —
    close that yourself.
    """

    def __init__(self, audit_logger: AuditLogger, level: int = logging.NOTSET):
        super().__init__(level=level)
        self.audit_logger = audit_logger

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.audit_logger.log_record(record)
        except Exception:
            self.handleError(record)

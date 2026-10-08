"""
c66_logger — multi-tenant audit logging for embedding in other Python packages.

    from c66_logger import AuditLogger

    with AuditLogger(
        tenant_id="5a1e5f0c-...",            # app.tenant
        environment_id="e0000000-...",       # app.tenant_environment
        target_type="postgres",
        connection={"dsn": "postgresql://user:pass@host:5432/db"},   # writes logs.log_entry / logs.log_run
        logger_name="order_service",
    ) as audit, audit.run("nightly_sync"):
        audit.info({"message": "order created", "order_id": 123})

Importing this package loads no database drivers and configures no logging;
each target's driver is an optional extra (``c66-logger[postgres]``,
``c66-logger[clickhouse]``, ``c66-logger[mongodb]``). Targets accept the
connection objects (or factories/pools) the host's own connection library
hands out.
"""

import logging as _logging

from .exceptions import (
    C66LoggerError,
    ConfigurationError,
    InvalidLogInputError,
    MissingDependencyError,
    RejectedRecordsError,
    RunWriteError,
    UnknownTargetError,
)
from .handler import AuditLogHandler
from .logger import AuditLogger
from .parsing import parse_items
from .record import LEVELS, RUN_STATUSES, LogEntry, LogRun
from .sinks import BaseSink, available_targets, create_sink, register_sink
from .telemetry import Telemetry

__version__ = "0.4.2"

# Library etiquette: never emit to the host app's handlers unless it opts in.
_logging.getLogger(__name__).addHandler(_logging.NullHandler())

__all__ = [
    "AuditLogger",
    "AuditLogHandler",
    "Telemetry",
    "LogEntry",
    "LogRun",
    "LEVELS",
    "RUN_STATUSES",
    "BaseSink",
    "register_sink",
    "create_sink",
    "available_targets",
    "parse_items",
    "C66LoggerError",
    "ConfigurationError",
    "InvalidLogInputError",
    "MissingDependencyError",
    "RejectedRecordsError",
    "RunWriteError",
    "UnknownTargetError",
    "__version__",
]

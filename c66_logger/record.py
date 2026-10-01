"""
The two record types, shaped exactly like the target tables:

* LogEntry -> logs.log_entry  (one per logged item)
* LogRun   -> logs.log_run    (one per run: inserted as Running, updated when it ends)

``created_at`` is left to the database default and never sent.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import time
import uuid
from typing import Any, Dict, Optional

LEVELS: Dict[str, int] = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
RUN_STATUSES = ("Running", "Completed", "Failed", "Cancelled")
# id columns a caller may set per logger, per run, per context block, per call or per item
CONTEXT_FIELDS = ("run_id", "accelerator_id", "use_case_id", "tenant_user_id", "correlation_id")

# varchar limits from the DDL; longer values are clipped instead of failing the insert
MAX_LENGTHS = {
    "logger_name": 200,
    "module": 200,
    "func_name": 200,
    "pathname": 500,
    "process_name": 100,
    "thread_name": 100,
    "exception_type": 200,
    "run_type": 50,
}


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def new_uuid() -> str:
    return str(uuid.uuid4())


@dataclasses.dataclass(frozen=True)
class LogEntry:
    """One row of logs.log_entry."""

    tenant_id: str
    environment_id: str
    logger_name: str
    level: str
    message: str
    logged_at: dt.datetime = dataclasses.field(default_factory=utcnow)
    log_id: str = dataclasses.field(default_factory=new_uuid)
    run_id: Optional[str] = None
    accelerator_id: Optional[str] = None
    use_case_id: Optional[str] = None
    tenant_user_id: Optional[str] = None
    correlation_id: Optional[str] = None
    module: Optional[str] = None
    func_name: Optional[str] = None
    pathname: Optional[str] = None
    line_no: Optional[int] = None
    process_id: Optional[int] = None
    process_name: Optional[str] = None
    thread_id: Optional[int] = None
    thread_name: Optional[str] = None
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    exception_traceback: Optional[str] = None
    metadata_json: Dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def level_no(self) -> int:
        return LEVELS[self.level]

    def as_row(self) -> Dict[str, Any]:
        """Column -> value for an INSERT into logs.log_entry (no created_at)."""
        row = dataclasses.asdict(self)
        row["level_no"] = self.level_no
        return row

    def to_json(self) -> str:
        """JSON form for text targets (S3 objects, Kafka messages)."""
        return json.dumps(self.as_row(), default=_json_default, separators=(",", ":"))


@dataclasses.dataclass
class LogRun:
    """One row of logs.log_run. Mutable: its status changes when the run ends."""

    tenant_id: str
    environment_id: str
    run_id: str = dataclasses.field(default_factory=new_uuid)
    accelerator_id: Optional[str] = None
    use_case_id: Optional[str] = None
    tenant_user_id: Optional[str] = None
    run_type: Optional[str] = None
    status: str = "Running"
    started_at: dt.datetime = dataclasses.field(default_factory=utcnow)
    ended_at: Optional[dt.datetime] = None
    duration_ms: Optional[int] = None
    error_summary: Optional[str] = None
    metadata_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    updated_at: dt.datetime = dataclasses.field(default_factory=utcnow)
    _started_monotonic: float = dataclasses.field(
        default_factory=time.monotonic, repr=False, compare=False
    )
    # when this run's row was first written; lets append-only targets
    # (ClickHouse) keep a stable created_at across status updates
    _first_written_at: Optional[dt.datetime] = dataclasses.field(default=None, repr=False, compare=False)

    @property
    def is_running(self) -> bool:
        return self.status == "Running"

    def as_row(self) -> Dict[str, Any]:
        """Column -> value for an INSERT/UPDATE of logs.log_run (no created_at)."""
        row = {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if not f.name.startswith("_")}
        row["metadata_json"] = dict(self.metadata_json)
        return row

    def to_json(self) -> str:
        return json.dumps(self.as_row(), default=_json_default, separators=(",", ":"))


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return str(value)


def jsonable(value: Dict[str, Any]) -> Dict[str, Any]:
    """Make metadata safe for every target (JSONB, BSON, JSON text): unknown types become strings."""
    return json.loads(json.dumps(value, default=_json_default))

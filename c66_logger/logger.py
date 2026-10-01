from __future__ import annotations

import contextlib
import contextvars
import datetime as dt
import json
import logging
import threading
import time
from collections.abc import Mapping
from typing import Any, Dict, Iterator, Optional, Sequence, Union

from . import _build
from .exceptions import ConfigurationError, InvalidLogInputError, RunWriteError
from .parsing import parse_items
from .record import CONTEXT_FIELDS, RUN_STATUSES, LogEntry, LogRun, jsonable, new_uuid, utcnow
from .sinks import create_sink
from .sinks.base import BaseSink
from .writers import BufferedWriter, OnDrop, SyncWriter

_log = logging.getLogger("c66_logger")

# Keys an input item may use to set a column; every other key goes to metadata_json.
_ITEM_COLUMNS = ("message", "level", "logger_name", "logged_at", "log_id", "metadata_json") + CONTEXT_FIELDS
_ERROR_SUMMARY_MAX = 4000


class AuditLogger:
    """
    Writes log entries (logs.log_entry) and runs (logs.log_run) for a
    tenant + environment to one target. The caller supplies the target —
    connection options, or the connection object its own connection library
    hands out:

        audit = AuditLogger(
            tenant_id="5a1e5f0c-...", environment_id="e0000000-...",
            target_type="postgres",
            connection=get_pg_conn,              # or {"dsn": "postgresql://..."}, a psycopg2
            logger_name="order_service",         #    connection/pool, an Engine, ...
        )

        with audit.run(run_type="nightly_sync"):           # one logs.log_run row
            audit.info({"message": "sync started", "objects": 3})   # logs.log_entry rows, run_id attached

    A process serving many tenants creates one logger without tenant ids and
    calls ``bind(tenant_id=..., environment_id=...)`` per tenant: the bound
    loggers share its connection and background writer.

    Every instance is independent (no module-level state).
    """

    def __init__(
        self,
        tenant_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        *,
        target_type: Optional[str] = None,
        connection: Any = None,
        sink: Optional[BaseSink] = None,
        logger_name: str = "c66_logger",
        accelerator_id: Optional[str] = None,
        use_case_id: Optional[str] = None,
        tenant_user_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        mode: str = "buffered",
        batch_size: int = 500,
        flush_interval: float = 2.0,
        max_queue_size: int = 50_000,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
        on_drop: Optional[OnDrop] = None,
    ):
        if (sink is None) == (target_type is None):
            raise ConfigurationError("pass exactly one of target_type (+ connection) or sink")
        if sink is not None and connection:
            raise ConfigurationError("connection is only used with target_type, not with sink")
        if mode not in ("buffered", "sync"):
            raise ConfigurationError("mode must be 'buffered' or 'sync'")
        if not logger_name or not isinstance(logger_name, str):
            raise ConfigurationError("logger_name must be a non-empty string")

        self.tenant_id = _build.as_uuid(tenant_id, "tenant_id", ConfigurationError)
        self.environment_id = _build.as_uuid(environment_id, "environment_id", ConfigurationError)
        if bool(self.tenant_id) != bool(self.environment_id):
            raise ConfigurationError("pass both tenant_id and environment_id (UUIDs), or neither and use bind()")
        self.logger_name = _build.clip("logger_name", logger_name)
        self._defaults = self._validate_ids(
            dict(accelerator_id=accelerator_id, use_case_id=use_case_id,
                 tenant_user_id=tenant_user_id, correlation_id=correlation_id),
            ConfigurationError,
        )
        self._context: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
            f"c66_logger_context_{id(self)}", default={}
        )
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff

        self._owns_sink = sink is None
        if sink is None:
            if connection is None or isinstance(connection, Mapping):
                options = dict(connection or {})
            else:  # a connection object / pool / factory from the host's connection library
                options = {"connection": connection}
            sink = create_sink(target_type, **options)
        self.sink: BaseSink = sink
        try:
            self.sink.open()  # fail fast: bad connection details or missing tables surface here
        except Exception:
            if self._owns_sink:
                self.sink.close()
            raise

        retry = dict(max_retries=max_retries, retry_backoff=retry_backoff, on_drop=on_drop)
        if mode == "sync":
            self._writer: Union[SyncWriter, BufferedWriter] = SyncWriter(self.sink, **retry)
        else:
            self._writer = BufferedWriter(
                self.sink, batch_size=batch_size, flush_interval=flush_interval,
                max_queue_size=max_queue_size, **retry,
            )
        self._owns_writer = True
        self._closed = False
        self._close_lock = threading.Lock()

    def bind(
        self,
        tenant_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        *,
        logger_name: Optional[str] = None,
        **ids: Any,
    ) -> "AuditLogger":
        """
        A logger for another tenant / environment / logger_name / default ids
        that shares this one's connection and background writer (no new thread,
        no new pool). Unset arguments are inherited. Closing a bound logger is a
        no-op; close the one you created.

            root = AuditLogger(target_type="clickhouse", connection=get_ch_client)
            acme = root.bind(tenant_id=ACME_ID, environment_id=ACME_PROD, logger_name="chat")
        """
        unknown = set(ids) - {"accelerator_id", "use_case_id", "tenant_user_id", "correlation_id"}
        if unknown:
            raise TypeError(f"bind() got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        child = object.__new__(AuditLogger)
        child.__dict__.update(self.__dict__)
        child.tenant_id = _build.as_uuid(tenant_id, "tenant_id", ConfigurationError) or self.tenant_id
        child.environment_id = (_build.as_uuid(environment_id, "environment_id", ConfigurationError)
                                or self.environment_id)
        if logger_name is not None:
            if not logger_name:
                raise ConfigurationError("logger_name must be a non-empty string")
            child.logger_name = _build.clip("logger_name", logger_name)
        child._defaults = _build.merge_ids(self._defaults, self._validate_ids(ids, ConfigurationError))
        child._context = contextvars.ContextVar(f"c66_logger_context_{id(child)}", default={})
        child._owns_sink = False
        child._owns_writer = False
        child._closed = False
        child._close_lock = threading.Lock()
        return child

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "AuditLogger":
        """Build from a plain dict: {"tenant_id", "environment_id", "target_type", "connection", ...}."""
        return cls(**dict(config))

    # ---- log entries ---------------------------------------------------

    def log(
        self,
        *items: Any,
        level: str = "INFO",
        message: Optional[str] = None,
        fmt: str = "auto",
        csv_fieldnames: Optional[Sequence[str]] = None,
        exc_info: Any = None,
        **ids: Any,
    ) -> int:
        """
        Write one logs.log_entry row per item and return how many were created.

        Each item is a dict, list of dicts, JSON (object / array / JSON Lines),
        CSV text, or a single-line string (used as the message). Keys that match a column set it — ``message``, ``level``,
        ``logger_name``, ``logged_at``, ``log_id``, ``run_id``, ``correlation_id``,
        ``tenant_user_id``, ``use_case_id``, ``accelerator_id`` — every other key
        goes into ``metadata_json``. ``message`` is required: in the item, or as
        the ``message=`` default for the call.

        ``**ids`` sets run_id / correlation_id / tenant_user_id / use_case_id /
        accelerator_id for every item in this call.

        Raises InvalidLogInputError before anything is queued if any item is
        invalid. Write failures never raise here (see on_drop).
        """
        unknown = set(ids) - set(CONTEXT_FIELDS)
        if unknown:
            raise TypeError(f"log() got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        self._require_tenant()
        call_level = _build.as_level(level, InvalidLogInputError)
        call_ids = self._validate_ids(ids, InvalidLogInputError)
        base_ids = _build.merge_ids(self._defaults, self._context.get(), call_ids)

        if not items and message is not None:
            payloads = [{}]  # audit.info(message="sync started")
        else:
            payloads = parse_items(items, fmt=fmt, csv_fieldnames=csv_fieldnames)
        shared = {**_build.find_caller(), **_build.process_and_thread(), **_build.exception_fields(exc_info)}
        entries = [
            self._entry_from_item(n, payload, call_level, message, base_ids, shared)
            for n, payload in enumerate(payloads)
        ]
        self._writer.submit(entries)
        return len(entries)

    def debug(self, *items: Any, **kwargs: Any) -> int:
        return self.log(*items, level="DEBUG", **kwargs)

    def info(self, *items: Any, **kwargs: Any) -> int:
        return self.log(*items, level="INFO", **kwargs)

    def warning(self, *items: Any, **kwargs: Any) -> int:
        return self.log(*items, level="WARNING", **kwargs)

    def error(self, *items: Any, **kwargs: Any) -> int:
        return self.log(*items, level="ERROR", **kwargs)

    def critical(self, *items: Any, **kwargs: Any) -> int:
        return self.log(*items, level="CRITICAL", **kwargs)

    def exception(self, *items: Any, **kwargs: Any) -> int:
        """ERROR entry with the exception being handled (call inside ``except``)."""
        kwargs.setdefault("exc_info", True)
        return self.log(*items, level="ERROR", **kwargs)

    @contextlib.contextmanager
    def context(self, **ids: Any) -> Iterator[None]:
        """
        Attach ids to every entry logged inside the block (this thread / asyncio
        task only):  ``with audit.context(correlation_id=request_id): ...``
        """
        unknown = set(ids) - set(CONTEXT_FIELDS)
        if unknown:
            raise TypeError(f"context() got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        token = self._context.set(_build.merge_ids(self._context.get(), self._validate_ids(ids, ConfigurationError)))
        try:
            yield
        finally:
            self._context.reset(token)

    def log_record(self, record: logging.LogRecord) -> None:
        """Write a stdlib logging.LogRecord as one entry (used by AuditLogHandler)."""
        self._require_tenant()
        extra = getattr(record, "audit", None)
        item: Dict[str, Any] = dict(extra) if isinstance(extra, Mapping) else {}
        for key in CONTEXT_FIELDS:  # also accept extra={"run_id": ...} directly
            if key not in item and getattr(record, key, None) is not None:
                item[key] = getattr(record, key)
        item.setdefault("message", record.getMessage())
        item.setdefault("logger_name", record.name)
        item.setdefault("logged_at", record.created)
        level = _build.level_from_levelno(record.levelno)
        base_ids = _build.merge_ids(self._defaults, self._context.get())
        thread_id = record.thread if record.thread is not None and record.thread <= 2**63 - 1 else None
        shared = {
            **_build.location(record.pathname, record.funcName, record.lineno),
            "process_id": record.process,
            "process_name": _build.clip("process_name", record.processName),
            "thread_id": thread_id,
            "thread_name": _build.clip("thread_name", record.threadName),
            **_build.exception_fields(record.exc_info),
        }
        self._writer.submit([self._entry_from_item(0, item, level, None, base_ids, shared)])

    # ---- runs (logs.log_run) -------------------------------------------

    def start_run(
        self,
        run_type: Optional[str] = None,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        accelerator_id: Optional[str] = None,
        use_case_id: Optional[str] = None,
        tenant_user_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> LogRun:
        """
        Insert a logs.log_run row with status 'Running' and return it. Written
        synchronously (with retries) so entries that reference its run_id never
        reach the database before it. Raises RunWriteError if it can't be written.
        """
        self._ensure_open()
        self._require_tenant()
        ids = self._validate_ids(
            dict(accelerator_id=accelerator_id, use_case_id=use_case_id, tenant_user_id=tenant_user_id),
            ConfigurationError,
        )
        defaults = _build.merge_ids(self._defaults, self._context.get())
        run = LogRun(
            tenant_id=self.tenant_id,
            environment_id=self.environment_id,
            run_id=_build.as_uuid(run_id, "run_id", ConfigurationError) or new_uuid(),
            accelerator_id=ids.get("accelerator_id", defaults.get("accelerator_id")),
            use_case_id=ids.get("use_case_id", defaults.get("use_case_id")),
            tenant_user_id=ids.get("tenant_user_id", defaults.get("tenant_user_id")),
            run_type=_build.clip("run_type", run_type),
            metadata_json=jsonable(dict(metadata or {})),
        )
        self._write_run(run)
        return run

    def end_run(
        self,
        run: LogRun,
        status: str = "Completed",
        *,
        error_summary: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        flush_timeout: Optional[float] = 10.0,
    ) -> LogRun:
        """
        Mark a run Completed / Failed / Cancelled: sets ended_at, duration_ms,
        error_summary and updated_at, and merges ``metadata`` into metadata_json.
        Flushes the run's queued entries first, so they land before the status
        changes. Ending a run twice is a no-op.
        """
        if status not in RUN_STATUSES or status == "Running":
            raise ConfigurationError(f"status must be one of Completed, Failed, Cancelled; got {status!r}")
        if not run.is_running:
            _log.warning("c66_logger: run %s already ended as %s; ignoring end_run(%s)", run.run_id, run.status, status)
            return run
        self.flush(flush_timeout)
        now = utcnow()
        run.status = status
        run.ended_at = now
        run.updated_at = now
        run.duration_ms = int(round((time.monotonic() - run._started_monotonic) * 1000))
        if error_summary is not None:
            run.error_summary = error_summary[:_ERROR_SUMMARY_MAX]
        if metadata:
            run.metadata_json = {**run.metadata_json, **jsonable(dict(metadata))}
        self._write_run(run)
        return run

    def record_run(
        self,
        run_type: Optional[str] = None,
        *,
        status: str = "Completed",
        run_id: Optional[str] = None,
        started_at: Any = None,
        ended_at: Any = None,
        duration_ms: Optional[int] = None,
        error_summary: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        accelerator_id: Optional[str] = None,
        use_case_id: Optional[str] = None,
        tenant_user_id: Optional[str] = None,
    ) -> LogRun:
        """
        Write a run's state in one call — for work that is reported after the
        fact (a request that already finished, an ingestion job's summary).
        Upserts on run_id, so it can also overwrite a run started earlier with
        start_run(). ``started_at`` defaults to ``ended_at - duration_ms``.
        """
        self._ensure_open()
        self._require_tenant()
        if status not in RUN_STATUSES:
            raise ConfigurationError(f"status must be one of {', '.join(RUN_STATUSES)}; got {status!r}")
        ended = (_build.as_timestamp(ended_at, "ended_at", ConfigurationError) if ended_at is not None
                 else (None if status == "Running" else utcnow()))
        if started_at is not None:
            started = _build.as_timestamp(started_at, "started_at", ConfigurationError)
        elif ended is not None and duration_ms is not None:
            started = ended - dt.timedelta(milliseconds=int(duration_ms))
        else:
            started = ended or utcnow()
        if duration_ms is None and ended is not None:
            duration_ms = int(round((ended - started).total_seconds() * 1000))
        ids = self._validate_ids(
            dict(accelerator_id=accelerator_id, use_case_id=use_case_id, tenant_user_id=tenant_user_id),
            ConfigurationError,
        )
        defaults = _build.merge_ids(self._defaults, self._context.get())
        run = LogRun(
            tenant_id=self.tenant_id,
            environment_id=self.environment_id,
            run_id=_build.as_uuid(run_id, "run_id", ConfigurationError) or new_uuid(),
            accelerator_id=ids.get("accelerator_id", defaults.get("accelerator_id")),
            use_case_id=ids.get("use_case_id", defaults.get("use_case_id")),
            tenant_user_id=ids.get("tenant_user_id", defaults.get("tenant_user_id")),
            run_type=_build.clip("run_type", run_type),
            status=status,
            started_at=started,
            ended_at=ended,
            duration_ms=int(duration_ms) if duration_ms is not None else None,
            error_summary=error_summary[:_ERROR_SUMMARY_MAX] if error_summary else None,
            metadata_json=jsonable(dict(metadata or {})),
            updated_at=utcnow(),
        )
        self._write_run(run)
        return run

    @contextlib.contextmanager
    def run(
        self,
        run_type: Optional[str] = None,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        accelerator_id: Optional[str] = None,
        use_case_id: Optional[str] = None,
        tenant_user_id: Optional[str] = None,
    ) -> Iterator[LogRun]:
        """
        ``with audit.run("nightly_sync") as run:`` — starts a run, attaches its
        run_id (and its accelerator / use case / user) to every entry logged in
        the block, and ends it:

        * block finishes             -> Completed
        * block raises an Exception  -> an ERROR entry with the traceback, then Failed
        * KeyboardInterrupt / task cancellation / SystemExit -> Cancelled

        The exception is always re-raised; c66_logger never swallows it.
        """
        run = self.start_run(run_type, metadata=metadata, accelerator_id=accelerator_id,
                             use_case_id=use_case_id, tenant_user_id=tenant_user_id)
        run_ids = {k: getattr(run, k) for k in ("accelerator_id", "use_case_id", "tenant_user_id")}
        token = self._context.set(_build.merge_ids(self._context.get(), run_ids, {"run_id": run.run_id}))
        try:
            yield run
        except Exception as exc:
            self._log_run_failure(run, exc)
            self._end_quietly(run, "Failed", f"{type(exc).__name__}: {exc}")
            raise
        except BaseException as exc:  # KeyboardInterrupt, asyncio.CancelledError, SystemExit
            self._end_quietly(run, "Cancelled", f"{type(exc).__name__}: {exc}".rstrip(": "))
            raise
        else:
            if run.is_running:  # the block may have ended it explicitly
                self.end_run(run, "Completed")
        finally:
            self._context.reset(token)

    # ---- lifecycle -----------------------------------------------------

    def flush(self, timeout: Optional[float] = 10.0) -> bool:
        """Wait until every entry logged so far is written (or dropped). False on timeout."""
        return self._writer.flush(timeout)

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Write what's queued, stop the writer, release the sink (if we created it)."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        if not self._owns_writer:  # a bound logger: the logger it came from owns writer and sink
            return
        self._writer.close(timeout)
        if self._owns_sink:
            self.sink.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "AuditLogger":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return (f"AuditLogger(tenant_id={self.tenant_id!r}, environment_id={self.environment_id!r}, "
                f"target={self.sink.target_type!r}, closed={self._closed})")

    # ---- internals -----------------------------------------------------

    def _entry_from_item(
        self,
        n: int,
        payload: Dict[str, Any],
        call_level: str,
        call_message: Optional[str],
        base_ids: Dict[str, Any],
        shared: Dict[str, Any],
    ) -> LogEntry:
        item = dict(payload)
        where = f"record {n}"

        for key in ("tenant_id", "environment_id"):
            if key in item:
                value = _build.as_uuid(item.pop(key), f"{where}: {key}", InvalidLogInputError)
                if value is not None and value != getattr(self, key):
                    raise InvalidLogInputError(
                        f"{where}: {key} {value} doesn't match this logger's {key} {getattr(self, key)}"
                    )

        columns = _build.split_known(item, _ITEM_COLUMNS)
        ids = dict(base_ids)
        for key in CONTEXT_FIELDS:
            if key in columns:
                ids[key] = _build.as_uuid(columns[key], f"{where}: {key}", InvalidLogInputError)

        message = columns.get("message", call_message)
        if message is None or not str(message).strip():
            raise InvalidLogInputError(
                f"{where}: 'message' is required (log_entry.message is NOT NULL) — "
                "add a 'message' key to the item or pass message=... to log()"
            )
        level = _build.as_level(columns["level"], InvalidLogInputError) if columns.get("level") else call_level
        logged_at = (
            _build.as_timestamp(columns["logged_at"], f"{where}: logged_at", InvalidLogInputError)
            if columns.get("logged_at") not in (None, "")
            else utcnow()
        )

        metadata = columns.get("metadata_json")
        if isinstance(metadata, str) and metadata.strip():
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                raise InvalidLogInputError(f"{where}: metadata_json is not valid JSON") from None
        if metadata not in (None, "") and not isinstance(metadata, Mapping):
            raise InvalidLogInputError(f"{where}: metadata_json must be an object")
        metadata = {**item, **dict(metadata or {})}

        entry_fields: Dict[str, Any] = dict(
            tenant_id=self.tenant_id,
            environment_id=self.environment_id,
            logger_name=_build.clip("logger_name", str(columns.get("logger_name") or self.logger_name)),
            level=level,
            message=str(message),
            logged_at=logged_at,
            metadata_json=jsonable(metadata),
            **{k: ids.get(k) for k in CONTEXT_FIELDS},
            **shared,
        )
        log_id = _build.as_uuid(columns.get("log_id"), f"{where}: log_id", InvalidLogInputError)
        if log_id:
            entry_fields["log_id"] = log_id
        return LogEntry(**entry_fields)

    @staticmethod
    def _validate_ids(ids: Mapping[str, Any], error: type) -> Dict[str, Any]:
        return {k: _build.as_uuid(v, k, error) for k, v in ids.items() if v is not None}

    def _require_tenant(self) -> None:
        if not self.tenant_id:
            raise ConfigurationError(
                "this AuditLogger has no tenant_id/environment_id; create one with "
                "audit.bind(tenant_id=..., environment_id=...)"
            )

    def _write_run(self, run: LogRun) -> None:
        if run._first_written_at is None:
            run._first_written_at = utcnow()
        attempt = 0
        while True:
            try:
                self.sink.write_run(run)
                return
            except Exception as exc:  # noqa: BLE001 - driver errors vary
                attempt += 1
                if attempt > self._max_retries:
                    raise RunWriteError(
                        f"could not write log_run {run.run_id} (status {run.status}) after {attempt} attempts: {exc}"
                    ) from exc
                time.sleep(min(self._retry_backoff * (2 ** (attempt - 1)), 5.0))

    def _log_run_failure(self, run: LogRun, exc: BaseException) -> None:
        try:
            ids = _build.merge_ids(self._defaults, self._context.get())
            shared = {**_build.raise_site(exc), **_build.process_and_thread(), **_build.exception_fields(exc)}
            entry = self._entry_from_item(
                0, {"message": f"run failed: {type(exc).__name__}: {exc}"[:2000]}, "ERROR", None, ids, shared
            )
            self._writer.submit([entry])
        except Exception:  # never mask the caller's exception
            _log.exception("c66_logger: could not log failure of run %s", run.run_id)

    def _end_quietly(self, run: LogRun, status: str, summary: str) -> None:
        try:
            self.end_run(run, status, error_summary=summary)
        except Exception:  # the original exception matters more than ours
            _log.exception("c66_logger: could not mark run %s as %s", run.run_id, status)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("this AuditLogger is closed")

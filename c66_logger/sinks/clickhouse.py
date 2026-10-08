from __future__ import annotations

import contextlib
import hashlib
import json
import re
import threading
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

from ..exceptions import ConfigurationError, MissingDependencyError, RejectedRecordsError
from ..record import LogEntry, LogRun, utcnow
from .base import BaseSink

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ENTRY_COLUMNS = (
    "log_id", "tenant_id", "environment_id", "run_id", "accelerator_id", "use_case_id",
    "tenant_user_id", "correlation_id", "logger_name", "level", "level_no", "message",
    "module", "func_name", "pathname", "line_no", "process_id", "process_name",
    "thread_id", "thread_name", "exception_type", "exception_message", "exception_traceback",
    "metadata_json", "logged_at",
)
RUN_COLUMNS = (
    "run_id", "tenant_id", "environment_id", "accelerator_id", "use_case_id", "tenant_user_id",
    "run_type", "status", "started_at", "ended_at", "duration_ms", "error_summary",
    "metadata_json", "created_at", "updated_at",
)
_UUID_COLUMNS = {"log_id", "run_id", "tenant_id", "environment_id", "accelerator_id",
                 "use_case_id", "tenant_user_id", "correlation_id"}


def clickhouse_ddl(schema: str = "logs", entry_table: str = "log_entry", run_table: str = "log_run") -> List[str]:
    """
    ClickHouse equivalents of logs.log_entry / logs.log_run (docs/sql/clickhouse_logs_schema.sql).

    * Same column names and meaning as the Postgres tables, and the same
      CHECK constraints on level / status (ClickHouse enforces them on INSERT).
    * No foreign keys — ClickHouse has none — so ids are not checked against app.*.
    * log_entry: ReplacingMergeTree keyed on log_id plus an insert dedup window,
      so a retried batch doesn't leave duplicates.
    * log_run: ReplacingMergeTree(updated_at). Every status change is a new row
      version; read the current state with FINAL (or argMax by updated_at).
    """
    for name in (schema, entry_table, run_table):
        if not _IDENT.match(name):
            raise ConfigurationError(f"invalid identifier {name!r}")
    return [
        f"CREATE DATABASE IF NOT EXISTS {schema}",
        f"""CREATE TABLE IF NOT EXISTS {schema}.{entry_table}
(
    log_id              UUID,
    tenant_id           UUID,
    environment_id      UUID,
    run_id              Nullable(UUID),
    accelerator_id      Nullable(UUID),
    use_case_id         Nullable(UUID),
    tenant_user_id      Nullable(UUID),
    correlation_id      Nullable(UUID),
    logger_name         LowCardinality(String),
    level               LowCardinality(String),
    level_no            Int16,
    message             String,
    module              Nullable(String),
    func_name           Nullable(String),
    pathname            Nullable(String),
    line_no             Nullable(Int32),
    process_id          Nullable(Int32),
    process_name        Nullable(String),
    thread_id           Nullable(Int64),
    thread_name         Nullable(String),
    exception_type      Nullable(String),
    exception_message   Nullable(String),
    exception_traceback Nullable(String),
    metadata_json       String DEFAULT '{{}}',
    logged_at           DateTime64(6, 'UTC'),
    created_at          DateTime64(6, 'UTC') DEFAULT now64(6),
    INDEX ix_run_id run_id TYPE bloom_filter GRANULARITY 4,
    INDEX ix_correlation_id correlation_id TYPE bloom_filter GRANULARITY 4,
    CONSTRAINT chk_log_entry_level CHECK level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(logged_at)
ORDER BY (tenant_id, environment_id, logged_at, log_id)
SETTINGS non_replicated_deduplication_window = 1000""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.{run_table}
(
    run_id          UUID,
    tenant_id       UUID,
    environment_id  UUID,
    accelerator_id  Nullable(UUID),
    use_case_id     Nullable(UUID),
    tenant_user_id  Nullable(UUID),
    run_type        Nullable(String),
    status          LowCardinality(String),
    started_at      DateTime64(6, 'UTC'),
    ended_at        Nullable(DateTime64(6, 'UTC')),
    duration_ms     Nullable(Int32),
    error_summary   Nullable(String),
    metadata_json   String DEFAULT '{{}}',
    created_at      DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at      DateTime64(6, 'UTC'),
    CONSTRAINT chk_log_run_status CHECK status IN ('Running', 'Completed', 'Failed', 'Cancelled')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (tenant_id, environment_id, run_id)""",
    ]


class ClickHouseSink(BaseSink):
    """
    Writes log entries and runs to ClickHouse tables shaped like the Postgres
    ones (see ``clickhouse_ddl()`` / docs/sql/clickhouse_logs_schema.sql).

    Connection — give exactly one of:

    * ``connection``  what the host's connection library hands out:
                      - a clickhouse_connect client (``get_client()`` result,
                        e.g. from c66_clients) — shared, one call at a time
                      - a clickhouse_driver.Client
                      - a zero-argument factory returning either (e.g. the chatbot's
                        ``get_ch_client``); called once, the client is then reused
                      - a c66-data-connection-layer (enterprise_connectors) ClickHouse
                        connector, ``manager.get("logs_ch")``: every write borrows a client
                        with ``connector.connection()`` and hands it back, so the logger
                        uses the library's pool, login and health checks
                      Nothing passed in is ever closed by the sink.
    * ``host`` (+ ``port``, ``username``, ``password``, ``secure``, ``client_options``)
                      — the sink creates its own clickhouse_connect client and
                      closes it on ``close()``.

    A clickhouse_connect client shared with other threads in the host should
    be created with ``autogenerate_session_id=False``; a ClickHouse session
    can only run one query at a time.

    ``create_tables=True`` runs the DDL in ``open()`` (handy for development).
    Otherwise ``open()`` only checks that both tables exist.
    """

    target_type = "clickhouse"

    def __init__(
        self,
        *,
        connection: Any = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: str = "default",
        password: str = "",
        secure: bool = False,
        client_options: Optional[Dict[str, Any]] = None,
        schema: str = "logs",
        entry_table: str = "log_entry",
        run_table: str = "log_run",
        create_tables: bool = False,
    ):
        if (connection is None) == (not host):
            raise ConfigurationError("ClickHouseSink needs exactly one of connection or host")
        self._ddl = clickhouse_ddl(schema, entry_table, run_table)  # validates the identifiers
        self.schema, self.entry_table, self.run_table = schema, entry_table, run_table
        self._create_tables = create_tables
        self._lock = threading.RLock()
        self._client: Any = None
        self._factory: Optional[Callable[[], Any]] = None
        self._lender: Any = None  # enterprise_connectors connector: connection() lends a client
        self._owns_client = connection is None
        if connection is None:
            try:
                import clickhouse_connect
            except ImportError as exc:
                raise MissingDependencyError("clickhouse", "clickhouse") from exc
            options = {"autogenerate_session_id": False, **(client_options or {})}
            self._factory = lambda: clickhouse_connect.get_client(
                host=host, port=port, username=username, password=password, secure=secure, **options)
        elif _is_client(connection):
            self._client = connection
        elif callable(getattr(connection, "connection", None)):
            self._lender = connection
        elif callable(connection):
            self._factory = connection
        elif callable(getattr(connection, "get", None)) and callable(getattr(connection, "names", None)):
            raise ConfigurationError(
                f"got a {type(connection).__name__}; pass one named connection from it, "
                'e.g. connection=manager.get("logs_ch")'
            )
        else:
            raise ConfigurationError(
                f"don't know how to use a {type(connection).__name__} as a ClickHouse connection: pass a "
                "clickhouse_connect client, a clickhouse_driver Client, a connector with a connection() "
                "context manager (enterprise_connectors), or a zero-argument factory"
            )

    # ---- BaseSink --------------------------------------------------------

    def open(self) -> None:
        with self._use_client() as client:
            if self._create_tables:
                for statement in self._ddl:
                    _command(client, statement)
            missing = [f"{self.schema}.{t}" for t in (self.run_table, self.entry_table)
                       if not _exists(client, self.schema, t)]
        if missing:
            raise ConfigurationError(
                f"table(s) {', '.join(missing)} not found. Apply docs/sql/clickhouse_logs_schema.sql "
                "or pass create_tables=True."
            )

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        if not entries:
            return
        rows = []
        for entry in entries:
            row = entry.as_row()
            rows.append([_value(c, row[c]) for c in ENTRY_COLUMNS])
        # same token for the same batch -> a retried insert is deduplicated by ClickHouse
        token = hashlib.sha256(",".join(e.log_id for e in entries).encode()).hexdigest()
        try:
            with self._use_client() as client:
                _insert(client, self.schema, self.entry_table, ENTRY_COLUMNS, rows,
                        {"insert_deduplication_token": token})
        except Exception as exc:
            if "VIOLATED_CONSTRAINT" in str(exc) or "Code: 469" in str(exc):
                raise RejectedRecordsError([(e, str(exc).splitlines()[0][:500]) for e in entries]) from exc
            raise

    def write_run(self, run: LogRun) -> None:
        row = run.as_row()
        row["created_at"] = run._first_written_at or utcnow()
        values = [[_value(c, row[c]) for c in RUN_COLUMNS]]
        with self._use_client() as client:
            _insert(client, self.schema, self.run_table, RUN_COLUMNS, values, None)

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass

    # ---- internals -------------------------------------------------------

    @contextlib.contextmanager
    def _use_client(self) -> Iterator[Any]:
        """A client for one operation. A shared client is used one call at a time (a ClickHouse
        session runs one query at a time); a connector lends each caller its own pooled client."""
        if self._lender is not None:
            with self._lender.connection() as client:
                yield client
            return
        with self._lock:
            yield self._get_client()

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = self._factory()
        return self._client


def _is_client(obj: Any) -> bool:
    return (hasattr(obj, "insert") and hasattr(obj, "command")) or hasattr(obj, "execute")


def _value(column: str, value: Any) -> Any:
    if column == "metadata_json":
        return json.dumps(value or {}, default=str, separators=(",", ":"))
    if column in _UUID_COLUMNS and value is not None:
        return uuid.UUID(str(value))
    return value


def _command(client: Any, sql: str) -> Any:
    if hasattr(client, "command"):
        return client.command(sql)
    return client.execute(sql)


def _exists(client: Any, database: str, table: str) -> bool:
    result = _command(client, f"EXISTS TABLE {database}.{table}")
    if isinstance(result, (list, tuple)):  # clickhouse_driver: [(1,)]
        result = result[0][0] if result else 0
    return str(result).strip() == "1"


def _insert(client: Any, database: str, table: str, columns: Sequence[str], rows: List[list],
            settings: Optional[Dict[str, Any]]) -> None:
    if hasattr(client, "insert") and hasattr(client, "command"):  # clickhouse_connect
        client.insert(table, rows, column_names=list(columns), database=database, settings=settings or None)
    else:  # clickhouse_driver
        client.execute(f"INSERT INTO {database}.{table} ({', '.join(columns)}) VALUES", rows,
                       settings=settings or None)

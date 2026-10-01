from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..exceptions import ConfigurationError, MissingDependencyError, RejectedRecordsError
from ..record import LogEntry, LogRun
from ._connections import ConnectionProvider, OwnedPool, provider_for
from .base import BaseSink

_ENTRY_COLUMNS = (
    "log_id", "tenant_id", "environment_id", "run_id", "accelerator_id", "use_case_id",
    "tenant_user_id", "correlation_id", "logger_name", "level", "level_no", "message",
    "module", "func_name", "pathname", "line_no", "process_id", "process_name",
    "thread_id", "thread_name", "exception_type", "exception_message", "exception_traceback",
    "metadata_json", "logged_at",
)
_RUN_COLUMNS = (
    "run_id", "tenant_id", "environment_id", "accelerator_id", "use_case_id", "tenant_user_id",
    "run_type", "status", "started_at", "ended_at", "duration_ms", "error_summary",
    "metadata_json", "updated_at",
)
_RUN_UPDATE_COLUMNS = ("status", "ended_at", "duration_ms", "error_summary", "metadata_json", "updated_at")
_UUID_COLUMNS = {"log_id", "run_id", "tenant_id", "environment_id", "accelerator_id",
                 "use_case_id", "tenant_user_id", "correlation_id"}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ROWS_PER_STATEMENT = 500


class PostgresSink(BaseSink):
    """
    Writes to the existing ``logs.log_entry`` and ``logs.log_run`` tables
    (docs/sql/logs_schema.sql) over plain DB-API (psycopg2 or psycopg 3). It
    never creates or alters the tables; ``open()`` checks that both exist.

    Connection — give exactly one of:

    * ``connection``  what the host's connection library hands out:
                      - a DB-API connection (psycopg2 / psycopg 3, or a pooled proxy
                        such as c66_clients' get_pg_conn() result), used for the
                        logger's lifetime, one write at a time. Give the logger its own
                        connection: it commits after every write, so it must not
                        share an open transaction with the host.
                      - a pool with getconn()/putconn() (psycopg2.pool, psycopg_pool)
                      - a SQLAlchemy Engine
                      - a zero-argument factory, e.g. ``get_pg_conn`` itself: called
                        for each write and ``.close()``d after (a pooled proxy's
                        close() returns it to the pool)
                      Nothing passed in is ever closed by the sink.
    * ``dsn``         e.g. "postgresql://user:pass@host:5432/db" — the sink opens its
                      own small pool (``pool_size``) and closes it on ``close()``
    * ``host`` + ``database`` (+ ``user``, ``password``, ``port``) — same, from parts
    * ``engine``      kept for v0.3 compatibility; same as ``connection=<Engine>``

    Entries: one transaction per batch, ``ON CONFLICT (log_id) DO NOTHING`` so a
    retried batch never duplicates rows. If the batch hits a foreign-key, CHECK
    or data error (SQLSTATE class 23 / 22), rows are retried one by one so only
    the offending rows are rejected (RejectedRecordsError) and the rest written.

    Runs: ``INSERT ... ON CONFLICT (run_id) DO UPDATE`` of status, ended_at,
    duration_ms, error_summary, metadata_json and updated_at.
    """

    target_type = "postgres"

    def __init__(
        self,
        *,
        connection: Any = None,
        dsn: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 5432,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        engine: Any = None,
        schema: str = "logs",
        entry_table: str = "log_entry",
        run_table: str = "log_run",
        pool_size: int = 4,
        connect_args: Optional[Dict[str, Any]] = None,
    ):
        given = [n for n, v in (("connection", connection), ("engine", engine), ("dsn", dsn), ("host", host))
                 if v is not None and v != ""]
        if len(given) != 1:
            raise ConfigurationError(
                "PostgresSink needs exactly one of connection, dsn, or host(+database); "
                f"got {given or 'none'}"
            )
        if host and not database:
            raise ConfigurationError("PostgresSink: 'database' is required with 'host'")
        for name in (schema, entry_table, run_table):
            if not _IDENT.match(name):
                raise ConfigurationError(f"invalid identifier {name!r}")

        if connection is not None or engine is not None:
            self._provider: ConnectionProvider = provider_for(connection if connection is not None else engine)
        else:
            self._provider = OwnedPool(_connector(dsn, host, port, database, user, password, connect_args), pool_size)

        self.schema = schema
        self._entry_fqn = f'"{schema}"."{entry_table}"'
        self._run_fqn = f'"{schema}"."{run_table}"'
        self._entry_sql_head = (
            f"INSERT INTO {self._entry_fqn} ({', '.join(_quote(c) for c in _ENTRY_COLUMNS)}) VALUES "
        )
        self._entry_row_template = "(" + ", ".join(_placeholder(c) for c in _ENTRY_COLUMNS) + ")"
        self._entry_sql_tail = " ON CONFLICT (log_id) DO NOTHING"
        self._run_sql = (
            f"INSERT INTO {self._run_fqn} ({', '.join(_RUN_COLUMNS)}) VALUES "
            "(" + ", ".join(_placeholder(c) for c in _RUN_COLUMNS) + ") "
            "ON CONFLICT (run_id) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in _RUN_UPDATE_COLUMNS)
        )

    # ---- BaseSink --------------------------------------------------------

    def open(self) -> None:
        with self._provider.acquire() as conn:  # also proves the credentials work
            rows = self._run(conn, "SELECT to_regclass(%s), to_regclass(%s)",
                             (self._run_fqn, self._entry_fqn), fetch=True)
        missing = [name for name, found in zip((self._run_fqn, self._entry_fqn), rows[0]) if found is None]
        if missing:
            raise ConfigurationError(
                f"table(s) {', '.join(missing)} not found. c66_logger doesn't create them; "
                "apply docs/sql/logs_schema.sql first."
            )

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        if not entries:
            return
        with self._provider.acquire() as conn:
            for start in range(0, len(entries), _ROWS_PER_STATEMENT):
                chunk = list(entries[start:start + _ROWS_PER_STATEMENT])
                try:
                    self._insert_entries(conn, chunk)
                except Exception as exc:
                    if not _is_permanent(exc):
                        raise
                    self._isolate_rejected(conn, chunk, exc)

    def write_run(self, run: LogRun) -> None:
        row = run.as_row()
        params = tuple(_param(c, row[c]) for c in _RUN_COLUMNS)
        with self._provider.acquire() as conn:
            self._run(conn, self._run_sql, params)

    def close(self) -> None:
        self._provider.close()

    # ---- internals -------------------------------------------------------

    def _insert_entries(self, conn: Any, entries: List[LogEntry]) -> None:
        params: List[Any] = []
        for entry in entries:
            row = entry.as_row()
            params.extend(_param(c, row[c]) for c in _ENTRY_COLUMNS)
        sql = self._entry_sql_head + ", ".join([self._entry_row_template] * len(entries)) + self._entry_sql_tail
        self._run(conn, sql, params)

    def _isolate_rejected(self, conn: Any, entries: List[LogEntry], batch_exc: Exception) -> None:
        if len(entries) == 1:
            raise RejectedRecordsError([(entries[0], _reason(batch_exc))]) from batch_exc
        rejected: List[Tuple[LogEntry, str]] = []
        written = 0
        for entry in entries:
            try:
                self._insert_entries(conn, [entry])
                written += 1
            except Exception as exc:
                if not _is_permanent(exc):
                    raise
                rejected.append((entry, _reason(exc)))
        if rejected:
            raise RejectedRecordsError(rejected, written)

    @staticmethod
    def _run(conn: Any, sql: str, params: Sequence[Any], fetch: bool = False) -> List[tuple]:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall() if fetch else []
            conn.commit()
            return rows
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass


def _quote(column: str) -> str:
    return f'"{column}"' if column in ("level", "module") else column


def _placeholder(column: str) -> str:
    if column in _UUID_COLUMNS:
        return "%s::uuid"
    if column == "metadata_json":
        return "%s::jsonb"
    return "%s"


def _param(column: str, value: Any) -> Any:
    if column == "metadata_json":
        return json.dumps(value or {}, default=str)
    return value


def _sqlstate(exc: Exception) -> Optional[str]:
    for candidate in (exc, getattr(exc, "orig", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "pgcode", None) or getattr(candidate, "sqlstate", None)
        if code:
            return str(code)
    return None


def _is_permanent(exc: Exception) -> bool:
    """Integrity (23xxx: FK, CHECK, NOT NULL, unique) and data (22xxx) errors can't be fixed by retrying."""
    code = _sqlstate(exc)
    return bool(code) and code[:2] in ("22", "23")


def _reason(exc: Exception) -> str:
    """First line of the database's own message, e.g. 'violates foreign key constraint "fk_log_entry_run"'."""
    orig = getattr(exc, "orig", None) or exc
    return str(orig).strip().splitlines()[0][:500]


def _connector(dsn, host, port, database, user, password, connect_args):
    """A zero-argument function opening one new connection with whichever driver is installed."""
    extra = dict(connect_args or {})
    if dsn:
        dsn = re.sub(r"^postgres(ql)?\+[a-z0-9_]+://", "postgresql://", dsn)
        dsn = re.sub(r"^postgres://", "postgresql://", dsn)
    kwargs = dict(host=host, port=port, dbname=database, user=user, password=password)
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    try:
        import psycopg  # psycopg 3

        return lambda: psycopg.connect(dsn, **extra) if dsn else psycopg.connect(**kwargs, **extra)
    except ImportError:
        pass
    try:
        import psycopg2

        return lambda: psycopg2.connect(dsn, **extra) if dsn else psycopg2.connect(**kwargs, **extra)
    except ImportError as exc:
        raise MissingDependencyError("postgres", "postgres") from exc

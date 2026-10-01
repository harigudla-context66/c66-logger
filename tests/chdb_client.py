"""
A test stand-in for a clickhouse_connect client, backed by a real embedded
ClickHouse engine (chdb).

Rows are serialized by clickhouse_connect's own Native-format writer — the
code path a real ``client.insert()`` uses — and loaded into chdb, so tests
exercise both clickhouse_connect's type conversion (UUID, DateTime64,
Nullable, ...) and ClickHouse itself (DDL, CHECK constraints,
ReplacingMergeTree, insert deduplication).
"""

from __future__ import annotations

import os
import tempfile
import threading
from typing import Any, Dict, List, Optional, Sequence

from chdb import session as chs
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.transform import NativeTransform


_SHARED: Dict[str, Any] = {}
_SHARED_LOCK = threading.Lock()


def _shared_session():
    """chdb allows one embedded server per process, so every ChdbClient shares it."""
    with _SHARED_LOCK:
        if "session" not in _SHARED:
            path = tempfile.mkdtemp(prefix="chdb_")
            _SHARED.update(session=chs.Session(path), dir=path, lock=threading.Lock())
        return _SHARED["session"], _SHARED["dir"], _SHARED["lock"]


class ChdbClient:
    def __init__(self, reset_databases: Sequence[str] = ("logs",)):
        """Each new client starts from an empty `logs` database (tests run one at a time)."""
        self._session, self._dir, self._lock = _shared_session()
        self.inserts: List[Dict[str, Any]] = []
        self.closed = False
        for database in reset_databases:
            self.command(f"DROP DATABASE IF EXISTS {database}")

    # --- the subset of clickhouse_connect.driver.Client that ClickHouseSink uses ---

    def command(self, sql: str) -> Any:
        with self._lock:
            out = str(self._session.query(sql, "TabSeparated")).strip()
        return int(out) if out.isdigit() else out

    def insert(self, table: str, data: Sequence[Sequence[Any]], column_names: Sequence[str] = "*",
               database: str = "", settings: Optional[Dict[str, Any]] = None, **_: Any) -> None:
        types = self._column_types(database, table)
        names = list(column_names)
        ctx = InsertContext(f"{database}.{table}", names, [get_from_name(types[n]) for n in names], data)
        blob = b"".join(NativeTransform().build_insert(ctx))
        # the first block starts with the query text that clickhouse_connect sends
        # as the HTTP body ("INSERT INTO ... FORMAT Native\n"); the rest is Native data
        marker = b"FORMAT Native\n"
        blob = blob[blob.index(marker) + len(marker):]
        fd, path = tempfile.mkstemp(suffix=".native", dir=self._dir)
        with os.fdopen(fd, "wb") as f:
            f.write(blob)
        setting_sql = ""
        if settings:
            setting_sql = " SETTINGS " + ", ".join(f"{k} = '{v}'" for k, v in settings.items())
        cols = ", ".join(names)
        try:
            with self._lock:
                self._session.query(
                    f"INSERT INTO {database}.{table} ({cols}){setting_sql} "
                    f"SELECT {cols} FROM file('{path}', 'Native')"
                )
        finally:
            os.remove(path)
        self.inserts.append({"table": f"{database}.{table}", "rows": len(data), "settings": settings})

    def close(self) -> None:
        self.closed = True

    # --- helpers for assertions ---

    def rows(self, sql: str) -> List[Dict[str, Any]]:
        import json

        with self._lock:
            out = str(self._session.query(sql, "JSONEachRow")).strip()
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    def _column_types(self, database: str, table: str) -> Dict[str, str]:
        rows = self.rows(f"SELECT name, type FROM system.columns WHERE database = '{database}' AND table = '{table}'")
        return {r["name"]: r["type"] for r in rows}

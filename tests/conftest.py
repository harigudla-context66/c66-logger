from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path
from typing import Dict, List, Sequence

import pytest

from c66_logger.record import LogEntry, LogRun
from c66_logger.sinks.base import BaseSink

# Demo ids — the same rows tests/sql/app_stub.sql inserts into app.*
TENANT = "5a1e5f0c-0000-4000-8000-000000000001"
ENV = "e0000000-0000-4000-8000-000000000001"
ACCELERATOR = "acce1e7a-0000-4000-8000-000000000001"
USE_CASE = "0c0c0c0c-0000-4000-8000-000000000001"
USER = "00000000-0000-4000-8000-00000000a11c"
IDS = dict(tenant_id=TENANT, environment_id=ENV)

ROOT = Path(__file__).resolve().parents[1]
PG_DSN = os.environ.get("C66_TEST_POSTGRES_DSN")


class FlakySink(BaseSink):
    """Records batches and runs; can be told to fail the next N entry writes."""

    target_type = "flaky"

    def __init__(self, fail_next: int = 0, **_ignored):
        self.batches: List[List[LogEntry]] = []
        self.runs: Dict[str, LogRun] = {}
        self.opened = False
        self.closed = False
        self._fail_next = fail_next
        self._lock = threading.Lock()

    def fail_next(self, n: int) -> None:
        with self._lock:
            self._fail_next = n

    def open(self) -> None:
        self.opened = True

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        with self._lock:
            if self._fail_next > 0:
                self._fail_next -= 1
                raise RuntimeError("simulated write failure")
            self.batches.append(list(entries))

    def write_run(self, run: LogRun) -> None:
        self.runs[run.run_id] = run

    def close(self) -> None:
        self.closed = True

    @property
    def entries(self) -> List[LogEntry]:
        return [e for b in self.batches for e in b]


@pytest.fixture
def sink() -> FlakySink:
    return FlakySink()


def make_entry(message: str = "hello", **metadata) -> LogEntry:
    return LogEntry(tenant_id=TENANT, environment_id=ENV, logger_name="test", level="INFO",
                    message=message, metadata_json=metadata)


@pytest.fixture(scope="session")
def pg_dsn():
    """A throwaway database with tests/sql/app_stub.sql + docs/sql/logs_schema.sql applied."""
    if not PG_DSN:
        pytest.skip("set C66_TEST_POSTGRES_DSN to run Postgres tests")
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2.extensions import make_dsn, parse_dsn

    name = f"c66_test_{uuid.uuid4().hex[:10]}"
    admin = psycopg2.connect(PG_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    test_dsn = make_dsn(PG_DSN, dbname=name)
    conn = psycopg2.connect(test_dsn)
    with conn, conn.cursor() as cur:
        for sql_file in ("tests/sql/app_stub.sql", "docs/sql/logs_schema.sql"):
            cur.execute((ROOT / sql_file).read_text())
    conn.close()
    params = parse_dsn(test_dsn)
    url = (f"postgresql://{params.get('user', '')}{':' + params['password'] if params.get('password') else ''}"
           f"@{params.get('host', 'localhost')}:{params.get('port', 5432)}/{name}")
    yield url
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
    admin.close()


def pg_rows(dsn, sql, params=()):
    """Run a query with psycopg2 and return dict rows."""
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

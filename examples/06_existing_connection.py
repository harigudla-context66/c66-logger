"""
Case 6 — use the connections your connection library already hands out.

c66_clients (and c66-chatbot's app_common today) give you a pooled psycopg2
connection from get_pg_conn() and a clickhouse_connect client from
get_ch_client(). Pass those — or the functions themselves — as `connection`.
c66_logger writes through them and never closes anything it didn't open.

    pip install -e ".[postgres,clickhouse]"
    python examples/06_existing_connection.py
"""

import threading

import psycopg2
import psycopg2.pool
from _settings import POSTGRES_DSN, TENANT, clickhouse_client

from c66_logger import AuditLogger

# --- what a connection library like c66_clients looks like (simplified) ---------

_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, POSTGRES_DSN)


class PooledPgConn:
    """Like the chatbot's _PooledPgConnProxy: close() returns the connection to the pool."""

    def __init__(self):
        object.__setattr__(self, "_real", _pool.getconn())

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)

    def close(self):
        self._real.rollback()
        _pool.putconn(self._real)


def get_pg_conn():
    return PooledPgConn()


_ch = None
_ch_lock = threading.Lock()


def get_ch_client():
    global _ch
    with _ch_lock:
        if _ch is None:
            _ch = clickhouse_client()
        return _ch


# --- handing them to c66_logger --------------------------------------------------

def postgres_examples() -> None:
    # 1. The factory: c66_logger calls get_pg_conn() per write and close()s it after,
    #    which puts it back in the pool. The recommended way for pooled connections.
    with AuditLogger(**TENANT, target_type="postgres", connection=get_pg_conn, logger_name="via_factory") as audit:
        with audit.run("factory_demo"):
            audit.info({"message": "written through get_pg_conn()", "pool": "c66_clients"})

    # 2. The pool itself (anything with getconn()/putconn()).
    with AuditLogger(**TENANT, target_type="postgres", connection=_pool, logger_name="via_pool") as audit:
        audit.info("written through the pool")

    # 3. One connection object, dedicated to the logger for its lifetime.
    #    c66_logger commits after each write, so don't share it with an open
    #    transaction of your own.
    dedicated = psycopg2.connect(POSTGRES_DSN)
    with AuditLogger(**TENANT, target_type="postgres", connection=dedicated, logger_name="via_connection") as audit:
        audit.info("written through one dedicated connection")
    print("postgres: connection still open after the logger closed:", dedicated.closed == 0)
    dedicated.close()  # yours to close


def clickhouse_examples() -> None:
    # The client object, or get_ch_client itself (called once, then reused).
    client = get_ch_client()
    for target in (client, get_ch_client):
        with AuditLogger(**TENANT, target_type="clickhouse",
                         connection={"connection": target, "create_tables": True},
                         logger_name="via_clickhouse") as audit:
            with audit.run("clickhouse_demo"):
                audit.info({"message": "written through the shared clickhouse client"})
    rows = client.rows("SELECT logger_name, count() AS n FROM logs.log_entry FINAL GROUP BY logger_name") \
        if hasattr(client, "rows") else client.query("SELECT logger_name, count() FROM logs.log_entry FINAL "
                                                       "GROUP BY logger_name").result_rows
    print("clickhouse:", rows)


if __name__ == "__main__":
    postgres_examples()
    clickhouse_examples()
    _pool.closeall()

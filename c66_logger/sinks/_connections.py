"""
Ways a sink can get a database connection.

The host application usually already has a connection library (c66_clients:
pooled psycopg2 connections, a clickhouse_connect client). Sinks accept what
that library hands out and never close anything they didn't create:

    object with .cursor()             one DB-API connection, used for the logger's
                                      lifetime and serialized with a lock
    object with .getconn/.putconn     a connection pool (psycopg2.pool, psycopg_pool):
                                      borrow per write, give back after
    object with .raw_connection()     a SQLAlchemy Engine: borrow per write
    object with .connection()         a connector whose connection() is a context
                                      manager lending a pooled connection, e.g.
                                      enterprise_connectors' manager.get("logs_db"):
                                      borrow per write, returned when the block ends
    zero-argument callable            a factory such as c66_clients' get_pg_conn():
                                      called per write; .close() afterwards, which
                                      for a pooled proxy returns it to its pool
"""

from __future__ import annotations

import contextlib
import queue
import threading
from typing import Any, Callable, Iterator, Optional

from ..exceptions import ConfigurationError


class ConnectionProvider:
    """Hands out a DB-API connection for the duration of one write."""

    owns_connections = False

    @contextlib.contextmanager
    def acquire(self) -> Iterator[Any]:  # pragma: no cover - interface
        raise NotImplementedError
        yield

    def discard(self, conn: Any) -> None:
        """Called when a borrowed connection looks broken."""

    def close(self) -> None:
        """Release anything this provider created."""


class SharedConnection(ConnectionProvider):
    """One caller-owned connection, used by every write, one write at a time."""

    def __init__(self, conn: Any):
        self._conn = conn
        self._lock = threading.RLock()

    @contextlib.contextmanager
    def acquire(self):
        with self._lock:
            yield self._conn


class PoolConnections(ConnectionProvider):
    """A pool exposing getconn()/putconn() — psycopg2.pool.*, psycopg_pool.ConnectionPool."""

    def __init__(self, pool: Any):
        self._pool = pool

    @contextlib.contextmanager
    def acquire(self):
        conn = self._pool.getconn()
        broken = False
        try:
            yield conn
        except Exception:
            broken = _looks_broken(conn)
            raise
        finally:
            try:
                if broken:
                    try:
                        self._pool.putconn(conn, close=True)  # psycopg2 pools
                    except TypeError:
                        self._pool.putconn(conn)             # psycopg_pool checks state itself
                else:
                    self._pool.putconn(conn)
            except Exception:
                pass


class FactoryConnections(ConnectionProvider):
    """A zero-argument callable returning a connection; closed after each write."""

    def __init__(self, factory: Callable[[], Any]):
        self._factory = factory

    @contextlib.contextmanager
    def acquire(self):
        conn = self._factory()
        try:
            yield conn
        finally:
            try:
                conn.close()  # pooled proxies return the connection to their pool here
            except Exception:
                pass


class LeasedConnections(ConnectionProvider):
    """A connector whose ``connection()`` lends a pooled connection as a context manager.

    This is the shape of c66-data-connection-layer (``enterprise_connectors``):
    ``with connector.connection() as conn:`` checks a psycopg connection out of
    its pool and returns it when the block ends. The library resets the
    connection on return (rolls back an open transaction, drops a broken one),
    so nothing else is needed here. The connector itself is never closed.
    """

    def __init__(self, source: Any):
        self._source = source

    @contextlib.contextmanager
    def acquire(self):
        with self._source.connection() as conn:
            yield conn


class OwnedPool(ConnectionProvider):
    """A small pool the sink creates itself from a DSN / connection parameters."""

    owns_connections = True

    def __init__(self, connect: Callable[[], Any], size: int):
        if size < 1:
            raise ConfigurationError("pool_size must be >= 1")
        self._connect = connect
        self._idle: "queue.LifoQueue[Any]" = queue.LifoQueue()
        self._slots = threading.BoundedSemaphore(size)
        self._closed = False

    @contextlib.contextmanager
    def acquire(self):
        self._slots.acquire()
        conn = None
        try:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                conn = self._connect()
            yield conn
        except Exception:
            if conn is not None and _looks_broken(conn):
                _close_quietly(conn)
                conn = None
            raise
        finally:
            if conn is not None:
                if self._closed:
                    _close_quietly(conn)
                else:
                    self._idle.put(conn)
            self._slots.release()

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                _close_quietly(self._idle.get_nowait())
            except queue.Empty:
                break


def provider_for(connection: Any) -> ConnectionProvider:
    """Pick the right provider for whatever the caller passed as ``connection``."""
    if hasattr(connection, "cursor"):
        return SharedConnection(connection)
    if hasattr(connection, "getconn") and hasattr(connection, "putconn"):
        return PoolConnections(connection)
    if hasattr(connection, "raw_connection"):  # SQLAlchemy Engine
        return FactoryConnections(connection.raw_connection)
    if callable(getattr(connection, "connection", None)):  # enterprise_connectors connector
        return LeasedConnections(connection)
    if callable(connection):
        return FactoryConnections(connection)
    if callable(getattr(connection, "get", None)) and callable(getattr(connection, "names", None)):
        raise ConfigurationError(
            f"got a {type(connection).__name__}; pass one named connection from it, "
            'e.g. connection=manager.get("logs_db")'
        )
    raise ConfigurationError(
        f"don't know how to use a {type(connection).__name__} as a Postgres connection: pass a DB-API "
        "connection, a pool with getconn()/putconn(), a SQLAlchemy Engine, a connector with a "
        "connection() context manager (enterprise_connectors), or a zero-argument factory"
    )


def _looks_broken(conn: Any) -> bool:
    closed = getattr(conn, "closed", 0)
    if closed:
        return True
    broken = getattr(conn, "broken", False)  # psycopg 3
    return bool(broken)


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass

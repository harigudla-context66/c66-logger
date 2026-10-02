"""
Logging through c66-data-connection-layer (``enterprise_connectors``).

That library hands out a connector per named connection; ``with
connector.connection() as conn`` lends a pooled psycopg 3 connection. These
tests pass the connector itself as ``connection=``.

The fake-connector tests always run. The rest need the library
(``pip install -e "<c66-data-connection-layer>[postgres,yaml]"``) and
C66_TEST_POSTGRES_DSN.
"""

import contextlib
import threading
import uuid
from urllib.parse import urlparse

import pytest

from c66_logger import AuditLogger, ConfigurationError, Telemetry
from c66_logger.sinks._connections import LeasedConnections, provider_for

from .conftest import IDS, pg_rows


UNKNOWN_USER_ID = "99999999-9999-4999-8999-999999999999"


# ---------- shape only: a stand-in connector ----------

class FakeConnector:
    """Same surface as enterprise_connectors.BaseConnector: connection() lends a connection."""

    def __init__(self):
        self.leased = 0
        self.returned = 0
        self.closed = False

    @contextlib.contextmanager
    def connection(self):
        self.leased += 1
        try:
            yield object()
        finally:
            self.returned += 1

    def connect(self):  # unpooled; c66_logger must not pick this
        raise AssertionError("connect() should not be used")

    def close(self):
        self.closed = True


class FakeManager:
    def get(self, name):
        return FakeConnector()

    def names(self):
        return ["logs_db"]


def test_connector_is_used_through_its_connection_context_manager():
    connector = FakeConnector()
    provider = provider_for(connector)
    assert isinstance(provider, LeasedConnections)
    with provider.acquire():
        pass
    with pytest.raises(RuntimeError), provider.acquire():
        raise RuntimeError("write failed")
    assert (connector.leased, connector.returned) == (2, 2)  # returned even when the write fails
    provider.close()
    assert not connector.closed  # the caller's connector is never closed


def test_passing_the_manager_says_which_object_to_pass():
    with pytest.raises(ConfigurationError, match=r'manager\.get\("logs_db"\)'):
        provider_for(FakeManager())


# ---------- the real library against Postgres ----------

@pytest.fixture(scope="module")
def ec():
    return pytest.importorskip("enterprise_connectors")


def _manager_and_connector(ec, dsn, **pool):
    url = urlparse(dsn)
    manager = ec.ConnectorManager()
    connector = manager.create(
        "postgres",
        {"host": url.hostname, "port": url.port or 5432, "database": url.path.lstrip("/"),
         "application_name": "c66_logger-tests"},
        ec.UsernamePassword(username=url.username or "postgres", password=url.password or "unused"),
        pool_config={"max_size": 3, **pool},
    )
    return manager, connector


def _pool_stats(manager):
    (stats,) = manager.pool_manager.stats().values()
    return stats


@pytest.mark.integration
def test_logs_through_an_enterprise_connector(ec, pg_dsn):
    manager, connector = _manager_and_connector(ec, pg_dsn)
    dropped = []
    try:
        with AuditLogger(**IDS, target_type="postgres", connection=connector, logger_name="dcl_test",
                         flush_interval=0.1, on_drop=lambda e, why: dropped.append(why)) as audit:
            with audit.run("dcl_test") as run:
                audit.info({"message": "via enterprise_connectors", "n": 1}, "message\nsecond row")
                audit.info({"message": "unknown user", "tenant_user_id": UNKNOWN_USER_ID})
        rows = pg_rows(pg_dsn, "SELECT message FROM logs.log_entry WHERE run_id = %s ORDER BY logged_at",
                       (run.run_id,))
        status = pg_rows(pg_dsn, "SELECT status FROM logs.log_run WHERE run_id = %s", (run.run_id,))[0]["status"]
        assert [r["message"] for r in rows] == ["via enterprise_connectors", "second row"]
        assert status == "Completed"
        assert len(dropped) == 1 and "fk_log_entry_tenant_user" in dropped[0]  # only the bad row

        stats = _pool_stats(manager)
        assert stats.in_use == 0 and stats.total <= 3  # every connection went back to her pool
        assert not connector.closed  # audit.close() leaves the connector to its owner
        with connector.connection() as conn:  # and the app can still use it
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        manager.close()


@pytest.mark.integration
def test_many_threads_share_the_connector_pool(ec, pg_dsn):
    manager, connector = _manager_and_connector(ec, pg_dsn, max_size=2)
    marker = uuid.uuid4().hex
    try:
        with AuditLogger(**IDS, target_type="postgres", connection=connector, mode="sync") as audit:
            def worker(n):
                for i in range(15):
                    audit.info({"message": f"t{n}-{i}", "m": marker})

            threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        n = pg_rows(pg_dsn, "SELECT count(*) AS n FROM logs.log_entry WHERE metadata_json->>'m' = %s",
                    (marker,))[0]["n"]
        assert n == 90
        stats = _pool_stats(manager)
        assert stats.in_use == 0 and stats.total <= 2
    finally:
        manager.close()


@pytest.mark.integration
def test_connections_yaml_with_env_references(ec, pg_dsn, tmp_path):
    """The way the guide sets it up: connections.yaml + env vars, then manager.get(name)."""
    pytest.importorskip("yaml")
    url = urlparse(pg_dsn)
    (tmp_path / "connections.yaml").write_text(
        "connections:\n"
        "  logs_db:\n"
        "    type: postgres\n"
        "    config: {host: env://LOGS_HOST, port: env://LOGS_PORT, database: env://LOGS_DB}\n"
        "    credentials: {username: env://LOGS_USER, password: env://LOGS_PASSWORD}\n"
        "    pool: {max_size: 2}\n"
    )
    environ = {"LOGS_HOST": url.hostname, "LOGS_PORT": str(url.port or 5432), "LOGS_DB": url.path.lstrip("/"),
               "LOGS_USER": url.username or "postgres", "LOGS_PASSWORD": url.password or "unused"}
    manager = ec.ConnectorManager.from_yaml(tmp_path / "connections.yaml", environ=environ)
    try:
        assert manager.load_errors == {}
        with AuditLogger(**IDS, target_type="postgres", connection=manager.get("logs_db"), mode="sync") as audit:
            audit.info(message="from connections.yaml")
        rows = pg_rows(pg_dsn, "SELECT count(*) AS n FROM logs.log_entry WHERE message = 'from connections.yaml'")
        assert rows[0]["n"] == 1
    finally:
        manager.close()


@pytest.mark.integration
def test_chatbot_telemetry_through_the_connector(ec, pg_dsn):
    manager, connector = _manager_and_connector(ec, pg_dsn)
    request_id = str(uuid.uuid4())
    try:
        root = AuditLogger(target_type="postgres", connection=connector)
        telemetry = Telemetry(root, resolve_tenant={"acme": (IDS["tenant_id"], IDS["environment_id"])}.get)
        telemetry.log_llm_call(request_id, "acme", 1, "answer", "anthropic", 1000, 200)
        telemetry.log_event("acme", "/api/chat", "hello?", "graph_traversal", latency_ms=900,
                            request_id=request_id)
        telemetry.flush()
        root.close()
        run = pg_rows(pg_dsn, "SELECT run_type, status FROM logs.log_run WHERE run_id = %s", (request_id,))
        entries = pg_rows(pg_dsn, "SELECT logger_name FROM logs.log_entry WHERE run_id = %s", (request_id,))
        assert run == [{"run_type": "chat_request", "status": "Completed"}]
        assert [e["logger_name"] for e in entries] == ["c66.llm"]
    finally:
        manager.close()

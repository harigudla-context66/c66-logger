"""
Logging to ClickHouse through c66-data-connection-layer (``enterprise_connectors``).

Its ClickHouse connector lends one clickhouse_connect client per caller
(``with connector.connection() as client``) from an exclusive pool. These
tests pass the connector itself as ``connection=``.

ClickHouse here is the embedded engine (chdb) behind tests/chdb_client.py.
For the library's own connector, ``clickhouse_connect.get_client`` is pointed
at that engine, so everything the library does (login check, pooling, health
checks, leasing) runs for real; only the HTTP hop to a server is skipped.
With C66_TEST_CLICKHOUSE_HOST set, the last test also runs against that server.
"""

import contextlib
import itertools
import os
import threading
import uuid

import pytest

from c66_logger import AuditLogger, ConfigurationError, Telemetry
from c66_logger.sinks.clickhouse import ClickHouseSink

from .conftest import ENV, IDS, TENANT

pytest.importorskip("clickhouse_connect")
pytest.importorskip("chdb")
from .chdb_client import ChdbClient  # noqa: E402

CH_HOST = os.environ.get("C66_TEST_CLICKHOUSE_HOST")
OTHER_TENANT = ("4b0b5907-0000-4000-8000-000000000002", "e0000000-0000-4000-8000-000000000002")


def _reader():
    return ChdbClient()  # also gives every test an empty `logs` database


# ---------- shape only: a stand-in connector ----------

class LendingConnector:
    """Same surface as enterprise_connectors' ClickHouse connector: connection() lends a client."""

    def __init__(self, client):
        self.client = client
        self.leased = 0
        self.returned = 0
        self.closed = False

    @contextlib.contextmanager
    def connection(self):
        self.leased += 1
        try:
            yield self.client
        finally:
            self.returned += 1

    def close(self):
        self.closed = True


class FakeManager:
    def get(self, name):
        return LendingConnector(None)

    def names(self):
        return ["logs_ch"]


def test_each_write_borrows_a_client_and_gives_it_back():
    reader = _reader()
    connector = LendingConnector(ChdbClient(reset_databases=()))
    with AuditLogger(**IDS, target_type="clickhouse", mode="sync",
                     connection={"connection": connector, "create_tables": True}) as audit:
        with audit.run("lent_clients"):
            audit.info({"message": "through a lent client", "n": 1})
    assert connector.leased >= 4 and connector.leased == connector.returned  # open, run x2, entry
    assert not connector.closed and not connector.client.closed
    assert [r["message"] for r in reader.rows("SELECT message FROM logs.log_entry")] == ["through a lent client"]
    assert reader.rows("SELECT status FROM logs.log_run FINAL") == [{"status": "Completed"}]


def test_passing_the_manager_says_which_object_to_pass():
    with pytest.raises(ConfigurationError, match=r'manager\.get\("logs_ch"\)'):
        ClickHouseSink(connection=FakeManager())


# ---------- the real enterprise_connectors ClickHouse connector ----------

@pytest.fixture
def ec():
    ec = pytest.importorskip("enterprise_connectors")
    pytest.importorskip("enterprise_connectors.connectors.warehouses.clickhouse")
    return ec


class SessionClient(ChdbClient):
    """What clickhouse_connect.get_client() returns, minus the HTTP: params carry the session id."""

    _ids = itertools.count(1)

    def __init__(self, **params):
        super().__init__(reset_databases=())
        self.login = {k: params[k] for k in ("host", "port", "username", "database")}
        self.params = {"session_id": f"session-{next(self._ids)}"}


@pytest.fixture
def chdb_driver(monkeypatch):
    import clickhouse_connect

    created = []

    def get_client(**params):
        client = SessionClient(**params)
        created.append(client)
        return client

    monkeypatch.setattr(clickhouse_connect, "get_client", get_client)
    return created


def _connector(ec, **pool):
    manager = ec.ConnectorManager()
    connector = manager.create(
        "clickhouse",
        {"host": "clickhouse.internal", "port": 8123, "database": "default", "client_name": "c66_logger-tests"},
        ec.UsernamePassword(username="logger", password="s3cret"),
        pool_config={"max_size": 2, **pool},
        connection_name="logs_ch",
    )
    return manager, connector


def _stats(manager):
    (stats,) = manager.pool_manager.stats().values()
    return stats


def test_logs_through_the_clickhouse_connector(ec, chdb_driver):
    reader = _reader()
    manager, connector = _connector(ec)
    try:
        assert connector.test_connection(), connector.last_result
        with AuditLogger(**IDS, target_type="clickhouse", logger_name="ch_connector", flush_interval=0.1,
                         connection={"connection": connector, "create_tables": True}) as audit:
            with audit.run("ch_connector_run", metadata={"via": "enterprise_connectors"}) as run:
                audit.info({"message": "first", "n": 1}, "message\nsecond")
                audit.warning({"message": "third"})
            failed = None
            with contextlib.suppress(ValueError), audit.run("ch_connector_fail") as failed:
                raise ValueError("boom")

        entries = reader.rows(
            f"SELECT message, level FROM logs.log_entry WHERE run_id = '{run.run_id}' ORDER BY logged_at")
        assert [(e["message"], e["level"]) for e in entries] == [("first", "INFO"), ("second", "INFO"),
                                                                 ("third", "WARNING")]
        runs = {r["run_type"]: r["status"] for r in reader.rows("SELECT run_type, status FROM logs.log_run FINAL")}
        assert runs == {"ch_connector_run": "Completed", "ch_connector_fail": "Failed"}
        assert reader.rows(f"SELECT count() AS n FROM logs.log_entry WHERE run_id = '{failed.run_id}'")[0]["n"] == 1

        stats = _stats(manager)
        assert stats.in_use == 0 and stats.total <= 2               # everything went back to the pool
        assert all(c.login["username"] == "logger" for c in chdb_driver)  # clients made by the library's login
        assert not any(c.closed for c in chdb_driver[1:])           # c66_logger closed none of them
        with connector.connection() as client:                      # the app keeps using the connector
            assert client.command("SELECT 1") == 1
    finally:
        manager.close()
    assert all(c.closed for c in chdb_driver)                       # the library closes them, not us


def test_threads_share_the_pool_one_client_each(ec, chdb_driver):
    reader = _reader()
    manager, connector = _connector(ec, max_size=2)
    marker = uuid.uuid4().hex
    try:
        with AuditLogger(**IDS, target_type="clickhouse", mode="sync",
                         connection={"connection": connector, "create_tables": True}) as audit:
            def worker(n):
                for i in range(10):
                    audit.info({"message": f"t{n}-{i}", "m": marker})

            threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        n = reader.rows(f"SELECT count() AS n FROM logs.log_entry "
                        f"WHERE JSONExtractString(metadata_json, 'm') = '{marker}'")[0]["n"]
        assert n == 60
        stats = _stats(manager)
        assert stats.in_use == 0 and stats.total <= 2
        assert len({c.params["session_id"] for c in chdb_driver}) <= 3  # test_connection's + at most 2 pooled
    finally:
        manager.close()


def test_chatbot_telemetry_through_the_clickhouse_connector(ec, chdb_driver):
    reader = _reader()
    manager, connector = _connector(ec)
    request_id = str(uuid.uuid4())
    try:
        root = AuditLogger(target_type="clickhouse", connection={"connection": connector, "create_tables": True})
        telemetry = Telemetry(root, resolve_tenant={"acme": (TENANT, ENV), "globex": OTHER_TENANT}.get)
        telemetry.log_llm_call(request_id, "acme", 1, "answer", "anthropic", 1000, 200)
        telemetry.log_graph_query(request_id, "acme", "graph_traversal", nodes_returned=3)
        telemetry.log_event("acme", "/api/chat", "hello?", "graph_traversal", latency_ms=900, request_id=request_id)
        telemetry.log_audit_event("alice", "globex", "purge_tenant")
        telemetry.flush()
        root.close()
        run = reader.rows(f"SELECT run_type, status FROM logs.log_run FINAL WHERE run_id = '{request_id}'")
        assert run == [{"run_type": "chat_request", "status": "Completed"}]
        names = sorted(r["logger_name"] for r in reader.rows("SELECT logger_name FROM logs.log_entry"))
        assert names == ["c66.audit", "c66.graph", "c66.llm"]
        assert _stats(manager).in_use == 0
    finally:
        manager.close()


@pytest.mark.integration
def test_against_a_real_clickhouse_server(ec):
    if not CH_HOST:
        pytest.skip("set C66_TEST_CLICKHOUSE_HOST to run against a real ClickHouse server")
    schema = f"c66_test_{uuid.uuid4().hex[:8]}"
    manager = ec.ConnectorManager()
    connector = manager.create(
        "clickhouse",
        {"host": CH_HOST, "port": int(os.environ.get("C66_TEST_CLICKHOUSE_PORT", "8123")),
         "secure": os.environ.get("C66_TEST_CLICKHOUSE_SECURE", "false").lower() == "true"},
        ec.UsernamePassword(username=os.environ.get("C66_TEST_CLICKHOUSE_USER", "default"),
                            password=os.environ.get("C66_TEST_CLICKHOUSE_PASSWORD", "")),
    )
    try:
        with AuditLogger(**IDS, target_type="clickhouse", mode="sync",
                         connection={"connection": connector, "schema": schema, "create_tables": True}) as audit:
            with audit.run("server_check"):
                audit.info("hello from c66_logger via enterprise_connectors")
        with connector.connection() as client:
            assert client.command(f"SELECT count() FROM {schema}.log_entry") == 1
            assert client.command(f"SELECT status FROM {schema}.log_run FINAL") == "Completed"
            client.command(f"DROP DATABASE {schema}")
    finally:
        manager.close()

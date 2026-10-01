"""
ClickHouse sink tests.

Always: an embedded ClickHouse engine (chdb, ClickHouse 26.x) behind a client
that serializes rows with clickhouse_connect's real Native writer — see
tests/chdb_client.py. When C66_TEST_CLICKHOUSE_HOST is set, the same checks
also run against that server through a real clickhouse_connect client.
"""

import os
import uuid
from pathlib import Path

import pytest

from c66_logger import AuditLogger, ConfigurationError
from c66_logger.record import LogRun
from c66_logger.sinks.clickhouse import ClickHouseSink, clickhouse_ddl

from .conftest import ENV, IDS, TENANT, USER, make_entry

pytest.importorskip("clickhouse_connect")
pytest.importorskip("chdb")
from .chdb_client import ChdbClient  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CH_HOST = os.environ.get("C66_TEST_CLICKHOUSE_HOST")


class RealClient:
    """Thin wrapper so assertions read rows the same way as ChdbClient."""

    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        return getattr(self.client, name)

    def rows(self, sql):
        result = self.client.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]


@pytest.fixture(params=["chdb", "server"])
def ch(request):
    if request.param == "chdb":
        yield ChdbClient(), "logs"
        return
    if not CH_HOST:
        pytest.skip("set C66_TEST_CLICKHOUSE_HOST to run against a real ClickHouse server")
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=CH_HOST, port=int(os.environ.get("C66_TEST_CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("C66_TEST_CLICKHOUSE_USER", "default"),
        password=os.environ.get("C66_TEST_CLICKHOUSE_PASSWORD", ""),
        autogenerate_session_id=False,
    )
    schema = f"c66_test_{uuid.uuid4().hex[:8]}"
    yield RealClient(client), schema
    client.command(f"DROP DATABASE IF EXISTS {schema}")


def _logger(client, schema, **kw):
    kw.setdefault("mode", "sync")
    return AuditLogger(**IDS, target_type="clickhouse",
                       connection={"connection": client, "schema": schema, "create_tables": True}, **kw)


def test_docs_ddl_matches_the_code():
    expected = ";\n\n".join(clickhouse_ddl()) + ";\n"
    text = (ROOT / "docs/sql/clickhouse_logs_schema.sql").read_text()
    assert expected in text, "docs/sql/clickhouse_logs_schema.sql is out of date: regenerate from clickhouse_ddl()"


def test_config_errors():
    with pytest.raises(ConfigurationError):
        ClickHouseSink()
    with pytest.raises(ConfigurationError):
        ClickHouseSink(connection=ChdbClient(), host="h")
    with pytest.raises(ConfigurationError):
        ClickHouseSink(connection=42)
    with pytest.raises(ConfigurationError):
        ClickHouseSink(connection=ChdbClient(), schema="bad-name")


def test_missing_tables_fail_fast():
    with pytest.raises(ConfigurationError, match="clickhouse_logs_schema.sql"):
        AuditLogger(**IDS, target_type="clickhouse", connection=ChdbClient())


def test_entries_and_run_round_trip(ch):
    client, schema = ch
    with _logger(client, schema, logger_name="order_service", tenant_user_id=USER) as audit:
        with audit.run("nightly_sync", metadata={"source": "crm"}) as run:
            audit.info({"message": "order created", "order_id": 1}, "message,level\nslow,WARNING")
            try:
                raise KeyError("sku")
            except KeyError:
                audit.exception(message="lookup failed")
    rows = client.rows(f"SELECT * FROM {schema}.log_entry FINAL ORDER BY logged_at")
    assert [r["message"] for r in rows] == ["order created", "slow", "lookup failed"]
    first = rows[0]
    assert str(first["tenant_id"]) == TENANT and str(first["environment_id"]) == ENV
    assert str(first["run_id"]) == run.run_id and str(first["tenant_user_id"]) == USER
    assert (first["logger_name"], first["level"], first["level_no"]) == ("order_service", "INFO", 20)
    assert first["metadata_json"] == '{"order_id":1}'
    assert first["func_name"] == "test_entries_and_run_round_trip" and first["line_no"] > 0
    assert first["correlation_id"] is None and first["created_at"]
    assert rows[1]["level_no"] == 30 and rows[2]["exception_type"] == "KeyError"

    runs = client.rows(f"SELECT * FROM {schema}.log_run FINAL")
    assert len(runs) == 1
    r = runs[0]
    assert (r["run_type"], r["status"], r["metadata_json"]) == ("nightly_sync", "Completed", '{"source":"crm"}')
    assert r["duration_ms"] >= 0 and r["ended_at"] is not None


def test_run_versions_keep_first_created_at(ch):
    client, schema = ch
    audit = _logger(client, schema)
    run = audit.start_run("job")
    audit.end_run(run, "Failed", error_summary="boom")
    versions = client.rows(f"SELECT status, created_at, updated_at FROM {schema}.log_run ORDER BY updated_at")
    assert [v["status"] for v in versions] == ["Running", "Failed"]
    assert versions[0]["created_at"] == versions[1]["created_at"]
    latest = client.rows(f"SELECT status, error_summary FROM {schema}.log_run FINAL")
    assert latest == [{"status": "Failed", "error_summary": "boom"}]
    audit.close()


def test_retried_batch_is_deduplicated(ch):
    client, schema = ch
    audit = _logger(client, schema)
    batch = [make_entry(f"d{i}", k="dedup") for i in range(5)]
    audit.sink.write_batch(batch)
    audit.sink.write_batch(batch)  # a retry after a lost response
    plain = client.rows(f"SELECT count() AS n FROM {schema}.log_entry WHERE logger_name = 'test'")
    assert int(plain[0]["n"]) == 5  # rejected by insert_deduplication_token, not just collapsed by FINAL
    audit.close()


def test_check_constraint_is_enforced(ch):
    client, schema = ch
    audit = _logger(client, schema)
    with pytest.raises(Exception, match="VIOLATED_CONSTRAINT|469"):
        client.command(
            f"INSERT INTO {schema}.log_entry (log_id, tenant_id, environment_id, logger_name, level, level_no, "
            f"message, logged_at) VALUES (generateUUIDv4(), '{TENANT}', '{ENV}', 'x', 'AUDIT', 20, 'm', now64(6))"
        )
    audit.close()


def test_constraint_violation_is_rejected_not_retried():
    from c66_logger.exceptions import RejectedRecordsError

    client = ChdbClient()
    sink = ClickHouseSink(connection=client, create_tables=True)
    sink.open()
    calls = []

    def failing_insert(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("Code: 469. DB::Exception: Constraint `chk_log_entry_level` ... (VIOLATED_CONSTRAINT)")

    client.insert = failing_insert
    with pytest.raises(RejectedRecordsError):
        sink.write_batch([make_entry("x")])
    assert calls == [1]


def test_factory_is_called_once_and_never_closed():
    client = ChdbClient()
    for statement in clickhouse_ddl():
        client.command(statement)
    calls = []

    def get_ch_client():
        calls.append(1)
        return client

    with AuditLogger(**IDS, target_type="clickhouse", connection=get_ch_client, mode="sync") as audit:
        for i in range(5):
            audit.info(message=f"m{i}")
    assert calls == [1] and client.closed is False
    assert int(client.rows("SELECT count() AS n FROM logs.log_entry")[0]["n"]) == 5


def test_clickhouse_driver_style_client():
    """clickhouse_driver.Client has execute(), not insert()/command()."""
    backing = ChdbClient()
    executed = []

    class DriverStyle:
        def execute(self, sql, params=None, settings=None):
            executed.append(sql.split("(")[0].strip())
            if params is None:
                out = backing.command(sql)
                return [(out,)]
            table = sql.split()[2]
            database, name = table.split(".")
            cols = [c.strip() for c in sql[sql.index("(") + 1:sql.index(")")].split(",")]
            backing.insert(name, params, column_names=cols, database=database, settings=settings)
            return []

    with AuditLogger(**IDS, target_type="clickhouse",
                     connection={"connection": DriverStyle(), "create_tables": True}, mode="sync") as audit:
        with audit.run("driver"):
            audit.info(message="via clickhouse_driver")
    assert any(e.startswith("INSERT INTO logs.log_entry") for e in executed)
    assert backing.rows("SELECT message FROM logs.log_entry") == [{"message": "via clickhouse_driver"}]


def test_owned_client_from_host_options(monkeypatch):
    """With host=..., the sink creates its own clickhouse_connect client and closes it."""
    import clickhouse_connect

    created = []

    def fake_get_client(**kwargs):
        client = ChdbClient()
        for statement in clickhouse_ddl():
            client.command(statement)
        created.append((kwargs, client))
        return client

    monkeypatch.setattr(clickhouse_connect, "get_client", fake_get_client)
    audit = AuditLogger(**IDS, target_type="clickhouse",
                        connection={"host": "ch.example", "port": 8443, "secure": True}, mode="sync")
    audit.info(message="owned")
    audit.close()
    kwargs, client = created[0]
    assert kwargs["host"] == "ch.example" and kwargs["secure"] is True
    assert kwargs["autogenerate_session_id"] is False
    assert client.closed is True

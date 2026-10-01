"""
Postgres and MongoDB sink tests.

Postgres: a real server with the exact DDL (docs/sql/logs_schema.sql) plus stub
app.* tables — set C66_TEST_POSTGRES_DSN to a server where the user may
CREATE DATABASE; a throwaway database is created and dropped. Every way of
handing over a connection is exercised.

MongoDB: mongomock always; a real server when C66_TEST_MONGO_URI is set.
"""

import os
import threading
import uuid

import pytest

from c66_logger import AuditLogger, ConfigurationError, RejectedRecordsError
from c66_logger.record import LogRun

from .conftest import ACCELERATOR, ENV, IDS, TENANT, USE_CASE, USER, make_entry, pg_rows

psycopg2 = pytest.importorskip("psycopg2")
mongomock = pytest.importorskip("mongomock")
import psycopg2.pool  # noqa: E402

from c66_logger.sinks.mongodb import MongoSink  # noqa: E402
from c66_logger.sinks.postgres import PostgresSink  # noqa: E402

MONGO_URI = os.environ.get("C66_TEST_MONGO_URI")
UNKNOWN = "99999999-9999-4999-8999-999999999999"


class PooledProxy:
    """Mimics c66-chatbot's _PooledPgConnProxy / c66_clients' pooled connection:
    everything passes through; close() hands the connection back to the pool."""

    def __init__(self, real, pool):
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_pool", pool)
        object.__setattr__(self, "returned", False)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)

    def close(self):
        try:
            self._real.rollback()
        except Exception:
            pass
        self._pool.putconn(self._real)
        object.__setattr__(self, "returned", True)


# ---------- configuration (no database needed) ----------

@pytest.mark.parametrize(
    "kwargs",
    [{}, {"dsn": "postgresql://x", "host": "h", "database": "d"}, {"host": "h"},
     {"dsn": "postgresql://x", "schema": "bad-name"}, {"connection": 42}],
)
def test_postgres_sink_config_errors(kwargs):
    with pytest.raises(ConfigurationError):
        PostgresSink(**kwargs)


# ---------- Postgres: every connection style ----------

def _connection_variants(dsn):
    pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn)
    sa = pytest.importorskip("sqlalchemy")
    return {
        "raw psycopg2 connection": (psycopg2.connect(dsn), None),
        "factory returning pooled proxy": (lambda: PooledProxy(pool.getconn(), pool), pool),
        "psycopg2 pool": (pool, pool),
        "SQLAlchemy engine": (sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg2://", 1)), None),
        "options dict (own pool)": ({"dsn": dsn}, None),
    }


@pytest.mark.integration
def test_every_connection_style_writes_and_isolates_fk_errors(pg_dsn):
    for name, (connection, pool) in _connection_variants(pg_dsn).items():
        dropped = []
        with AuditLogger(**IDS, target_type="postgres", connection=connection, logger_name="conn_test",
                         flush_interval=0.1, on_drop=lambda e, why: dropped.append(why)) as audit:
            with audit.run("conn_test", metadata={"variant": name}) as run:
                audit.info({"message": name, "x": 1}, "message\nsecond row")
                audit.info({"message": "unknown user", "tenant_user_id": UNKNOWN})
        rows = pg_rows(pg_dsn, "SELECT message FROM logs.log_entry WHERE run_id = %s ORDER BY logged_at",
                       (run.run_id,))
        status = pg_rows(pg_dsn, "SELECT status FROM logs.log_run WHERE run_id = %s", (run.run_id,))[0]["status"]
        assert [r["message"] for r in rows] == [name, "second row"], name
        assert status == "Completed", name
        assert len(dropped) == 1 and "fk_log_entry_tenant_user" in dropped[0], name
        if isinstance(connection, psycopg2.extensions.connection):
            assert connection.closed == 0, "a caller's connection must never be closed"
        if pool is not None:
            conn = pool.getconn()  # pool still healthy, connections were returned
            pool.putconn(conn)


@pytest.mark.integration
def test_shared_connection_is_used_one_write_at_a_time(pg_dsn):
    conn = psycopg2.connect(pg_dsn)
    marker = uuid.uuid4().hex
    with AuditLogger(**IDS, target_type="postgres", connection=conn, mode="sync") as audit:
        def worker(n):
            for i in range(20):
                audit.info({"message": f"t{n}-{i}", "m": marker})

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    n = pg_rows(pg_dsn, "SELECT count(*) AS n FROM logs.log_entry WHERE metadata_json->>'m' = %s", (marker,))[0]["n"]
    assert n == 160
    conn.close()


@pytest.mark.integration
def test_factory_connections_go_back_to_the_pool(pg_dsn):
    pool = psycopg2.pool.ThreadedConnectionPool(1, 2, pg_dsn)
    handed_out = []

    def get_pg_conn():
        proxy = PooledProxy(pool.getconn(), pool)
        handed_out.append(proxy)
        return proxy

    with AuditLogger(**IDS, target_type="postgres", connection=get_pg_conn, mode="sync") as audit:
        for i in range(10):  # more writes than the pool has connections
            audit.info(message=f"pooled {i}")
    assert handed_out and all(p.returned for p in handed_out)
    pool.closeall()


@pytest.mark.integration
def test_missing_tables_fail_fast(pg_dsn):
    with pytest.raises(ConfigurationError, match="logs_schema.sql"):
        AuditLogger(**IDS, target_type="postgres", connection={"dsn": pg_dsn, "schema": "nope"})


# ---------- Postgres: end to end ----------

@pytest.mark.integration
def test_entries_and_run_land_in_the_real_tables(pg_dsn):
    with AuditLogger(**IDS, target_type="postgres", connection={"dsn": pg_dsn},
                     logger_name="order_service", use_case_id=USE_CASE, flush_interval=0.2) as audit:
        with audit.run("nightly_sync", metadata={"source": "crm"}, accelerator_id=ACCELERATOR) as run:
            n = audit.info(
                {"message": "order created", "order_id": 1},
                '[{"message": "a"}, {"message": "b", "level": "WARNING"}]',
                "message,account\nsynced,001A",
                tenant_user_id=USER,
            )
            try:
                raise KeyError("sku")
            except KeyError:
                audit.exception(message="lookup failed")
    assert n == 4

    r = pg_rows(pg_dsn, "SELECT * FROM logs.log_run WHERE run_id = %s", (run.run_id,))[0]
    assert (r["status"], r["run_type"], r["metadata_json"]) == ("Completed", "nightly_sync", {"source": "crm"})
    assert str(r["accelerator_id"]) == ACCELERATOR and str(r["use_case_id"]) == USE_CASE
    assert r["ended_at"] >= r["started_at"] and r["duration_ms"] >= 0 and r["created_at"] is not None

    rows = pg_rows(pg_dsn, "SELECT * FROM logs.log_entry WHERE run_id = %s ORDER BY logged_at", (run.run_id,))
    assert [x["message"] for x in rows] == ["order created", "a", "b", "synced", "lookup failed"]
    first = rows[0]
    assert str(first["tenant_id"]) == TENANT and str(first["environment_id"]) == ENV
    assert str(first["tenant_user_id"]) == USER and str(first["accelerator_id"]) == ACCELERATOR
    assert (first["logger_name"], first["level"], first["level_no"]) == ("order_service", "INFO", 20)
    assert first["metadata_json"] == {"order_id": 1}
    assert first["func_name"] == "test_entries_and_run_land_in_the_real_tables" and first["line_no"] > 0
    assert first["process_id"] and first["thread_id"] and first["created_at"]
    assert rows[2]["level"] == "WARNING" and rows[2]["level_no"] == 30
    assert rows[-1]["exception_type"] == "KeyError" and "Traceback" in rows[-1]["exception_traceback"]


@pytest.mark.integration
def test_failed_run_is_recorded(pg_dsn):
    with AuditLogger(**IDS, target_type="postgres", connection={"dsn": pg_dsn}, mode="sync") as audit:
        with pytest.raises(ValueError):
            with audit.run("import") as run:
                raise ValueError("bad file")
    r = pg_rows(pg_dsn, "SELECT status, error_summary FROM logs.log_run WHERE run_id = %s", (run.run_id,))[0]
    assert (r["status"], r["error_summary"]) == ("Failed", "ValueError: bad file")
    err = pg_rows(pg_dsn, "SELECT level, exception_type FROM logs.log_entry WHERE run_id = %s", (run.run_id,))
    assert [(e["level"], e["exception_type"]) for e in err] == [("ERROR", "ValueError")]


@pytest.mark.integration
def test_fk_violation_rejects_only_the_bad_rows(pg_dsn):
    dropped = []
    marker = uuid.uuid4().hex
    with AuditLogger(**IDS, target_type="postgres", connection={"dsn": pg_dsn}, mode="sync",
                     on_drop=lambda e, why: dropped.append((e.message, why))) as audit:
        audit.info(
            {"message": "good 1", "m": marker},
            {"message": "unknown run", "run_id": UNKNOWN, "m": marker},
            {"message": "unknown user", "tenant_user_id": UNKNOWN, "m": marker},
            {"message": "good 2", "m": marker},
        )
    written = pg_rows(pg_dsn, "SELECT message FROM logs.log_entry WHERE metadata_json->>'m' = %s ORDER BY message",
                      (marker,))
    assert [w["message"] for w in written] == ["good 1", "good 2"]
    assert [d[0] for d in dropped] == ["unknown run", "unknown user"]
    assert "fk_log_entry_run" in dropped[0][1] and "fk_log_entry_tenant_user" in dropped[1][1]


@pytest.mark.integration
def test_unknown_environment_is_rejected_not_retried(pg_dsn):
    sink = PostgresSink(dsn=pg_dsn)
    entry = make_entry("orphan")
    object.__setattr__(entry, "environment_id", str(uuid.uuid4()))  # not in app.tenant_environment
    with pytest.raises(RejectedRecordsError) as info:
        sink.write_batch([entry])
    assert "fk_log_entry_environment" in info.value.rejected[0][1]
    sink.close()


@pytest.mark.integration
def test_retried_batch_does_not_duplicate(pg_dsn):
    sink = PostgresSink(dsn=pg_dsn)
    batch = [make_entry(f"dup {i}", k="dup-test") for i in range(3)]
    sink.write_batch(batch)
    sink.write_batch(batch)  # a retry after a lost ack
    n = pg_rows(pg_dsn, "SELECT count(*) AS n FROM logs.log_entry WHERE metadata_json->>'k' = 'dup-test'")[0]["n"]
    assert n == 3
    sink.close()


@pytest.mark.integration
def test_large_batch_is_split_into_statements(pg_dsn):
    sink = PostgresSink(dsn=pg_dsn)
    marker = uuid.uuid4().hex
    sink.write_batch([make_entry(f"bulk {i}", m=marker) for i in range(1234)])
    n = pg_rows(pg_dsn, "SELECT count(*) AS n FROM logs.log_entry WHERE metadata_json->>'m' = %s", (marker,))[0]["n"]
    assert n == 1234
    sink.close()


@pytest.mark.integration
def test_run_upsert_keeps_created_at_and_updates_status(pg_dsn):
    sink = PostgresSink(dsn=pg_dsn)
    run = LogRun(tenant_id=TENANT, environment_id=ENV, run_type="upsert")
    sink.write_run(run)
    created = pg_rows(pg_dsn, "SELECT created_at FROM logs.log_run WHERE run_id = %s", (run.run_id,))[0]["created_at"]
    run.status, run.error_summary = "Cancelled", "stopped"
    sink.write_run(run)
    r = pg_rows(pg_dsn, "SELECT status, error_summary, created_at FROM logs.log_run WHERE run_id = %s",
                (run.run_id,))[0]
    assert (r["status"], r["error_summary"], r["created_at"]) == ("Cancelled", "stopped", created)
    sink.close()


# ---------- MongoDB (same shapes as the tables) ----------

@pytest.fixture
def mongo_client():
    if MONGO_URI:
        import pymongo

        client = pymongo.MongoClient(MONGO_URI)
    else:
        client = mongomock.MongoClient()
    yield client
    client.close()


def test_mongo_mirrors_both_tables(mongo_client):
    db = f"audit_{uuid.uuid4().hex[:8]}"
    with AuditLogger(**IDS, sink=MongoSink(client=mongo_client, database=db), mode="sync") as audit:
        with audit.run("nightly_sync") as run:
            audit.info({"message": "order created", "order_id": 1}, "message\nsynced")
    entries = list(mongo_client[db]["log_entry"].find().sort("logged_at", 1))
    assert [e["message"] for e in entries] == ["order created", "synced"]
    doc = entries[0]
    assert doc["_id"] == doc["log_id"] and doc["run_id"] == run.run_id and doc["tenant_id"] == TENANT
    assert doc["level_no"] == 20 and doc["metadata_json"] == {"order_id": 1} and "created_at" in doc
    stored_run = mongo_client[db]["log_run"].find_one({"_id": run.run_id})
    assert stored_run["status"] == "Completed" and stored_run["duration_ms"] >= 0 and "created_at" in stored_run
    mongo_client.drop_database(db)


def test_mongo_retried_batch_does_not_duplicate(mongo_client):
    db = f"audit_{uuid.uuid4().hex[:8]}"
    sink = MongoSink(client=mongo_client, database=db)
    batch = [make_entry(f"m{i}") for i in range(3)]
    sink.write_batch(batch[:2])
    sink.write_batch(batch)
    assert mongo_client[db]["log_entry"].count_documents({}) == 3
    mongo_client.drop_database(db)


@pytest.mark.parametrize("kwargs", [{"database": "d"}, {"uri": "mongodb://x"}])
def test_mongo_sink_config_errors(kwargs):
    with pytest.raises(ConfigurationError):
        MongoSink(**kwargs)

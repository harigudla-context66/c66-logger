import asyncio
import datetime as dt
import logging
import subprocess
import sys

import pytest

from c66_logger import (
    AuditLogger,
    AuditLogHandler,
    ConfigurationError,
    InvalidLogInputError,
    RunWriteError,
    UnknownTargetError,
    available_targets,
    register_sink,
)

from .conftest import ACCELERATOR, ENV, IDS, TENANT, USE_CASE, USER, FlakySink

CORR = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def audit():
    logger = AuditLogger(**IDS, target_type="memory", mode="sync", logger_name="order_service")
    yield logger
    logger.close()


def entries(audit):
    return audit.sink.entries


# ---------- configuration ----------

@pytest.mark.parametrize(
    "kwargs",
    [
        dict(IDS),                                                         # no target
        dict(IDS, target_type="memory", sink=FlakySink()),                 # both
        dict(IDS, sink=FlakySink(), connection={"dsn": "x"}),
        dict(IDS, target_type="memory", mode="turbo"),
        dict(tenant_id="salesforce", environment_id=ENV, target_type="memory"),  # not a UUID
        dict(tenant_id=TENANT, environment_id=None, target_type="memory"),
        dict(IDS, target_type="memory", correlation_id="nope"),
    ],
)
def test_configuration_errors(kwargs):
    with pytest.raises(ConfigurationError):
        AuditLogger(**kwargs)


def test_unknown_target_lists_available():
    with pytest.raises(UnknownTargetError, match="postgres"):
        AuditLogger(**IDS, target_type="s3")


def test_ids_are_normalized():
    log = AuditLogger(tenant_id=TENANT.upper(), environment_id=ENV, target_type="memory")
    assert log.tenant_id == TENANT
    log.close()


# ---------- entry mapping ----------

def test_entry_columns(audit):
    audit.info({"message": "order created", "order_id": 7, "amount": 9.5})
    e = entries(audit)[0]
    assert (e.tenant_id, e.environment_id, e.logger_name) == (TENANT, ENV, "order_service")
    assert (e.level, e.level_no, e.message) == ("INFO", 20, "order created")
    assert e.metadata_json == {"order_id": 7, "amount": 9.5}
    assert e.logged_at.tzinfo is not None
    row = e.as_row()
    assert row["level_no"] == 20 and "created_at" not in row


def test_caller_process_and_thread_are_captured(audit):
    audit.info(message="where am I")  # this line is recorded
    e = entries(audit)[0]
    assert e.func_name == "test_caller_process_and_thread_are_captured"
    assert e.module == "test_logger" and e.pathname.endswith("test_logger.py") and e.line_no > 0
    assert e.process_id > 0 and e.process_name and e.thread_id and e.thread_name


def test_message_only_call_and_plain_string(audit):
    audit.info(message="sync started")
    audit.warning("disk almost full")
    assert [(e.level, e.message) for e in entries(audit)] == [("INFO", "sync started"), ("WARNING", "disk almost full")]


def test_message_is_required(audit):
    with pytest.raises(InvalidLogInputError, match="message"):
        audit.info({"order_id": 1})
    assert entries(audit) == []


def test_call_message_is_the_default_for_items(audit):
    audit.info({"order_id": 1}, {"order_id": 2, "message": "own message"}, message="bulk")
    assert [e.message for e in entries(audit)] == ["bulk", "own message"]


def test_item_can_set_level_logged_at_log_id_and_logger_name(audit):
    audit.log("message,level,logged_at,logger_name\nimported,error,2026-09-01T10:00:00Z,legacy")
    e = entries(audit)[0]
    assert (e.level, e.level_no, e.logger_name) == ("ERROR", 40, "legacy")
    assert e.logged_at == dt.datetime(2026, 9, 1, 10, tzinfo=dt.timezone.utc)


def test_metadata_json_key_is_merged(audit):
    audit.info({"message": "m", "a": 1, "metadata_json": {"b": 2}})
    assert entries(audit)[0].metadata_json == {"a": 1, "b": 2}


def test_non_json_metadata_is_stringified(audit):
    audit.info({"message": "m", "when": dt.date(2026, 9, 1), "obj": object()})
    meta = entries(audit)[0].metadata_json
    assert meta["when"] == "2026-09-01" and meta["obj"].startswith("<object")


@pytest.mark.parametrize(
    "item",
    [
        {"message": "m", "level": "AUDIT"},                      # not allowed by chk_log_entry_level
        {"message": "m", "run_id": "not-a-uuid"},
        {"message": "m", "logged_at": "yesterday"},
        {"message": "m", "tenant_id": "22222222-2222-4222-8222-222222222222"},  # other tenant
    ],
)
def test_invalid_items_raise_before_writing(audit, item):
    with pytest.raises(InvalidLogInputError):
        audit.info({"message": "fine"}, item)
    assert entries(audit) == []


def test_level_validation(audit):
    with pytest.raises(InvalidLogInputError):
        audit.log(message="x", level="AUDIT")
    audit.log(message="x", level="warn")
    assert entries(audit)[0].level == "WARNING"


def test_varchar_columns_are_clipped():
    log = AuditLogger(**IDS, target_type="memory", mode="sync", logger_name="x" * 300)
    log.info(message="m")
    assert len(log.sink.entries[0].logger_name) == 200
    log.close()


# ---------- ids: logger defaults < context < call < item ----------

def test_id_precedence(audit):
    log = AuditLogger(**IDS, target_type="memory", mode="sync", use_case_id=USE_CASE, tenant_user_id=USER)
    other_user = "00000000-0000-4000-8000-000000000b0b"
    with log.context(correlation_id=CORR):
        log.info(message="a")
        log.info(message="b", tenant_user_id=other_user)
        log.info({"message": "c", "correlation_id": None, "accelerator_id": ACCELERATOR})
    log.info(message="d")
    a, b, c, d = log.sink.entries
    assert (a.use_case_id, a.tenant_user_id, a.correlation_id) == (USE_CASE, USER, CORR)
    assert b.tenant_user_id == other_user
    assert c.accelerator_id == ACCELERATOR and c.correlation_id is None
    assert d.correlation_id is None
    log.close()


def test_unknown_id_kwarg_is_a_type_error(audit):
    with pytest.raises(TypeError):
        audit.info(message="m", order_id=1)


def test_context_does_not_leak_between_threads(audit):
    import threading

    seen = []
    with audit.context(correlation_id=CORR):
        t = threading.Thread(target=lambda: audit.info(message="other thread"))
        t.start()
        t.join()
        audit.info(message="this thread")
    seen = {e.message: e.correlation_id for e in entries(audit)}
    assert seen == {"other thread": None, "this thread": CORR}


# ---------- exceptions ----------

def test_exception_columns(audit):
    try:
        {}["missing"]
    except KeyError:
        audit.exception(message="lookup failed")
    e = entries(audit)[0]
    assert (e.level, e.exception_type, e.exception_message) == ("ERROR", "KeyError", "'missing'")
    assert "Traceback" in e.exception_traceback and "{}[\"missing\"]" in e.exception_traceback


def test_exc_info_accepts_an_exception_object(audit):
    audit.error(message="failed", exc_info=ValueError("bad value"))
    assert entries(audit)[0].exception_type == "ValueError"


# ---------- runs ----------

def test_run_lifecycle_completed(audit):
    with audit.run("nightly_sync", metadata={"source": "crm"}, use_case_id=USE_CASE) as run:
        stored = audit.sink.runs[run.run_id]
        assert stored.status == "Running" and stored.ended_at is None
        audit.info(message="inside")
    audit.info(message="outside")
    final = audit.sink.runs[run.run_id]
    assert final.status == "Completed" and final.ended_at and final.duration_ms >= 0
    assert final.metadata_json == {"source": "crm"} and final.use_case_id == USE_CASE
    inside, outside = entries(audit)
    assert inside.run_id == run.run_id and inside.use_case_id == USE_CASE
    assert outside.run_id is None


def test_run_failure_logs_error_and_marks_failed(audit):
    with pytest.raises(RuntimeError):
        with audit.run("import") as run:
            raise RuntimeError("source unavailable")
    final = audit.sink.runs[run.run_id]
    assert final.status == "Failed" and final.error_summary == "RuntimeError: source unavailable"
    err = entries(audit)[-1]
    assert err.level == "ERROR" and err.run_id == run.run_id and err.exception_type == "RuntimeError"
    assert err.func_name == "test_run_failure_logs_error_and_marks_failed"  # where it was raised


def test_run_cancelled_on_keyboard_interrupt(audit):
    with pytest.raises(KeyboardInterrupt):
        with audit.run("long_job") as run:
            raise KeyboardInterrupt
    assert audit.sink.runs[run.run_id].status == "Cancelled"


def test_run_cancelled_on_asyncio_cancel(audit):
    async def job():
        with audit.run("async_job") as run:
            await asyncio.sleep(10)
        return run

    async def main():
        task = asyncio.ensure_future(job())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    (run,) = audit.sink.runs.values()
    assert run.status == "Cancelled"


def test_explicit_start_end_and_double_end(audit):
    run = audit.start_run("manual", tenant_user_id=USER)
    audit.info(message="step", run_id=run.run_id)
    audit.end_run(run, "Failed", error_summary="partial", metadata={"rows": 10})
    audit.end_run(run, "Completed")  # ignored
    final = audit.sink.runs[run.run_id]
    assert (final.status, final.error_summary, final.metadata_json) == ("Failed", "partial", {"rows": 10})
    assert entries(audit)[0].run_id == run.run_id


def test_end_run_rejects_bad_status(audit):
    run = audit.start_run()
    with pytest.raises(ConfigurationError):
        audit.end_run(run, "Done")


def test_end_run_flushes_entries_first():
    log = AuditLogger(**IDS, target_type="memory", flush_interval=60)
    with log.run("job") as run:
        log.info(message="queued")
    assert log.sink.entries[0].run_id == run.run_id  # written before the run was marked Completed
    log.close()


def test_start_run_failure_raises_run_write_error():
    class NoRuns(FlakySink):
        def write_run(self, run):
            raise ConnectionError("db down")

    log = AuditLogger(**IDS, sink=NoRuns(), mode="sync", max_retries=1, retry_backoff=0.01)
    with pytest.raises(RunWriteError, match="db down"):
        log.start_run("job")


def test_nested_runs_restore_outer_run_id(audit):
    with audit.run("outer") as outer:
        with audit.run("inner") as inner:
            audit.info(message="in inner")
        audit.info(message="back in outer")
    a, b = entries(audit)
    assert (a.run_id, b.run_id) == (inner.run_id, outer.run_id)


# ---------- logging bridge ----------

def test_logging_handler_maps_log_record(audit):
    std = logging.getLogger("host_package.jobs")
    std.setLevel(logging.DEBUG)
    handler = AuditLogHandler(audit)
    std.addHandler(handler)
    try:
        with audit.run("job") as run:
            std.warning("slow %s", "page", extra={"audit": {"page": "/x", "correlation_id": CORR}})
            try:
                1 / 0
            except ZeroDivisionError:
                std.exception("math failed")
            std.log(25, "custom level")
    finally:
        std.removeHandler(handler)
    warn, err, custom = entries(audit)[:3]
    assert (warn.logger_name, warn.level, warn.message) == ("host_package.jobs", "WARNING", "slow page")
    assert warn.metadata_json == {"page": "/x"} and warn.correlation_id == CORR and warn.run_id == run.run_id
    assert warn.func_name == "test_logging_handler_maps_log_record" and warn.module == "test_logger"
    assert err.exception_type == "ZeroDivisionError" and "Traceback" in err.exception_traceback
    assert (custom.level, custom.level_no) == ("INFO", 20)  # 25 isn't allowed by the table: nearest below


# ---------- targets & library hygiene ----------

def test_caller_supplied_sink_is_not_closed():
    sink = FlakySink()
    log = AuditLogger(**IDS, sink=sink, mode="sync")
    assert sink.opened
    log.close()
    assert sink.closed is False


def test_logger_closes_sink_it_created():
    register_sink("flaky-owned", FlakySink, replace=True)
    log = AuditLogger(**IDS, target_type="flaky-owned", mode="sync")
    log.close()
    assert log.sink.closed is True


def test_register_custom_target():
    class FakeKafkaSink(FlakySink):
        target_type = "kafka"

        def __init__(self, bootstrap_servers, topic):
            super().__init__()
            self.topic = topic

    register_sink("kafka", FakeKafkaSink, replace=True)
    assert "kafka" in available_targets()
    log = AuditLogger(**IDS, target_type="kafka", connection={"bootstrap_servers": "b:9092", "topic": "t"}, mode="sync")
    log.info(message="x")
    assert log.sink.topic == "t" and len(log.sink.entries) == 1


def test_from_config():
    log = AuditLogger.from_config({**IDS, "target_type": "memory", "mode": "sync"})
    log.info(message="x")
    assert len(log.sink.entries) == 1


def test_open_failure_is_raised_at_init():
    class BrokenSink(FlakySink):
        def open(self):
            raise ConnectionError("db unreachable")

    with pytest.raises(ConnectionError):
        AuditLogger(**IDS, sink=BrokenSink())


def test_import_loads_no_drivers_and_no_logging_config():
    code = (
        "import logging, sys, c66_logger;"
        "assert 'sqlalchemy' not in sys.modules, 'sqlalchemy imported';"
        "assert 'pymongo' not in sys.modules, 'pymongo imported';"
        "assert not logging.getLogger().handlers, 'root logger touched'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)

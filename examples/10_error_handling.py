"""
Case 10 — what fails, where, and how to handle it.

  wrong configuration / missing tables  -> raises when the logger is built
  bad input to log()                    -> raises in log(), nothing written
  a row the database rejects (FK, CHECK) -> only that row dropped via on_drop, no retry
  database outage                        -> batch retried with backoff, then on_drop
  run can't be recorded                  -> start_run() raises RunWriteError

    python examples/10_error_handling.py          # parts 3 and 4 need Postgres
"""

import json
import logging
import os
import tempfile

from _settings import POSTGRES_DSN, TENANT, postgres_target

from c66_logger import (
    AuditLogger,
    BaseSink,
    ConfigurationError,
    InvalidLogInputError,
    RunWriteError,
)

# c66_logger reports drops on the "c66_logger" logger, which is silent by default.
logging.basicConfig(level=logging.WARNING, format="    [%(name)s] %(levelname)s %(message)s")

DEAD_LETTER = os.path.join(tempfile.gettempdir(), "c66_dead_letter.jsonl")


def to_dead_letter(entry, reason):
    """on_drop callback: keep what couldn't be written, to replay later."""
    with open(DEAD_LETTER, "a") as f:
        f.write(json.dumps({"reason": reason, "entry": json.loads(entry.to_json())}) + "\n")


def configuration_errors() -> None:
    print("1. configuration errors (raised at construction)")
    for kwargs in (
        dict(TENANT, target_type="memory", mode="turbo"),
        dict(tenant_id="salesforce", environment_id=TENANT["environment_id"], target_type="memory"),
        dict(TENANT, target_type="postgres", connection={"dsn": POSTGRES_DSN, "schema": "no_such_schema"}),
    ):
        try:
            AuditLogger(**kwargs)
        except ConfigurationError as exc:
            print(f"   ConfigurationError: {exc}")
        except Exception as exc:  # e.g. no database reachable for the last case
            print(f"   {type(exc).__name__}: {str(exc).splitlines()[0][:90]}")


def bad_input() -> None:
    print("2. bad input is rejected before anything is written")
    audit = AuditLogger(**TENANT, target_type="memory", mode="sync")
    for bad in ({"order_id": 1}, {"message": "x", "level": "AUDIT"}, {"message": "x", "run_id": "42"}, 3.14):
        try:
            audit.info({"message": "fine"}, bad)
        except InvalidLogInputError as exc:
            print(f"   InvalidLogInputError: {exc}")
    print(f"   rows written: {len(audit.sink.entries)}")
    audit.close()


def rejected_rows() -> None:
    print("3. Postgres rejects a row (unknown run_id -> fk_log_entry_run): only that row is dropped")
    with AuditLogger(**TENANT, **postgres_target(), mode="sync", on_drop=to_dead_letter) as audit:
        audit.info(
            {"message": "good row 1"},
            {"message": "refers to a run that doesn't exist", "run_id": "99999999-9999-4999-8999-999999999999"},
            {"message": "good row 2"},
        )


class OutageSink(BaseSink):
    """Stands in for a target that is down for the whole run."""

    target_type = "outage"

    def write_batch(self, entries):
        raise ConnectionError("log database unavailable")

    def write_run(self, run):
        raise ConnectionError("log database unavailable")


def outage() -> None:
    print("4. outage: entries retried then dead-lettered; a run can't start")
    audit = AuditLogger(**TENANT, sink=OutageSink(), max_retries=2, retry_backoff=0.1, on_drop=to_dead_letter)
    audit.info({"message": "order created", "order_id": 5001}, {"message": "order paid", "order_id": 5001})
    audit.flush()  # returns once the entries are written *or* dropped
    try:
        audit.start_run("nightly_sync")
    except RunWriteError as exc:
        print(f"   RunWriteError: {str(exc)[:90]}")
    audit.close()


if __name__ == "__main__":
    open(DEAD_LETTER, "w").close()
    configuration_errors()
    bad_input()
    try:
        rejected_rows()
    except Exception as exc:
        print(f"   skipped (no Postgres): {type(exc).__name__}")
    outage()
    with open(DEAD_LETTER) as f:
        lines = f.readlines()
    print(f"\n{len(lines)} entries in {DEAD_LETTER} — replay them once the target is back:")
    for line in lines:
        record = json.loads(line)
        print(f"   {record['entry']['message']!r:<40} {record['reason'][:70]}")

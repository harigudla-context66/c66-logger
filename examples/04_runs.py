"""
Case 4 — the logs.log_run lifecycle.

A run is inserted as 'Running' when it starts (synchronously, so entries that
reference it can never arrive first — log_entry.run_id has a foreign key to
it) and updated when it ends with ended_at, duration_ms and error_summary.

  with audit.run(...)       block ends -> Completed
                            Exception  -> ERROR entry with traceback, then Failed
                            Ctrl-C / task cancelled / SystemExit -> Cancelled
  start_run() / end_run()   when a with-block doesn't fit (callbacks, long-lived jobs)

    python examples/04_runs.py            # Postgres (C66_EXAMPLE_POSTGRES_DSN)
    python examples/04_runs.py memory     # no database
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from _settings import TENANT, TENANT_USER_ID, USE_CASE_ID, postgres_target

from c66_logger import AuditLogger


def main(target: dict) -> None:
    with AuditLogger(**TENANT, **target, logger_name="crm_sync") as audit:

        # 1. with-block -> Completed
        with audit.run("nightly_sync", metadata={"objects": ["Account", "Lead"]},
                       use_case_id=USE_CASE_ID, tenant_user_id=TENANT_USER_ID) as run:
            audit.info("sync started")
            time.sleep(0.05)
            audit.info({"message": "accounts synced", "rows": 1204})
        print(f"1. {run.run_type}: {run.status} in {run.duration_ms} ms")

        # 2. with-block that raises -> ERROR entry + Failed, exception re-raised
        try:
            with audit.run("lead_import") as run:
                audit.info("reading leads file")
                raise FileNotFoundError("leads_0923.csv")
        except FileNotFoundError:
            pass
        print(f"2. {run.run_type}: {run.status} — {run.error_summary}")

        # 3. explicit start/end, with metadata added at the end
        run = audit.start_run("report_export", metadata={"format": "xlsx"})
        audit.info("export started", run_id=run.run_id)
        audit.end_run(run, "Completed", metadata={"rows": 5000, "file": "q3.xlsx"})
        print(f"3. {run.run_type}: {run.status}, metadata {run.metadata_json}")

        # 4. work fanned out to threads: pass run_id explicitly — plain threads
        #    (and ThreadPoolExecutor) don't inherit the with-block's context
        with audit.run("parallel_enrichment") as run:
            def enrich(batch_no: int) -> None:
                audit.info({"message": "batch enriched", "batch": batch_no}, run_id=run.run_id)

            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(enrich, range(8)))
        print(f"4. {run.run_type}: {run.status}")

        # 5. cancelled
        run = audit.start_run("user_triggered_backfill")
        audit.end_run(run, "Cancelled", error_summary="stopped by user")
        print(f"5. {run.run_type}: {run.status}")

        if audit.sink.target_type == "memory":
            for r in audit.sink.runs.values():
                n = sum(1 for e in audit.sink.entries if e.run_id == r.run_id)
                print(f"   log_run {r.run_type:<24} {r.status:<10} {n} log_entry rows")


if __name__ == "__main__":
    use_memory = len(sys.argv) > 1 and sys.argv[1] == "memory"
    main({"target_type": "memory"} if use_memory else postgres_target())

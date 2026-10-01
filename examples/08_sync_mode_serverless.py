"""
Case 8 — short-lived processes: CLI tools, cron jobs, serverless handlers.

mode="sync" writes before log() returns, so nothing depends on a background
thread surviving until the process exits (a Lambda can be frozen the moment
the handler returns). Build the logger, log, close — inside one invocation.

    python examples/08_sync_mode_serverless.py             # memory target
    python examples/08_sync_mode_serverless.py postgres    # C66_EXAMPLE_POSTGRES_DSN
"""

import json
import sys

from _settings import TENANT, postgres_target

from c66_logger import AuditLogger

TARGET = {"target_type": "memory"}


def handler(event: dict, context=None) -> dict:
    with AuditLogger(**TENANT, **TARGET, mode="sync", logger_name="nightly_sync_lambda") as audit:
        with audit.run("lambda_sync", metadata={"request_id": event.get("request_id")}) as run:
            written = audit.info(event["records"], message="account synced")
    return {"statusCode": 200, "body": json.dumps({"run_id": run.run_id, "status": run.status, "entries": written})}


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "postgres":
        TARGET = postgres_target()
    print(handler({
        "request_id": "req-123",
        "records": [{"account_id": "001A"}, {"account_id": "001B"}],
    }))

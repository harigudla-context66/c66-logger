"""
Case 5 — filling the id columns: correlation_id, tenant_user_id, use_case_id,
accelerator_id, run_id.

Set them at whichever level fits; the more specific one wins:

    logger defaults  <  with audit.context(...) / audit.run(...)  <  log(..., id=...)  <  a key in the item

Runs with no database (memory target).

    python examples/05_context_ids.py
"""

import uuid

from _settings import ACCELERATOR_ID, TENANT, TENANT_USER_ID, USE_CASE_ID

from c66_logger import AuditLogger


def handle_request(audit: AuditLogger, user_id: str) -> None:
    """A web request: one correlation_id ties together everything it logs."""
    with audit.context(correlation_id=str(uuid.uuid4()), tenant_user_id=user_id):
        audit.info("request received")
        audit.info({"message": "score computed", "score": 0.87})
        audit.warning("model cache miss")


def main() -> None:
    audit = AuditLogger(
        **TENANT,
        target_type="memory",
        mode="sync",
        logger_name="lead_scoring_api",
        accelerator_id=ACCELERATOR_ID,       # logger default: on every row
    )

    handle_request(audit, TENANT_USER_ID)
    handle_request(audit, TENANT_USER_ID)

    # per call
    audit.info("batch scoring started", use_case_id=USE_CASE_ID)
    # per item (e.g. ids carried in an incoming JSON/CSV payload)
    audit.info(f'{{"message": "external event", "correlation_id": "{uuid.uuid4()}"}}')

    print(f"{'message':<24} {'correlation_id':<38} {'tenant_user_id':<38} use_case_id")
    for e in audit.sink.entries:
        print(f"{e.message:<24} {str(e.correlation_id):<38} {str(e.tenant_user_id):<38} {e.use_case_id}")
    assert all(e.accelerator_id == ACCELERATOR_ID for e in audit.sink.entries)
    audit.close()


if __name__ == "__main__":
    main()

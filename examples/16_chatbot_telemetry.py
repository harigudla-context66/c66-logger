"""
Case 16 — every logging call c66-chatbot makes today, through c66_logger.

Telemetry has the same functions with the same arguments as app_common.py
(log_event, log_llm_call, log_graph_query, log_retrieval_event,
log_ingestion_event, log_feedback, log_eval_run, log_deletion_receipt,
log_audit_event), so the chatbot switches by changing imports. Everything
lands in log_run / log_entry — Postgres or ClickHouse, on the connection the
app already has. See docs/chatbot-log-mapping.md for the field-by-field map.

    python examples/16_chatbot_telemetry.py              # ClickHouse (embedded unless C66_EXAMPLE_CLICKHOUSE_HOST)
    python examples/16_chatbot_telemetry.py postgres     # Postgres (C66_EXAMPLE_POSTGRES_DSN)
"""

import logging
import sys
import uuid

from _settings import POSTGRES_DSN, clickhouse_client

from c66_logger import AuditLogger, Telemetry

# The chatbot knows tenants by client name; log tables need UUIDs from app.*.
CLIENTS = {
    "meta_telephony": ("5a1e5f0c-0000-4000-8000-000000000001", "e0000000-0000-4000-8000-000000000001"),
    "acme":           ("4b0b5907-0000-4000-8000-000000000002", "e0000000-0000-4000-8000-000000000002"),
}
SYSTEM = ("2e4de5c0-0000-4000-8000-000000000003", "e0000000-0000-4000-8000-000000000003")
USERS = {"alice": "00000000-0000-4000-8000-00000000a11c"}
PRICES = {"anthropic": (3e-6, 15e-6)}


def calc_cost_split(provider, input_tokens, output_tokens):  # = app_common._calc_cost_split
    i, o = PRICES.get(provider, (0, 0))
    return round(input_tokens * i, 6), round(output_tokens * o, 6)


def build(target: str):
    if target == "postgres":
        root = AuditLogger(target_type="postgres", connection={"dsn": POSTGRES_DSN})   # or connection=get_pg_conn
        query = None
    else:
        client = clickhouse_client()                                                  # or connection=get_ch_client
        root = AuditLogger(target_type="clickhouse", connection={"connection": client, "create_tables": True})
        query = client.rows if hasattr(client, "rows") else (lambda sql: client.query(sql).named_results())
    telemetry = Telemetry(
        root,
        resolve_tenant=CLIENTS.get,               # client name -> (tenant_id, environment_id)
        resolve_user=USERS.get,                   # username -> app.tenant_user id (optional)
        system_tenant=SYSTEM,                     # for eval runs and plain logging
        cost_fn=calc_cost_split,
        model_fn={"anthropic": "claude-sonnet-4"}.get,   # = PROVIDER_MODEL.get
        environment="development",                # = APP_ENVIRONMENT
        prompt_version="v42",                     # = PROMPT_VERSION
    )
    return root, telemetry, query


def one_chat_request(telemetry: Telemetry) -> str:
    """What /api/chat does today, call for call."""
    request_id = str(uuid.uuid4())
    telemetry.log_llm_call(request_id, "meta_telephony", 1, "routing", "anthropic", 1200, 40, max_tokens=200,
                           username="alice", latency_ms=310, finish_reason="end_turn")
    telemetry.log_retrieval_event(request_id, "meta_telephony", 12, True, username="alice")
    telemetry.log_graph_query(request_id, "meta_telephony", "graph_traversal", cypher_text="MATCH (c:Customer) ...",
                              anchor_count=3, nodes_returned=40, relationships_returned=55, username="alice")
    telemetry.log_llm_call(request_id, "meta_telephony", 2, "answer", "anthropic", 5000, 700, max_tokens=4096,
                           username="alice", latency_ms=4200)
    telemetry.log_llm_call(request_id, "meta_telephony", 0, "groundedness_check", "anthropic", 900, 30,
                           username="alice")
    telemetry.log_event("meta_telephony", "/api/chat", "Why did churn rise in Q3?", "graph_traversal",
                        provider="anthropic", llm_call_count=3, input_tokens=7100, output_tokens=770,
                        latency_ms=5300, nodes_referenced=40, relationships_referenced=55, request_id=request_id,
                        username="alice", user_id=7, groundedness_score=4, groundedness_reason="supported")
    telemetry.log_feedback(request_id, "meta_telephony", 1, comment="spot on", username="alice")
    return request_id


def the_rest(telemetry: Telemetry) -> None:
    telemetry.log_ingestion_event("meta_telephony", "doc-9", "contracts.pdf", "pdf", uploaded_by="alice",
                                  latency_ms=8200, total_chunks=42, llm_cost_usd=0.03, embedding_cost_usd=0.001)
    telemetry.log_eval_run("hallucination_v3", 17, "numeric", "What was Q3 revenue?", True, [], [], "$4.2M")
    telemetry.log_deletion_receipt("acme", "alice", nodes_deleted=120, chunks_deleted=900)
    telemetry.log_audit_event("alice", "acme", "purge_tenant", "requested from admin console")
    # print("... (non-fatal): ...") becomes a logging call:
    log = logging.getLogger("c66.app")
    log.setLevel(logging.INFO)
    log.addHandler(telemetry.logging_handler())
    log.warning("ClickHouse schema init error (non-fatal, logging disabled): %s", "timeout")


def main(target: str) -> None:
    root, telemetry, query = build(target)
    try:
        request_id = one_chat_request(telemetry)
        the_rest(telemetry)
        telemetry.flush()
    finally:
        root.close()

    print(f"request {request_id} written to {target}")
    if query:  # ClickHouse: show what landed
        for row in query(f"SELECT run_type, status, duration_ms, JSONExtractFloat(metadata_json, 'total_cost_usd') "
                         f"AS cost_usd FROM logs.log_run FINAL WHERE run_id = '{request_id}'"):
            print("  log_run  ", dict(row))
        for row in query("SELECT logger_name, level, message FROM logs.log_entry FINAL ORDER BY logged_at"):
            print("  log_entry", dict(row))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "clickhouse")

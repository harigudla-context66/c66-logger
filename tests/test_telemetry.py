"""
Telemetry: every logging call c66-chatbot makes today (app_common.py),
replayed with the same arguments the chatbot passes, mapped onto
log_run / log_entry. Unit tests use the memory target; the replay at the
bottom runs against real Postgres (exact DDL) and embedded ClickHouse.
"""

import inspect
import logging
import uuid

import pytest

from c66_logger import AuditLogger, ConfigurationError, Telemetry

from .conftest import ENV, TENANT, USER, pg_rows

OTHER_TENANT = "4b0b5907-0000-4000-8000-000000000002"
OTHER_ENV = "e0000000-0000-4000-8000-000000000002"
SYSTEM_TENANT = "2e4de5c0-0000-4000-8000-000000000003"
SYSTEM_ENV = "e0000000-0000-4000-8000-000000000003"

TENANTS = {"meta_telephony": (TENANT, ENV), "acme": (OTHER_TENANT, OTHER_ENV)}
PRICES = {"anthropic": (3e-6, 15e-6), "openai": (2.5e-6, 10e-6)}


def cost_fn(provider, input_tokens, output_tokens):  # stands in for app_common._calc_cost_split
    i, o = PRICES.get(provider, (0, 0))
    return round(input_tokens * i, 6), round(output_tokens * o, 6)


@pytest.fixture
def root():
    audit = AuditLogger(target_type="memory", mode="sync")
    yield audit
    audit.close()


@pytest.fixture
def telemetry(root):
    return Telemetry(
        root,
        resolve_tenant=TENANTS.get,
        resolve_user={"alice": USER}.get,
        system_tenant=(SYSTEM_TENANT, SYSTEM_ENV),
        cost_fn=cost_fn,
        model_fn={"anthropic": "claude-sonnet-4", "openai": "gpt-4o"}.get,
        environment="production",
        prompt_version="v42",
    )


def entries(root):
    return root.sink.entries


def runs(root):
    return list(root.sink.runs.values())


# ---------- same signatures as c66-chatbot ----------

CHATBOT_SIGNATURES = {
    "log_event": "(client, route, question, retrieval_mode, provider='', document_names='', llm_call_count=0, "
                 "input_tokens=0, output_tokens=0, latency_ms=0, nodes_referenced=0, relationships_referenced=0, "
                 "cache_hit=False, success=True, error='', request_id='', device_id='', username='', user_id=None, "
                 "contains_pii=False, groundedness_score=-1, groundedness_reason='', prompt_version=None, "
                 "temperature=0.0, total_retry_count=0, estimated_cost_saved_usd=0.0, judge_provider='', "
                 "judge_score=-1, judge_reason='', self_consistency_score=-1, self_consistency_reason='')",
    "log_ingestion_event": "(client, doc_id, filename, doc_type, uploaded_by='', category='', success=True, "
                           "error='', latency_ms=0, total_chunks=0, embedding_missing=0, pg_insert_failed=0, "
                           "chunks_with_pii=0, entity_count=0, relationship_count=0, rows_written=0, "
                           "rows_skipped=0, sparse_columns_count=0, schema_status='', schema_missing_columns=None, "
                           "schema_new_columns=None, llm_cost_usd=0.0, embedding_cost_usd=0.0, llm_call_count=0)",
    "log_llm_call": "(request_id, client, call_index, purpose, provider, input_tokens, output_tokens, max_tokens=0, "
                    "username='', temperature=0.0, latency_ms=0, retry_count=0, finish_reason='', prompt_text='', "
                    "response_text='', success=True, error='')",
    "log_graph_query": "(request_id, client, query_type, cypher_text='', anchor_count=0, nodes_returned=0, "
                       "relationships_returned=0, success=True, error='', device_id='', result_data='', username='')",
    "log_retrieval_event": "(request_id, client, records_matched, aggregate_computed, username='')",
    "log_feedback": "(request_id, client, rating, comment='', username='')",
    "log_eval_run": "(eval_set_name, question_id, category, question, passed, missing_terms, forbidden_found, "
                    "answer)",
    "log_deletion_receipt": "(client, requested_by, nodes_deleted=0, chunks_deleted=0, cache_keys_deleted=0, "
                            "observability_rows_deleted=0, success=True, error='')",
    "log_audit_event": "(username, client_name, action, detail='')",
}


@pytest.mark.parametrize("name", sorted(CHATBOT_SIGNATURES))
def test_signature_matches_app_common(name):
    """Copied from c66-chatbot app_common.py (commit 5bb35d3): call sites can switch by import alone."""
    sig = str(inspect.signature(getattr(Telemetry, name))).replace("(self, ", "(")
    assert sig == CHATBOT_SIGNATURES[name]


# ---------- one /api/chat request ----------

def test_chat_request_becomes_one_run_with_its_entries(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_llm_call(rid, "meta_telephony", 1, "routing", "anthropic", 1200, 40, max_tokens=200,
                           username="alice", temperature=0.0, latency_ms=310, finish_reason="end_turn",
                           prompt_text="Route this question…", response_text="graph")
    telemetry.log_retrieval_event(rid, "meta_telephony", 12, True, username="alice")
    telemetry.log_graph_query(rid, "meta_telephony", "graph_traversal", cypher_text="MATCH (n) RETURN n",
                              anchor_count=3, nodes_returned=40, relationships_returned=55, username="alice")
    telemetry.log_graph_query(rid, "meta_telephony", "llm_generated_cypher", cypher_text="MATCH (", success=False,
                              error="SyntaxError: Invalid input", username="alice")
    telemetry.log_llm_call(rid, "meta_telephony", 2, "answer", "anthropic", 5000, 700, max_tokens=4096,
                           username="alice", latency_ms=4200)
    telemetry.log_event("meta_telephony", "/api/chat", "Why did churn rise in Q3?", "graph_traversal",
                        provider="anthropic", document_names="q3.pdf", llm_call_count=2, input_tokens=6200,
                        output_tokens=740, latency_ms=5300, nodes_referenced=40, relationships_referenced=55,
                        request_id=rid, device_id="dev-1", username="alice", user_id=7, groundedness_score=4,
                        groundedness_reason="supported", temperature=0.2, judge_provider="openai", judge_score=5)

    (run,) = runs(root)
    assert (run.run_id, run.run_type, run.status) == (rid, "chat_request", "Completed")
    assert (run.tenant_id, run.environment_id, run.tenant_user_id) == (TENANT, ENV, USER)
    assert run.duration_ms == 5300
    meta = run.metadata_json
    assert meta["question"] == "Why did churn rise in Q3?" and meta["retrieval_mode"] == "graph_traversal"
    assert meta["model"] == "claude-sonnet-4" and meta["environment"] == "production"
    assert meta["prompt_version"] == "v42" and meta["user_id"] == 7 and meta["judge_score"] == 5
    assert meta["total_cost_usd"] == pytest.approx(6200 * 3e-6 + 740 * 15e-6)

    logged = entries(root)
    assert [e.logger_name for e in logged] == ["c66.llm", "c66.retrieval", "c66.graph", "c66.graph", "c66.llm"]
    assert all(e.run_id == rid and e.correlation_id == rid for e in logged)
    assert all(e.tenant_user_id == USER for e in logged)
    routing, retrieval, graph_ok, graph_bad, answer = logged
    assert routing.message == "LLM call 1: routing via anthropic" and routing.level == "INFO"
    assert routing.metadata_json["prompt_text"] == "Route this question…"
    assert routing.metadata_json["total_cost_usd"] == pytest.approx(1200 * 3e-6 + 40 * 15e-6)
    assert retrieval.metadata_json == {"request_id": rid, "client": "meta_telephony", "records_matched": 12,
                                       "aggregate_computed": True, "username": "alice"}
    assert graph_bad.level == "ERROR" and "SyntaxError" in graph_bad.message
    assert graph_ok.metadata_json["nodes_returned"] == 40
    # call sites are recorded as this test, not c66_logger internals
    assert routing.func_name == "test_chat_request_becomes_one_run_with_its_entries"


def test_run_is_opened_before_the_first_entry(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_llm_call(rid, "meta_telephony", 1, "routing", "anthropic", 10, 2)
    (run,) = runs(root)  # written synchronously, before the entry was queued
    assert run.status == "Running" and run.run_id == rid


def test_failed_request_is_a_failed_run_plus_error_entry(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_event("meta_telephony", "/api/chat", "q", "error", provider="anthropic", success=False,
                        error="Neo4j timeout", request_id=rid)
    (run,) = runs(root)
    assert (run.status, run.error_summary) == ("Failed", "Neo4j timeout")
    (err,) = entries(root)
    assert err.level == "ERROR" and err.run_id == rid and "Neo4j timeout" in err.message


def test_cache_hit_with_no_earlier_calls_is_a_completed_run(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_event("meta_telephony", "/api/chat", "q", "cache_hit", cache_hit=True, request_id=rid,
                        estimated_cost_saved_usd=0.0123)
    (run,) = runs(root)
    assert run.status == "Completed" and run.metadata_json["cache_hit"] is True
    assert run.metadata_json["estimated_cost_saved_usd"] == 0.0123
    assert entries(root) == []


def test_second_log_event_for_a_request_overwrites_the_first(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_event("meta_telephony", "/api/chat", "q", "graph_traversal", request_id=rid)
    telemetry.log_event("meta_telephony", "/api/chat", "q", "error", success=False, error="late", request_id=rid)
    (run,) = runs(root)
    assert run.status == "Failed" and run.error_summary == "late"


def test_other_routes_get_their_own_run_type(telemetry, root):
    telemetry.log_event("meta_telephony", "/api/agent/ask", "q", "poc_agent", request_id=str(uuid.uuid4()))
    assert runs(root)[0].run_type == "api_agent_ask"


def test_request_without_uuid_still_logs(telemetry, root):
    telemetry.log_llm_call("", "meta_telephony", 0, "suggested_questions", "anthropic", 100, 20)
    (e,) = entries(root)
    assert e.run_id is None and e.correlation_id is None and runs(root) == []


# ---------- the other calls ----------

def test_ingestion_is_a_run_with_one_entry(telemetry, root):
    telemetry.log_ingestion_event("meta_telephony", "doc-9", "contracts.pdf", "pdf", uploaded_by="alice",
                                  category="legal", latency_ms=8200, total_chunks=42, entity_count=17,
                                  schema_missing_columns=["region"], llm_cost_usd=0.03, embedding_cost_usd=0.001,
                                  llm_call_count=6)
    (run,) = runs(root)
    assert (run.run_type, run.status, run.duration_ms, run.tenant_user_id) == (
        "document_ingestion", "Completed", 8200, USER)
    assert run.metadata_json["schema_missing_columns"] == "region"
    assert run.metadata_json["total_cost_usd"] == pytest.approx(0.031)
    (e,) = entries(root)
    assert (e.logger_name, e.level, e.run_id) == ("c66.ingestion", "INFO", run.run_id)
    assert e.message == "ingested contracts.pdf: 42 chunks"


def test_failed_ingestion(telemetry, root):
    telemetry.log_ingestion_event("meta_telephony", "doc-10", "bad.xlsx", "xlsx", success=False,
                                  error="no header row")
    assert runs(root)[0].status == "Failed"
    assert entries(root)[0].level == "ERROR" and "no header row" in entries(root)[0].message


def test_feedback_links_to_a_known_request(telemetry, root):
    rid = str(uuid.uuid4())
    telemetry.log_event("meta_telephony", "/api/chat", "q", "graph_traversal", request_id=rid)
    telemetry.log_feedback(rid, "meta_telephony", -1, comment="wrong quarter", username="alice")
    telemetry.log_feedback(str(uuid.uuid4()), "meta_telephony", 1)  # request this process never saw
    known, unknown = entries(root)
    assert (known.run_id, known.correlation_id, known.message) == (rid, rid, "feedback -1")
    assert known.metadata_json["comment"] == "wrong quarter" and known.tenant_user_id == USER
    assert unknown.run_id is None and unknown.correlation_id  # no FK risk; still joinable


def test_eval_runs_go_to_the_system_tenant(telemetry, root):
    telemetry.log_eval_run("hallucination_v3", 17, "numeric", "What was Q3 revenue?", False,
                           ["$4.2M"], ["$5M"], "Q3 revenue was $5M")
    (e,) = entries(root)
    assert (e.tenant_id, e.environment_id, e.logger_name, e.level) == (SYSTEM_TENANT, SYSTEM_ENV, "c66.eval",
                                                                       "WARNING")
    assert e.message == "eval hallucination_v3/17: failed"
    assert e.metadata_json["missing_terms"] == "$4.2M" and e.metadata_json["forbidden_found"] == "$5M"


def test_deletion_receipt(telemetry, root):
    telemetry.log_deletion_receipt("acme", "alice", nodes_deleted=120, chunks_deleted=900, cache_keys_deleted=14,
                                   observability_rows_deleted=8)
    (e,) = entries(root)
    assert (e.tenant_id, e.logger_name, e.level) == (OTHER_TENANT, "c66.governance", "INFO")
    assert e.message == "tenant data purged by alice" and e.metadata_json["chunks_deleted"] == 900


def test_audit_event(telemetry, root):
    telemetry.log_audit_event("alice", "acme", "purge_tenant", "requested from admin console")
    (e,) = entries(root)
    assert (e.tenant_id, e.logger_name, e.message, e.tenant_user_id) == (OTHER_TENANT, "c66.audit",
                                                                          "purge_tenant", USER)
    assert e.metadata_json == {"username": "alice", "action": "purge_tenant",
                               "detail": "requested from admin console"}


def test_print_replacement_via_logging(telemetry, root):
    log = logging.getLogger("c66.app")
    log.setLevel(logging.INFO)
    handler = telemetry.logging_handler()
    log.addHandler(handler)
    try:
        log.warning("log_event (ClickHouse) error (non-fatal): %s", "timeout")
    finally:
        log.removeHandler(handler)
    (e,) = entries(root)
    assert (e.tenant_id, e.logger_name, e.level) == (SYSTEM_TENANT, "c66.app", "WARNING")
    assert e.message == "log_event (ClickHouse) error (non-fatal): timeout"


# ---------- never raises, like the originals ----------

def test_unknown_client_is_skipped_and_reported_once(telemetry, root, caplog):
    with caplog.at_level(logging.WARNING, logger="c66_logger"):
        telemetry.log_audit_event("bob", "no_such_client", "login")
        telemetry.log_audit_event("bob", "no_such_client", "logout")
    assert entries(root) == []
    assert sum("no_such_client" in r.message for r in caplog.records) == 1


def test_failures_never_reach_the_caller(root):
    def broken_resolver(client):
        raise RuntimeError("tenant service down")

    t = Telemetry(root, resolve_tenant=broken_resolver)
    t.log_event("x", "/api/chat", "q", "m", request_id=str(uuid.uuid4()))
    t.log_llm_call(str(uuid.uuid4()), "x", 1, "answer", "anthropic", 1, 1)
    t.log_eval_run("s", 1, "c", "q", True, [], [], "a")  # no system tenant configured
    assert entries(root) == [] and runs(root) == []


def test_bad_values_do_not_raise(telemetry, root):
    telemetry.log_llm_call(str(uuid.uuid4()), "meta_telephony", "not-a-number", "answer", "anthropic", 1, 1)
    assert entries(root) == []  # dropped with a warning instead of breaking the request


# ---------- bind() and record_run() underneath ----------

def test_bound_loggers_share_one_writer_and_sink():
    root = AuditLogger(target_type="memory")
    a = root.bind(tenant_id=TENANT, environment_id=ENV, logger_name="a")
    b = root.bind(tenant_id=OTHER_TENANT, environment_id=OTHER_ENV)
    assert a._writer is root._writer and b.sink is root.sink
    a.info(message="from a")
    b.info(message="from b")
    b.close()  # no-op for bound loggers
    root.flush()
    assert {(e.tenant_id, e.logger_name) for e in root.sink.entries} == {(TENANT, "a"), (OTHER_TENANT, "c66_logger")}
    root.close()


def test_root_without_tenant_cannot_log(root):
    with pytest.raises(ConfigurationError, match="bind"):
        root.info(message="x")
    with pytest.raises(ConfigurationError):
        AuditLogger(tenant_id=TENANT, target_type="memory")  # environment missing


def test_record_run_derives_start_from_duration():
    audit = AuditLogger(TENANT, ENV, target_type="memory", mode="sync")
    run = audit.record_run("nightly", status="Failed", duration_ms=1500, error_summary="x")
    assert run.duration_ms == 1500 and (run.ended_at - run.started_at).total_seconds() == pytest.approx(1.5)
    assert audit.sink.runs[run.run_id].status == "Failed"


# ---------- replay against the real databases ----------

def _replay(telemetry):
    rid = str(uuid.uuid4())
    telemetry.log_llm_call(rid, "meta_telephony", 1, "routing", "anthropic", 1200, 40, username="alice")
    telemetry.log_retrieval_event(rid, "meta_telephony", 12, True, username="alice")
    telemetry.log_graph_query(rid, "meta_telephony", "graph_traversal", cypher_text="MATCH (n) RETURN n",
                              nodes_returned=40)
    telemetry.log_event("meta_telephony", "/api/chat", "Why did churn rise?", "graph_traversal",
                        provider="anthropic", input_tokens=1200, output_tokens=40, latency_ms=900,
                        request_id=rid, username="alice")
    telemetry.log_feedback(rid, "meta_telephony", 1, username="alice")
    telemetry.log_ingestion_event("meta_telephony", "doc-1", "a.pdf", "pdf", uploaded_by="alice", total_chunks=3)
    telemetry.log_eval_run("set", 1, "c", "q", True, [], [], "a")
    telemetry.log_deletion_receipt("acme", "alice", chunks_deleted=5)
    telemetry.log_audit_event("alice", "acme", "delete_document", "doc-1")
    telemetry.flush()
    return rid


def _telemetry(root):
    return Telemetry(root, resolve_tenant=TENANTS.get, resolve_user={"alice": USER}.get,
                     system_tenant=(SYSTEM_TENANT, SYSTEM_ENV), cost_fn=cost_fn, environment="test")


@pytest.mark.integration
def test_replay_on_postgres(pg_dsn):
    dropped = []
    with AuditLogger(target_type="postgres", connection={"dsn": pg_dsn}, flush_interval=0.1,
                     on_drop=lambda e, why: dropped.append(why)) as root:
        rid = _replay(_telemetry(root))
    assert dropped == []
    run = pg_rows(pg_dsn, "SELECT * FROM logs.log_run WHERE run_id = %s", (rid,))[0]
    assert (run["run_type"], run["status"], run["duration_ms"]) == ("chat_request", "Completed", 900)
    assert run["metadata_json"]["total_cost_usd"] == pytest.approx(1200 * 3e-6 + 40 * 15e-6)
    names = pg_rows(pg_dsn, "SELECT logger_name FROM logs.log_entry WHERE correlation_id = %s ORDER BY logged_at",
                    (rid,))
    assert [n["logger_name"] for n in names] == ["c66.llm", "c66.retrieval", "c66.graph", "c66.feedback"]
    counts = {r["logger_name"]: r["n"] for r in pg_rows(
        pg_dsn, "SELECT logger_name, count(*) AS n FROM logs.log_entry GROUP BY 1")}
    for name in ("c66.ingestion", "c66.eval", "c66.governance", "c66.audit"):
        assert counts.get(name, 0) >= 1, name


def test_replay_on_clickhouse():
    pytest.importorskip("chdb")
    from .chdb_client import ChdbClient

    client = ChdbClient()
    with AuditLogger(target_type="clickhouse", connection={"connection": client, "create_tables": True},
                     flush_interval=0.1) as root:
        rid = _replay(_telemetry(root))
    run = client.rows(f"SELECT run_type, status, duration_ms, JSONExtractFloat(metadata_json, 'total_cost_usd') "
                      f"AS cost FROM logs.log_run FINAL WHERE run_id = '{rid}'")[0]
    assert (run["run_type"], run["status"], run["duration_ms"]) == ("chat_request", "Completed", 900)
    assert run["cost"] == pytest.approx(1200 * 3e-6 + 40 * 15e-6)
    names = client.rows(f"SELECT logger_name FROM logs.log_entry FINAL WHERE correlation_id = '{rid}' "
                        "ORDER BY logged_at")
    assert [n["logger_name"] for n in names] == ["c66.llm", "c66.retrieval", "c66.graph", "c66.feedback"]
    loggers = {r["logger_name"] for r in client.rows("SELECT DISTINCT logger_name FROM logs.log_entry")}
    assert {"c66.ingestion", "c66.eval", "c66.governance", "c66.audit"} <= loggers

# c66-chatbot logging → c66_logger

_Last updated 2026-10-01 · Source reviewed: c66-chatbot `app_common.py` at commit `5bb35d3` · See also: [architecture.md](architecture.md) · [example 16](../examples/16_chatbot_telemetry.py)_

Every logging call c66-chatbot makes today can be made through c66_logger. The
data goes to `log_run` / `log_entry`, in Postgres or ClickHouse, using the
connections the app already gets from its connection library (`get_pg_conn()`,
`get_ch_client()`, or c66_clients once it's filled in).

`c66_logger.Telemetry` has one method per helper in `app_common.py`. Each
method has the **same name, parameters and defaults**, and a test checks this.
So a call site changes only where the function comes from. Like the
originals, the methods never raise.

## The nine helpers

| Today (app_common.py) | Old table | Now | `logger_name` / `run_type` | Level |
| --- | --- | --- | --- | --- |
| `log_event()` — one per `/api/chat` request (8 call sites) | ClickHouse `events` | **log_run**, `run_id` = `request_id` | `chat_request` (other routes: e.g. `api_agent_ask`) | status Completed / Failed. On failure also an ERROR entry in `c66.chat` |
| `log_llm_call()` (10) | ClickHouse `llm_calls` | log_entry, in the request's run | `c66.llm` | INFO, or ERROR when `success=False` |
| `log_graph_query()` (8) | ClickHouse `graph_queries` | log_entry, in the request's run | `c66.graph` | INFO / ERROR |
| `log_retrieval_event()` (1) | ClickHouse `retrieval_events` | log_entry, in the request's run | `c66.retrieval` | INFO |
| `log_ingestion_event()` (1) | ClickHouse `ingestion_events` | **log_run** + one log_entry in it | `document_ingestion` / `c66.ingestion` | Completed / Failed; entry INFO / ERROR |
| `log_feedback()` (1) | ClickHouse `feedback` | log_entry, `correlation_id` = `request_id` | `c66.feedback` | INFO |
| `log_eval_run()` (1) | ClickHouse `eval_runs` | log_entry, under the system tenant (no client) | `c66.eval` | INFO if passed, WARNING if not |
| `log_deletion_receipt()` (1) | ClickHouse `deletion_receipts` | log_entry | `c66.governance` | INFO / ERROR |
| `log_audit_event()` (24) | Postgres `audit_log` | log_entry | `c66.audit` | INFO |
| `print(...)` (~400) | stdout only | log_entry, through `logging` + `telemetry.logging_handler()` | the `logging` logger's name | the logging level used |

## Where each field goes

- **Every column** of the old table is kept in `metadata_json`, under the same name. The old truncation lengths and cost math are kept too. For example, `events.total_cost_usd` → `log_run.metadata_json.total_cost_usd`, and `llm_calls.prompt_text` → `log_entry.metadata_json.prompt_text`.
- **Tenant:** `client` / `client_name` → `tenant_id` + `environment_id`, through `resolve_tenant(client)`. The chatbot only has client names, so this function has to come from wherever app.tenant lives. A client it can't resolve is skipped with one warning.
- **User:** `username` / `uploaded_by` / `requested_by` → `tenant_user_id`, through `resolve_user(username)`. This is optional; the username always stays in `metadata_json`. The chatbot's integer `user_id` stays in `metadata_json`.
- **Request:** `request_id` → `correlation_id` on every row of the request, and `run_id` on the request's entries. Feedback gets `run_id` only when this process has seen the request, because `run_id` is a foreign key.
- **Message:** a readable `message` is generated for each entry, e.g. "LLM call 2: answer via anthropic", "graph query: llm_generated_cypher failed: …", "tenant data purged by alice".
- **Run duration:** `latency_ms` → `log_run.duration_ms` (and `started_at = ended_at − latency`). The request's error → `error_summary`.
- **Costs and model:** cost fields are computed by the `cost_fn` you pass (`_calc_cost_split`), and the model by `model_fn` (`PROVIDER_MODEL.get`), exactly as today.
- **Environment and prompt version:** `environment` (`APP_ENVIRONMENT`) and `prompt_version` (`PROMPT_VERSION`) are set once on `Telemetry`.

**Run before entries.** The first call that carries a new `request_id` opens that request's `log_run` as Running, synchronously. Every entry that references it therefore lands after it, as the `log_entry.run_id` foreign key requires. `log_event()` at the end of the request then writes the final state as an upsert. If the request already logged earlier calls, `started_at` is kept.

## Switching app_common.py over

```python
# app_common.py — once at startup
from c66_logger import AuditLogger, Telemetry

_audit = AuditLogger(target_type="clickhouse", connection=get_ch_client)   # or target_type="postgres", connection=get_pg_conn
telemetry = Telemetry(
    _audit,
    resolve_tenant=lookup_tenant_ids,          # client_name -> (tenant_id, environment_id), from app.tenant
    resolve_user=lookup_tenant_user_id,        # username -> app.tenant_user id (optional)
    system_tenant=(PLATFORM_TENANT_ID, PLATFORM_ENV_ID),   # eval runs + plain logging
    cost_fn=_calc_cost_split,
    model_fn=PROVIDER_MODEL.get,
    environment=APP_ENVIRONMENT,
    prompt_version=PROMPT_VERSION,
)
log_event = telemetry.log_event
log_llm_call = telemetry.log_llm_call
log_graph_query = telemetry.log_graph_query
log_retrieval_event = telemetry.log_retrieval_event
log_ingestion_event = telemetry.log_ingestion_event
log_feedback = telemetry.log_feedback
log_eval_run = telemetry.log_eval_run
log_deletion_receipt = telemetry.log_deletion_receipt
log_audit_event = telemetry.log_audit_event

# print(...) → logging
log = logging.getLogger("c66")
log.addHandler(telemetry.logging_handler())
# print(f"log_event (ClickHouse) error (non-fatal): {e}")  becomes  log.warning("log_event (ClickHouse) error (non-fatal): %s", e)
```

`app.py`, `app_governance.py` and the scoring functions import these names from `app_common`, so their call sites don't change.

## What's different at runtime

| | Today | With c66_logger |
| --- | --- | --- |
| Connections | each call opens a new ClickHouse client (`get_ch_client()`). Each audit event borrows a Postgres connection | one shared client or pool. Postgres borrows from `get_pg_conn()` per batch and returns it |
| When the write happens | inline, inside the request, on every call | entries are queued and batched by one background thread (500 rows / 2 s). Only run rows are written inline |
| On failure | the error is printed, the row is lost | retried 3× with backoff, then `on_drop` (e.g. a dead-letter file). FK-violating rows are dropped individually |
| Duplicates on retry | n/a | none: `ON CONFLICT DO NOTHING` (Postgres), dedup token + ReplacingMergeTree (ClickHouse) |
| Tenant identity | `client` string | `tenant_id` / `environment_id` UUIDs (FKs to app.* in Postgres) |

## Dashboards: reading the new tables

The Observability and AI-economics pages query `events` / `llm_calls` today. The same numbers come from:

```sql
-- Postgres: per-tenant cost and latency per day (was: events)
SELECT tenant_id, date_trunc('day', started_at) AS day,
       count(*)                                          AS requests,
       sum((metadata_json->>'total_cost_usd')::numeric)  AS cost_usd,
       avg(duration_ms)                                  AS avg_latency_ms,
       avg((metadata_json->>'cache_hit')::boolean::int)  AS cache_hit_rate
FROM logs.log_run WHERE run_type = 'chat_request'
GROUP BY 1, 2 ORDER BY 2 DESC;

-- ClickHouse: same, current run state via FINAL
SELECT tenant_id, toDate(started_at) AS day, count() AS requests,
       sum(JSONExtractFloat(metadata_json, 'total_cost_usd')) AS cost_usd,
       avg(duration_ms) AS avg_latency_ms
FROM logs.log_run FINAL WHERE run_type = 'chat_request'
GROUP BY tenant_id, day ORDER BY day DESC;

-- per-call breakdown of one request (was: llm_calls WHERE request_id = ...)
SELECT JSONExtractString(metadata_json, 'purpose') AS purpose,
       JSONExtractUInt(metadata_json, 'input_tokens') AS input_tokens,
       JSONExtractFloat(metadata_json, 'total_cost_usd') AS cost_usd
FROM logs.log_entry WHERE run_id = '<request_id>' AND logger_name = 'c66.llm' ORDER BY logged_at;
```

## Open points for Pal

- **Tenant ids:** where should `resolve_tenant` read from? The chatbot's own `tenants` table has a serial `id` and `client_name`, not the `app.tenant` UUIDs these tables reference.
- **System tenant:** which tenant/environment should own platform-level events (eval runs, startup messages)?
- **Dashboards:** re-point them to `log_run` / `log_entry`, using the queries above.
- **Cut-over:** keep writing the old ClickHouse tables during the switch, or cut over in one go?

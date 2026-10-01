"""
c66-chatbot's logging calls, reimplemented on c66_logger.

``Telemetry`` has one method per logging helper in c66-chatbot's
``app_common.py``, with the same name, parameters and defaults, so a call site
changes only where the function comes from:

    log_event               -> log_run    run_type "chat_request" (one per /api/chat request)
    log_llm_call            -> log_entry  logger "c66.llm"         (run_id = the request's run)
    log_graph_query         -> log_entry  logger "c66.graph"
    log_retrieval_event     -> log_entry  logger "c66.retrieval"
    log_ingestion_event     -> log_run    run_type "document_ingestion" + log_entry "c66.ingestion"
    log_feedback            -> log_entry  logger "c66.feedback"    (correlation_id = request_id)
    log_eval_run            -> log_entry  logger "c66.eval"        (system tenant)
    log_deletion_receipt    -> log_entry  logger "c66.governance"
    log_audit_event         -> log_entry  logger "c66.audit"
    print(...)              -> log_entry  via logging + Telemetry.logging_handler()

Every column the old ClickHouse/Postgres tables had is kept, under the same
name, in ``metadata_json`` — so ``events.total_cost_usd`` becomes
``metadata_json->>'total_cost_usd'`` (Postgres) or
``JSONExtractFloat(metadata_json, 'total_cost_usd')`` (ClickHouse).

Like the originals, these methods never raise: a logging failure must never
break the answer, upload or deletion being logged. Problems are reported on
the ``c66_logger`` logger.

Tenants: the chatbot names tenants by ``client`` (e.g. "meta_telephony");
log_entry/log_run need tenant_id + environment_id UUIDs. ``resolve_tenant``
maps one to the other; clients it returns None for are skipped (and
reported once).
"""

from __future__ import annotations

import collections
import logging
import threading
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

from .handler import AuditLogHandler
from .logger import AuditLogger
from .record import LogRun

_log = logging.getLogger("c66_logger")

TenantIds = Tuple[str, str]  # (tenant_id, environment_id)
CostFn = Callable[[str, int, int], Tuple[float, float]]  # (provider, input_tokens, output_tokens) -> (in $, out $)

_CHAT_ROUTE = "/api/chat"


class Telemetry:
    def __init__(
        self,
        audit: AuditLogger,
        *,
        resolve_tenant: Callable[[str], Optional[TenantIds]],
        resolve_user: Optional[Callable[[str], Optional[str]]] = None,
        system_tenant: Optional[TenantIds] = None,
        cost_fn: Optional[CostFn] = None,
        model_fn: Optional[Callable[[str], str]] = None,
        environment: str = "",
        prompt_version: Any = None,
        request_cache_size: int = 10_000,
    ):
        """
        audit           an AuditLogger created by the host (usually without tenant ids;
                        Telemetry binds one per tenant, all sharing its connection)
        resolve_tenant  client name -> (tenant_id, environment_id), or None to skip
        resolve_user    username -> app.tenant_user id (UUID), optional
        system_tenant   (tenant_id, environment_id) for events with no client:
                        log_eval_run and the logging_handler() bridge
        cost_fn         e.g. app_common._calc_cost_split; without it cost fields are omitted
        model_fn        provider -> model name, e.g. PROVIDER_MODEL.get
        environment     e.g. APP_ENVIRONMENT — stored on every request run like events.environment
        prompt_version  default for log_event(prompt_version=None), like PROMPT_VERSION
        """
        self._audit = audit
        self._resolve_tenant = resolve_tenant
        self._resolve_user = resolve_user
        self._system_tenant = system_tenant
        self._cost_fn = cost_fn
        self._model_fn = model_fn
        self._environment = environment
        self._prompt_version = prompt_version
        self._lock = threading.Lock()
        self._tenants: Dict[Any, Optional[AuditLogger]] = {}
        self._warned: set = set()
        self._requests: "collections.OrderedDict[str, LogRun]" = collections.OrderedDict()
        self._request_cache_size = request_cache_size

    # ---- request lifecycle -----------------------------------------------

    def start_request(self, request_id: str, client: str, route: str = _CHAT_ROUTE, username: str = "") -> None:
        """
        Optional: open the request's log_run as 'Running' up front. Otherwise it
        is opened automatically by the first call that carries this request_id.
        Either way the run exists before any entry references it.
        """
        self._guard("start_request", self._ensure_request_run, request_id, client, route, username)

    # ---- c66-chatbot helpers ---------------------------------------------

    def log_event(self, client, route, question, retrieval_mode, provider="", document_names="",
                  llm_call_count=0, input_tokens=0, output_tokens=0, latency_ms=0,
                  nodes_referenced=0, relationships_referenced=0, cache_hit=False,
                  success=True, error="", request_id="", device_id="", username="", user_id=None,
                  contains_pii=False, groundedness_score=-1, groundedness_reason="", prompt_version=None,
                  temperature=0.0, total_retry_count=0, estimated_cost_saved_usd=0.0,
                  judge_provider="", judge_score=-1, judge_reason="",
                  self_consistency_score=-1, self_consistency_reason=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            metadata = {
                "client": client, "route": route, "question": _s(question)[:500],
                "document_names": _s(document_names)[:500], "retrieval_mode": retrieval_mode,
                "provider": provider, "model": self._model(provider), "llm_call_count": int(llm_call_count),
                "input_tokens": int(input_tokens), "output_tokens": int(output_tokens),
                **self._costs(provider, input_tokens, output_tokens),
                "latency_ms": int(latency_ms), "nodes_referenced": int(nodes_referenced),
                "relationships_referenced": int(relationships_referenced), "cache_hit": bool(cache_hit),
                "success": bool(success), "error": _s(error)[:500], "request_id": request_id,
                "device_id": device_id, "username": username,
                "user_id": int(user_id) if user_id is not None else 0, "contains_pii": bool(contains_pii),
                "groundedness_score": int(groundedness_score),
                "groundedness_reason": _s(groundedness_reason)[:500],
                "prompt_version": prompt_version if prompt_version is not None else self._prompt_version,
                "temperature": float(temperature), "total_retry_count": int(total_retry_count),
                "environment": self._environment, "estimated_cost_saved_usd": float(estimated_cost_saved_usd),
                "judge_provider": judge_provider, "judge_score": int(judge_score),
                "judge_reason": _s(judge_reason)[:600],
                "self_consistency_score": int(self_consistency_score),
                "self_consistency_reason": _s(self_consistency_reason)[:500],
            }
            run_id = _uuid_or_none(request_id)
            with self._lock:
                started = self._requests.get(run_id) if run_id else None
            run = audit.record_run(
                _run_type(route),
                run_id=run_id,
                status="Completed" if success else "Failed",
                started_at=started.started_at if started else None,
                duration_ms=int(latency_ms) if latency_ms else None,
                error_summary=_s(error) or None,
                metadata=metadata,
                tenant_user_id=self._user(username),
            )
            if run_id:
                self._remember(run)  # the row exists now: later feedback can reference it
            if not success:
                audit.error({"message": f"{route} failed: {_s(error)}"[:2000], "logger_name": "c66.chat",
                             "request_id": request_id, "retrieval_mode": retrieval_mode, "provider": provider},
                            **self._request_ids(run.run_id, request_id))
        self._guard("log_event", _write)

    def log_ingestion_event(self, client, doc_id, filename, doc_type, uploaded_by="", category="",
                            success=True, error="", latency_ms=0, total_chunks=0,
                            embedding_missing=0, pg_insert_failed=0, chunks_with_pii=0,
                            entity_count=0, relationship_count=0, rows_written=0, rows_skipped=0,
                            sparse_columns_count=0, schema_status="", schema_missing_columns=None,
                            schema_new_columns=None, llm_cost_usd=0.0, embedding_cost_usd=0.0,
                            llm_call_count=0):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            metadata = {
                "client": client, "doc_id": doc_id, "filename": _s(filename)[:500], "doc_type": doc_type,
                "uploaded_by": uploaded_by, "category": category, "success": bool(success),
                "error": _s(error)[:500], "latency_ms": int(latency_ms), "total_chunks": int(total_chunks),
                "embedding_missing": int(embedding_missing), "pg_insert_failed": int(pg_insert_failed),
                "chunks_with_pii": int(chunks_with_pii), "entity_count": int(entity_count),
                "relationship_count": int(relationship_count), "rows_written": int(rows_written),
                "rows_skipped": int(rows_skipped), "sparse_columns_count": int(sparse_columns_count),
                "schema_status": schema_status,
                "schema_missing_columns": ",".join(schema_missing_columns) if schema_missing_columns else "",
                "schema_new_columns": ",".join(schema_new_columns) if schema_new_columns else "",
                "llm_cost_usd": round(float(llm_cost_usd), 6),
                "embedding_cost_usd": round(float(embedding_cost_usd), 6),
                "total_cost_usd": round(float(llm_cost_usd) + float(embedding_cost_usd), 6),
                "llm_call_count": int(llm_call_count),
            }
            user = self._user(uploaded_by)
            run = audit.record_run(
                "document_ingestion",
                status="Completed" if success else "Failed",
                duration_ms=int(latency_ms) if latency_ms else None,
                error_summary=_s(error) or None,
                metadata=metadata,
                tenant_user_id=user,
            )
            message = (f"ingested {filename}: {int(total_chunks)} chunks" if success
                       else f"ingestion failed for {filename}: {_s(error)}")
            audit.log({"message": message[:2000], "logger_name": "c66.ingestion", "doc_id": doc_id,
                       "filename": _s(filename)[:500], "doc_type": doc_type},
                      level="INFO" if success else "ERROR", run_id=run.run_id, tenant_user_id=user)
        self._guard("log_ingestion_event", _write)

    def log_llm_call(self, request_id, client, call_index, purpose, provider, input_tokens, output_tokens,
                     max_tokens=0, username="", temperature=0.0, latency_ms=0, retry_count=0, finish_reason="",
                     prompt_text="", response_text="", success=True, error=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            item = {
                "message": f"LLM call {int(call_index)}: {purpose} via {provider}"
                           + ("" if success else f" failed: {_s(error)}"),
                "logger_name": "c66.llm",
                "request_id": request_id, "client": client, "call_index": int(call_index), "purpose": purpose,
                "provider": provider, "model": self._model(provider),
                "input_tokens": int(input_tokens), "output_tokens": int(output_tokens),
                **self._costs(provider, input_tokens, output_tokens),
                "max_tokens": int(max_tokens), "username": username, "temperature": float(temperature),
                "latency_ms": int(latency_ms), "retry_count": int(retry_count), "finish_reason": finish_reason,
                "prompt_text": _s(prompt_text), "response_text": _s(response_text),
                "success": bool(success), "error": _s(error)[:1000],
            }
            self._entry(audit, item, success, request_id, client, username)
        self._guard("log_llm_call", _write)

    def log_graph_query(self, request_id, client, query_type, cypher_text="", anchor_count=0,
                        nodes_returned=0, relationships_returned=0, success=True, error="",
                        device_id="", result_data="", username=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            item = {
                "message": f"graph query: {query_type}" + ("" if success else f" failed: {_s(error)}"),
                "logger_name": "c66.graph",
                "request_id": request_id, "client": client, "query_type": query_type,
                "cypher_text": _s(cypher_text)[:2000], "anchor_count": int(anchor_count),
                "nodes_returned": int(nodes_returned), "relationships_returned": int(relationships_returned),
                "success": bool(success), "error": _s(error)[:500], "device_id": device_id,
                "result_data": _s(result_data)[:5000], "username": username,
            }
            self._entry(audit, item, success, request_id, client, username)
        self._guard("log_graph_query", _write)

    def log_retrieval_event(self, request_id, client, records_matched, aggregate_computed, username=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            item = {
                "message": f"structured retrieval: {int(records_matched)} records"
                           + (", aggregate computed" if aggregate_computed else ""),
                "logger_name": "c66.retrieval",
                "request_id": request_id, "client": client, "records_matched": int(records_matched),
                "aggregate_computed": bool(aggregate_computed), "username": username,
            }
            self._entry(audit, item, True, request_id, client, username)
        self._guard("log_retrieval_event", _write)

    def log_feedback(self, request_id, client, rating, comment="", username=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            run_id = _uuid_or_none(request_id)
            with self._lock:  # only link run_id when we know the run row exists
                known = run_id in self._requests if run_id else False
            ids = {"correlation_id": run_id}
            if known:
                ids["run_id"] = run_id
            user = self._user(username)
            if user:
                ids["tenant_user_id"] = user
            audit.info({
                "message": f"feedback {'+1' if int(rating) > 0 else '-1'}",
                "logger_name": "c66.feedback",
                "request_id": request_id, "client": client, "username": username,
                "rating": int(rating), "comment": _s(comment)[:1000],
            }, **ids)
        self._guard("log_feedback", _write)

    def log_eval_run(self, eval_set_name, question_id, category, question, passed, missing_terms,
                     forbidden_found, answer):
        def _write():
            audit = self._system()
            if audit is None:
                return
            audit.log({
                "message": f"eval {eval_set_name}/{question_id}: {'passed' if passed else 'failed'}",
                "logger_name": "c66.eval",
                "eval_set_name": eval_set_name, "question_id": str(question_id), "category": category,
                "question": _s(question)[:500], "passed": bool(passed),
                "missing_terms": ",".join(missing_terms or []), "forbidden_found": ",".join(forbidden_found or []),
                "answer": _s(answer)[:1000],
            }, level="INFO" if passed else "WARNING")
        self._guard("log_eval_run", _write)

    def log_deletion_receipt(self, client, requested_by, nodes_deleted=0, chunks_deleted=0,
                             cache_keys_deleted=0, observability_rows_deleted=0, success=True, error=""):
        def _write():
            audit = self._tenant(client)
            if audit is None:
                return
            user = self._user(requested_by)
            audit.log({
                "message": (f"tenant data purged by {requested_by}" if success
                            else f"tenant data purge by {requested_by} failed: {_s(error)}"),
                "logger_name": "c66.governance",
                "client": client, "requested_by": requested_by, "nodes_deleted": int(nodes_deleted),
                "chunks_deleted": int(chunks_deleted), "cache_keys_deleted": int(cache_keys_deleted),
                "observability_rows_deleted": int(observability_rows_deleted), "success": bool(success),
                "error": _s(error)[:500],
            }, level="INFO" if success else "ERROR", **({"tenant_user_id": user} if user else {}))
        self._guard("log_deletion_receipt", _write)

    def log_audit_event(self, username, client_name, action, detail=""):
        def _write():
            audit = self._tenant(client_name)
            if audit is None:
                return
            user = self._user(username)
            audit.info({
                "message": action or "audit event",
                "logger_name": "c66.audit",
                "username": username, "action": action, "detail": _s(detail)[:2000],
            }, **({"tenant_user_id": user} if user else {}))
        self._guard("log_audit_event", _write)

    # ---- print() replacement --------------------------------------------

    def logging_handler(self, level: int = logging.INFO, client: Optional[str] = None) -> AuditLogHandler:
        """
        A logging.Handler for the app's operational messages (today's print()
        calls). Entries go to ``client``'s tenant, or the system tenant.

            log = logging.getLogger("c66")
            log.addHandler(telemetry.logging_handler())
            log.warning("log_event (ClickHouse) error (non-fatal): %s", e)   # was print(...)
        """
        audit = self._tenant(client) if client is not None else self._system()
        if audit is None:
            raise ValueError("no tenant for the logging handler: pass client= or set system_tenant")
        return AuditLogHandler(audit, level=level)

    def flush(self, timeout: Optional[float] = 10.0) -> bool:
        return self._audit.flush(timeout)

    # ---- internals -------------------------------------------------------

    def _guard(self, name: str, fn: Callable, *args: Any) -> None:
        try:
            fn(*args)
        except Exception:
            _log.warning("c66_logger telemetry: %s failed (non-fatal)", name, exc_info=True)

    def _tenant(self, client: Optional[str]) -> Optional[AuditLogger]:
        with self._lock:
            if client in self._tenants:
                return self._tenants[client]
        ids = self._resolve_tenant(client)
        audit = self._audit.bind(tenant_id=ids[0], environment_id=ids[1]) if ids else None
        with self._lock:
            self._tenants.setdefault(client, audit)
            if audit is None and client not in self._warned:
                self._warned.add(client)
                _log.warning("c66_logger telemetry: no tenant/environment for client %r; its events are skipped",
                             client)
            return self._tenants[client]

    def _system(self) -> Optional[AuditLogger]:
        if self._system_tenant is None:
            if "__system__" not in self._warned:
                self._warned.add("__system__")
                _log.warning("c66_logger telemetry: no system_tenant configured; eval/handler events are skipped")
            return None
        with self._lock:
            if "__system__" not in self._tenants:
                self._tenants["__system__"] = self._audit.bind(
                    tenant_id=self._system_tenant[0], environment_id=self._system_tenant[1])
            return self._tenants["__system__"]

    def _ensure_request_run(self, request_id: str, client: str, route: str = _CHAT_ROUTE,
                            username: str = "") -> Optional[str]:
        run_id = _uuid_or_none(request_id)
        if run_id is None:
            return None
        with self._lock:
            if run_id in self._requests:
                self._requests.move_to_end(run_id)
                return run_id
        audit = self._tenant(client)
        if audit is None:
            return None
        run = audit.start_run(_run_type(route), run_id=run_id, tenant_user_id=self._user(username),
                              metadata={"client": client, "route": route, "request_id": request_id,
                                        "environment": self._environment})
        self._remember(run)
        return run_id

    def _remember(self, run: LogRun) -> None:
        with self._lock:
            self._requests[run.run_id] = run
            self._requests.move_to_end(run.run_id)
            while len(self._requests) > self._request_cache_size:
                self._requests.popitem(last=False)

    def _entry(self, audit: AuditLogger, item: Dict[str, Any], success: bool, request_id: str,
               client: str, username: str) -> None:
        run_id = None
        try:
            run_id = self._ensure_request_run(request_id, client, username=username)
        except Exception:
            _log.warning("c66_logger telemetry: could not open run for request %s", request_id, exc_info=True)
        ids = self._request_ids(run_id, request_id)
        user = self._user(username)
        if user:
            ids["tenant_user_id"] = user
        audit.log(item, level="INFO" if success else "ERROR", **ids)

    @staticmethod
    def _request_ids(run_id: Optional[str], request_id: str) -> Dict[str, Any]:
        corr = _uuid_or_none(request_id)
        ids: Dict[str, Any] = {}
        if run_id:
            ids["run_id"] = run_id
        if corr:
            ids["correlation_id"] = corr
        return ids

    def _user(self, username: str) -> Optional[str]:
        if not username or self._resolve_user is None:
            return None
        try:
            return _uuid_or_none(self._resolve_user(username))
        except Exception:
            return None

    def _model(self, provider: str) -> str:
        if self._model_fn is None:
            return provider
        try:
            return self._model_fn(provider) or provider
        except Exception:
            return provider

    def _costs(self, provider: str, input_tokens: Any, output_tokens: Any) -> Dict[str, float]:
        if self._cost_fn is None:
            return {}
        input_cost, output_cost = self._cost_fn(provider, int(input_tokens), int(output_tokens))
        return {"input_cost_usd": float(input_cost), "output_cost_usd": float(output_cost),
                "total_cost_usd": round(float(input_cost) + float(output_cost), 6)}


def _run_type(route: str) -> str:
    if route == _CHAT_ROUTE:
        return "chat_request"
    return (str(route).strip("/").replace("/", "_") or "request")[:50]


def _uuid_or_none(value: Any) -> Optional[str]:
    if not value:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        return None


def _s(value: Any) -> str:
    return "" if value is None else str(value)


__all__ = ["Telemetry"]

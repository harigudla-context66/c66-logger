"""
Case 20 — c66-chatbot's logging calls (Telemetry) on the app's real setup:
.env + connections.yaml + enterprise_connectors, to Postgres and ClickHouse.

The calls are case 16's, unchanged: one /api/chat request (LLM calls, a
retrieval, a graph query, the request event, feedback) plus an ingestion,
an eval run, a deletion receipt, an audit event and a plain logging warning.
Only the connection differs: it comes from connections.yaml by name, the same
way as case 19.

    pip install -e "/path/to/c66-data-connection-layer[postgres,clickhouse,yaml]" python-dotenv
    cp examples/.env.example examples/.env          # then edit it
    python examples/20_telemetry_connections_yaml.py                  # both targets
    python examples/20_telemetry_connections_yaml.py clickhouse       # or just one: postgres | clickhouse

The chatbot names tenants by client; case 16's CLIENTS map points them at the
demo tenants in tests/sql/app_stub.sql (meta_telephony -> salesforce,
acme -> hubspot, system events -> zendesk).
"""

import importlib
import sys
from pathlib import Path

from c66_logger import AuditLogger, Telemetry

try:
    from dotenv import load_dotenv
    from enterprise_connectors import ConnectorManager
except ImportError:
    raise SystemExit('needs c66-data-connection-layer and python-dotenv: pip install -e "<its folder>[postgres,clickhouse,yaml]" python-dotenv') from None

chatbot = importlib.import_module("16_chatbot_telemetry")          # case 16: CLIENTS, USERS, the chatbot's calls

HERE = Path(__file__).resolve().parent
TARGETS = {"postgres": "logs_db", "clickhouse": "logs_ch"}       # target_type -> connection name in connections.yaml


def read_back(target: str, connector, request_id: str) -> str:
    """The request's run and how many entries each part of the app wrote."""
    with connector.connection() as conn:
        if target == "postgres":
            run = conn.execute("SELECT run_type, status, metadata_json->>'total_cost_usd' FROM logs.log_run "
                               "WHERE run_id = %s", (request_id,)).fetchone()
            loggers = conn.execute("SELECT logger_name, count(*) FROM logs.log_entry WHERE created_at > now() - "
                                   "interval '1 minute' GROUP BY 1 ORDER BY 1").fetchall()
        else:
            run = conn.query(f"SELECT run_type, status, JSONExtractFloat(metadata_json, 'total_cost_usd') "
                             f"FROM logs.log_run FINAL WHERE run_id = '{request_id}'").result_rows[0]
            loggers = conn.query("SELECT logger_name, count() FROM logs.log_entry WHERE created_at > now() - "
                                 "INTERVAL 1 MINUTE GROUP BY 1 ORDER BY 1").result_rows
    return (f"run {run[0]} {run[1]}, total_cost_usd {run[2]}; entries: "
            + ", ".join(f"{name} {n}" for name, n in loggers))


def main() -> None:
    targets = sys.argv[1:] or list(TARGETS)
    if unknown := set(targets) - set(TARGETS):
        raise SystemExit(f"unknown target(s) {sorted(unknown)}; choose from {list(TARGETS)}")
    if not load_dotenv(HERE / ".env"):
        raise SystemExit(f"no {HERE / '.env'}: copy .env.example to .env and fill it in")
    manager = ConnectorManager.from_yaml(HERE / "connections.yaml")  # app startup, once
    try:
        for target in targets:
            connector = manager.get(TARGETS[target])
            root = AuditLogger(target_type=target, connection=connector)   # no tenant: Telemetry binds one per client
            telemetry = Telemetry(
                root,
                resolve_tenant=chatbot.CLIENTS.get,       # client name -> (tenant_id, environment_id)
                resolve_user=chatbot.USERS.get,           # username -> app.tenant_user id (optional)
                system_tenant=chatbot.SYSTEM,             # for eval runs and plain logging
                cost_fn=chatbot.calc_cost_split,
                model_fn={"anthropic": "claude-sonnet-4"}.get,
                environment="development",
                prompt_version="v42",
            )
            try:
                request_id = chatbot.one_chat_request(telemetry)   # what /api/chat does today, call for call
                chatbot.the_rest(telemetry)
                telemetry.flush()
            finally:
                root.close()
            print(f"{target:<10} request {request_id}: {read_back(target, connector, request_id)}")
    finally:
        manager.close()                                             # app shutdown


if __name__ == "__main__":
    main()

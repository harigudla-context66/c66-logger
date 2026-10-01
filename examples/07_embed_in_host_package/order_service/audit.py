"""How a host package wires c66_logger in: one small factory, no credentials of its own."""

from __future__ import annotations

from typing import Any, Mapping

from c66_logger import AuditLogger


def build_audit_logger(tenant_id: str, environment_id: str, log_target: Mapping[str, Any]) -> AuditLogger:
    """
    ``log_target`` comes from whoever initializes order_service, e.g.
    {"target_type": "postgres", "connection": {"dsn": "..."}, "use_case_id": "..."}
    """
    return AuditLogger(
        tenant_id=tenant_id,
        environment_id=environment_id,
        target_type=log_target["target_type"],
        connection=log_target.get("connection", {}),
        use_case_id=log_target.get("use_case_id"),
        mode=log_target.get("mode", "buffered"),
        logger_name="order_service",
    )

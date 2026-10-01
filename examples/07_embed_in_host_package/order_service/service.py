from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from c66_logger import AuditLogger

from .audit import build_audit_logger


class PaymentDeclined(Exception):
    pass


class OrderService:
    """
    Business logic that writes logs as a side effect. Two ways in:
      * tenant + environment + log target -> it builds (and owns) the logger
      * or inject an AuditLogger you already have (tests do this)
    """

    def __init__(
        self,
        tenant_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        log_target: Optional[Mapping[str, Any]] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        if (log_target is None) == (audit_logger is None):
            raise ValueError("pass exactly one of log_target or audit_logger")
        self._owns_audit = audit_logger is None
        self.audit = audit_logger or build_audit_logger(tenant_id, environment_id, log_target)
        self._orders: Dict[int, Dict[str, Any]] = {}

    def create_order(self, order_id: int, amount: float, customer: str) -> None:
        self._orders[order_id] = {"amount": amount, "customer": customer, "status": "new"}
        self.audit.info({"message": "order created", "order_id": order_id, "amount": amount, "customer": customer})

    def pay(self, order_id: int, card_ok: bool) -> None:
        if not card_ok:
            raise PaymentDeclined(f"card declined for order {order_id}")
        self._orders[order_id]["status"] = "paid"
        self.audit.info({"message": "order paid", "order_id": order_id})

    def process_batch(self, orders: list, correlation_id: Optional[str] = None) -> int:
        """One logs.log_run per batch; each order's entries carry its run_id."""
        paid = 0
        with self.audit.run("order_batch", metadata={"orders": len(orders)}), \
                self.audit.context(correlation_id=correlation_id):
            for order in orders:
                self.create_order(order["order_id"], order["amount"], order["customer"])
                try:
                    self.pay(order["order_id"], order["card_ok"])
                    paid += 1
                except PaymentDeclined:
                    self.audit.exception({"message": "payment failed", "order_id": order["order_id"]})
        return paid

    def import_csv(self, csv_text: str) -> int:
        """Rows arriving as CSV are logged as-is: one log_entry per row."""
        return self.audit.info(csv_text, message="order imported")

    def close(self) -> None:
        if self._owns_audit:
            self.audit.close()

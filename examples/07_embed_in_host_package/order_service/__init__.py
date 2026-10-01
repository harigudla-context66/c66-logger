"""
order_service — a stand-in for any package that embeds c66_logger.

It never hardcodes where its logs go. Whoever initializes it passes the
tenant, the environment and the log target (see ../run.py).
"""

from .service import OrderService

__all__ = ["OrderService"]

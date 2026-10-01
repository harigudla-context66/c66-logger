"""
Sink registry: maps a target_type string to the class that writes there.

Built-in targets are imported lazily, so a host package that only logs to
MongoDB never needs SQLAlchemy installed, and importing c66_logger pulls in
no database drivers at all.
"""

from __future__ import annotations

import importlib
import threading
from typing import Any, Callable, Dict, List, Union

from ..exceptions import UnknownTargetError
from .base import BaseSink

SinkFactory = Callable[..., BaseSink]

# target_type -> "module:ClassName" (lazy) or a callable registered at runtime
_BUILTINS: Dict[str, str] = {
    "postgres": "c66_logger.sinks.postgres:PostgresSink",
    "mongodb": "c66_logger.sinks.mongodb:MongoSink",
    "clickhouse": "c66_logger.sinks.clickhouse:ClickHouseSink",
    "memory": "c66_logger.sinks.memory:MemorySink",
}
_ALIASES: Dict[str, str] = {"postgresql": "postgres", "pg": "postgres", "mongo": "mongodb", "ch": "clickhouse"}

_registry: Dict[str, Union[str, SinkFactory]] = dict(_BUILTINS)
_lock = threading.Lock()


def register_sink(target_type: str, factory: SinkFactory, *, replace: bool = False) -> None:
    """
    Make a new target type available to ``AuditLogger(target_type=...)``.
    ``factory`` is usually a BaseSink subclass; it's called with the
    caller's ``connection`` dict as keyword arguments.
    """
    key = target_type.strip().lower()
    with _lock:
        if key in _registry and not replace:
            raise ValueError(f"target_type {key!r} is already registered (pass replace=True)")
        _registry[key] = factory


def available_targets() -> List[str]:
    with _lock:
        return sorted(_registry)


def create_sink(target_type: str, **connection: Any) -> BaseSink:
    key = target_type.strip().lower()
    key = _ALIASES.get(key, key)
    with _lock:
        entry = _registry.get(key)
    if entry is None:
        raise UnknownTargetError(target_type, available_targets())
    factory = _load(entry) if isinstance(entry, str) else entry
    return factory(**connection)


def _load(path: str) -> SinkFactory:
    module_name, _, attr = path.partition(":")
    return getattr(importlib.import_module(module_name), attr)


__all__ = ["BaseSink", "register_sink", "create_sink", "available_targets"]

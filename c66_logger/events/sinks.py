"""
Where EventLogger sends events, and the registry of event targets.

Built-in targets (imported lazily, so no driver is loaded until used):

    kafka       KafkaEventSink       production: publish to Kafka; ClickHouse consumes the topics
    clickhouse  ClickHouseEventSink  write straight to logs.events (development, small installs)
    memory      MemoryEventSink      unit tests of host packages
"""

from __future__ import annotations

import abc
import importlib
import threading
from typing import Any, Callable, ClassVar, Dict, List, Sequence, Union

from ..exceptions import UnknownTargetError
from .model import Event


class BaseEventSink(abc.ABC):
    """
    A destination for events. Implement this to add a target.

    * ``open()``         called once before any write: connect, check topics / tables
                         exist. Raise here to fail fast on bad settings.
    * ``write_batch()``  deliver events. Raise on a transient failure and the writer
                         retries the batch, so delivery is at-least-once: make the
                         target tolerate repeats of ``event.event_id``. Raise
                         RejectedRecordsError for events that can never be
                         delivered; those are dropped, not retried. Called from one
                         background thread (or the caller's thread in sync mode).
    * ``close()``        release what the sink created itself — never a producer or
                         client the caller passed in.
    """

    target_type: ClassVar[str] = ""

    def open(self) -> None:
        """Optional setup; default does nothing."""

    @abc.abstractmethod
    def write_batch(self, events: Sequence[Event]) -> None:
        ...

    def close(self) -> None:
        """Optional teardown; default does nothing."""


class MemoryEventSink(BaseEventSink):
    """Keeps events in a list. For unit tests: ``sink.events``, ``sink.names()``."""

    target_type = "memory"

    def __init__(self, **_ignored: Any):
        self.events: List[Event] = []
        self._lock = threading.Lock()

    def write_batch(self, events: Sequence[Event]) -> None:
        with self._lock:
            self.events.extend(events)

    def names(self) -> List[str]:
        with self._lock:
            return [e.event_name for e in self.events]

    def clear(self) -> None:
        with self._lock:
            self.events.clear()


EventSinkFactory = Callable[..., BaseEventSink]

_BUILTINS: Dict[str, str] = {
    "kafka": "c66_logger.events.kafka:KafkaEventSink",
    "clickhouse": "c66_logger.events.clickhouse:ClickHouseEventSink",
    "memory": "c66_logger.events.sinks:MemoryEventSink",
}
_ALIASES: Dict[str, str] = {"ch": "clickhouse"}
_registry: Dict[str, Union[str, EventSinkFactory]] = dict(_BUILTINS)
_lock = threading.Lock()


def register_event_sink(target_type: str, factory: EventSinkFactory, *, replace: bool = False) -> None:
    """Make a new target available to ``EventLogger(target_type=...)``."""
    key = target_type.strip().lower()
    with _lock:
        if key in _registry and not replace:
            raise ValueError(f"event target_type {key!r} is already registered (pass replace=True)")
        _registry[key] = factory


def available_event_targets() -> List[str]:
    with _lock:
        return sorted(_registry)


def create_event_sink(target_type: str, **options: Any) -> BaseEventSink:
    key = target_type.strip().lower()
    key = _ALIASES.get(key, key)
    with _lock:
        entry = _registry.get(key)
    if entry is None:
        raise UnknownTargetError(target_type, available_event_targets())
    if isinstance(entry, str):
        module_name, _, attr = entry.partition(":")
        entry = getattr(importlib.import_module(module_name), attr)
    return entry(**options)

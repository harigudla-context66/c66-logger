"""
Generic events for telemetry, observability and audit, shaped like
OpenTelemetry log records, sent through Kafka to ClickHouse.

    from c66_logger import EventLogger

See docs/events.md.
"""

from .logger import EventLogger, Span, SpanContext, continue_trace, current_span
from .model import CATEGORIES, CRITICALITIES, EVENT_COLUMNS, SEVERITY_NUMBERS, Event, flatten_attributes
from .policy import EventPolicy
from .sinks import (BaseEventSink, MemoryEventSink, available_event_targets, create_event_sink,
                    register_event_sink)

__all__ = [
    "EventLogger",
    "Event",
    "EventPolicy",
    "Span",
    "SpanContext",
    "continue_trace",
    "current_span",
    "CATEGORIES",
    "CRITICALITIES",
    "EVENT_COLUMNS",
    "SEVERITY_NUMBERS",
    "flatten_attributes",
    "BaseEventSink",
    "MemoryEventSink",
    "register_event_sink",
    "create_event_sink",
    "available_event_targets",
]

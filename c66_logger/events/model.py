"""
The event: one generic record for everything the platform logs.

* telemetry      what users do in the UI (page viewed, button clicked, filter applied)
* observability  what the system does (pipeline created, run started / failed, config loaded)
* audit          who changed what (pipeline config updated by user X)

The fields follow the OpenTelemetry Logs data model — Timestamp,
ObservedTimestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber,
Body, Resource, InstrumentationScope, Attributes, EventName — plus the c66
fields every query filters on: EventId, TenantId, EnvironmentId, Category,
Criticality and RetentionDays.

Everything that isn't one of those fields is a key/value attribute
(``LogAttributes``); adding a new kind of data never needs a new column.
Attribute keys follow OpenTelemetry semantic conventions where one exists
(``exception.type``, ``code.function.name``, ``user.id``, ...).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import re
from collections.abc import Mapping
from typing import Any, Dict, Optional

from ..record import new_uuid, utcnow

CATEGORIES = ("telemetry", "observability", "audit")
CRITICALITIES = ("P1", "P2", "P3", "P4")
# OpenTelemetry SeverityNumber for each level (DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21)
SEVERITY_NUMBERS: Dict[str, int] = {"DEBUG": 5, "INFO": 9, "WARNING": 13, "ERROR": 17, "CRITICAL": 21}

# Column names as in the OpenTelemetry ClickHouse exporter's otel_logs table, plus the c66 columns.
# The same names are the keys of the JSON message sent to Kafka.
EVENT_COLUMNS = (
    "EventId", "Timestamp", "ObservedTimestamp", "TenantId", "EnvironmentId", "Category",
    "EventName", "SeverityText", "SeverityNumber", "Criticality", "RetentionDays", "ServiceName",
    "Body", "TraceId", "SpanId", "TraceFlags", "ScopeName", "ScopeVersion",
    "ResourceAttributes", "LogAttributes",
)

EVENT_NAME = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$")
MAX_EVENT_NAME = 200


@dataclasses.dataclass(frozen=True)
class Event:
    """One event. Build it with EventLogger; sinks turn it into a row or a Kafka message."""

    event_name: str
    category: str
    tenant_id: str
    environment_id: str
    service_name: str
    severity_text: str
    criticality: str
    retention_days: int
    body: str = ""
    attributes: Dict[str, str] = dataclasses.field(default_factory=dict)
    resource_attributes: Dict[str, str] = dataclasses.field(default_factory=dict)
    trace_id: str = ""
    span_id: str = ""
    trace_flags: int = 0
    scope_name: str = "c66_logger"
    scope_version: str = ""
    timestamp: dt.datetime = dataclasses.field(default_factory=utcnow)
    observed_timestamp: dt.datetime = dataclasses.field(default_factory=utcnow)
    event_id: str = dataclasses.field(default_factory=new_uuid)

    @property
    def severity_number(self) -> int:
        return SEVERITY_NUMBERS[self.severity_text]

    def as_row(self) -> Dict[str, Any]:
        """Column -> value, using the ClickHouse / Kafka column names (EVENT_COLUMNS)."""
        return {
            "EventId": self.event_id,
            "Timestamp": self.timestamp,
            "ObservedTimestamp": self.observed_timestamp,
            "TenantId": self.tenant_id,
            "EnvironmentId": self.environment_id,
            "Category": self.category,
            "EventName": self.event_name,
            "SeverityText": self.severity_text,
            "SeverityNumber": self.severity_number,
            "Criticality": self.criticality,
            "RetentionDays": self.retention_days,
            "ServiceName": self.service_name,
            "Body": self.body,
            "TraceId": self.trace_id,
            "SpanId": self.span_id,
            "TraceFlags": self.trace_flags,
            "ScopeName": self.scope_name,
            "ScopeVersion": self.scope_version,
            "ResourceAttributes": dict(self.resource_attributes),
            "LogAttributes": dict(self.attributes),
        }

    def to_json(self) -> str:
        """The Kafka message: one JSON object, readable by ClickHouse's JSONEachRow format."""
        row = self.as_row()
        row["Timestamp"] = _ch_time(self.timestamp)
        row["ObservedTimestamp"] = _ch_time(self.observed_timestamp)
        return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def flatten_attributes(values: Optional[Mapping[str, Any]], prefix: str = "") -> Dict[str, str]:
    """
    Turn caller data into string key/value attributes:

    * nested mappings become dotted keys:  {"pipeline": {"id": 7}} -> {"pipeline.id": "7"}
    * None is left out; booleans become "true" / "false"
    * lists and other objects become JSON text
    """
    flat: Dict[str, str] = {}
    for key, value in (values or {}).items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"attribute keys must be non-empty strings, got {key!r}")
        name = f"{prefix}{key}"
        if value is None:
            continue
        if isinstance(value, Mapping):
            flat.update(flatten_attributes(value, f"{name}."))
        else:
            flat[name] = attribute_value(value)
    return flat


def attribute_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def _ch_time(value: dt.datetime) -> str:
    """UTC 'YYYY-MM-DD hh:mm:ss.ffffff': parsed by ClickHouse DateTime64 without extra settings."""
    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")

"""
EventLogger: the generic logger for telemetry, observability and audit events.

    events = EventLogger(
        service_name="pipeline-service",
        tenant_id=TENANT_ID, environment_id=ENVIRONMENT_ID,
        target_type="kafka", connection={"bootstrap_servers": "kafka:9092"},
        policy={"rules": {"pipeline.run.failed": "P1"}},
    )

    events.observe("pipeline.created", pipeline_id=pid, source="s3")         # observability
    events.audit("pipeline.config.updated", actor=user_id, field="schedule")  # audit
    events.track("ui.pipeline.opened", user_id=user_id, page="/pipelines")   # telemetry

    with events.span("pipeline.run", pipeline_id=pid) as run:    # pipeline.run.started ...
        events.observe("pipeline.step.completed", step="load", rows=1200)   # same TraceId / SpanId
                                                                 # ... .completed / .failed / .cancelled

Calls never block on the target: events are queued and a background thread
sends them in batches (``mode="sync"`` sends before returning). In asyncio
code use ``await events.aemit(...)``.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import logging
import os
import platform
import re
import secrets
import socket
import threading
import time
from collections.abc import Mapping
from typing import Any, Dict, Iterator, Optional, Union

from .. import _build
from ..exceptions import ConfigurationError, InvalidLogInputError
from ..record import new_uuid, utcnow
from ..writers import BufferedWriter, OnDrop, SyncWriter
from .model import CATEGORIES, EVENT_NAME, MAX_EVENT_NAME, Event, attribute_value, flatten_attributes
from .policy import EventPolicy
from .sinks import BaseEventSink, create_event_sink

_log = logging.getLogger("c66_logger")

# limits for events that come from outside the process (the UI): see track_client_event()
CLIENT_MAX_ATTRIBUTES = 64
CLIENT_MAX_VALUE_LENGTH = 4096
CLIENT_SEVERITIES = ("DEBUG", "INFO", "WARNING", "ERROR")
_TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


@dataclasses.dataclass(frozen=True)
class SpanContext:
    """W3C trace context: which trace and span an event belongs to."""

    trace_id: str
    span_id: str
    trace_flags: int = 1

    @property
    def traceparent(self) -> str:
        """The W3C ``traceparent`` header value, to pass the trace on (HTTP, Kafka headers)."""
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags:02x}"

    @classmethod
    def from_traceparent(cls, header: Optional[str]) -> Optional["SpanContext"]:
        match = _TRACEPARENT.match((header or "").strip().lower())
        if not match or match.group(1) == "ff" or set(match.group(2)) == {"0"} or set(match.group(3)) == {"0"}:
            return None
        return cls(match.group(2), match.group(3), int(match.group(4), 16))


# Shared by every EventLogger in the process, so a request's trace and context
# attributes reach every bound / tenant logger it touches. Per thread / asyncio task.
_SPAN: contextvars.ContextVar[Optional[SpanContext]] = contextvars.ContextVar("c66_event_span", default=None)
_ATTRIBUTES: contextvars.ContextVar[Dict[str, str]] = contextvars.ContextVar("c66_event_attributes", default={})


def current_span() -> Optional[SpanContext]:
    """The active trace: an EventLogger span, else an active OpenTelemetry span (if that SDK is used)."""
    return _SPAN.get() or _otel_span()


@contextlib.contextmanager
def continue_trace(traceparent: Optional[str]) -> Iterator[Optional[SpanContext]]:
    """
    Join a trace started elsewhere — e.g. the UI sends ``traceparent`` with a request:

        with continue_trace(request.headers.get("traceparent")):
            events.observe("pipeline.created", ...)     # same TraceId as the UI events

    An invalid or missing header starts nothing (events get a trace only inside a span).
    """
    context = SpanContext.from_traceparent(traceparent)
    if context is None:
        yield None
        return
    token = _SPAN.set(context)
    try:
        yield context
    finally:
        _SPAN.reset(token)


class Span:
    """A unit of work (a pipeline run, a request). Returned by ``EventLogger.span()``."""

    def __init__(self, name: str, context: SpanContext, parent_span_id: Optional[str]):
        self.name = name
        self.context = context
        self.parent_span_id = parent_span_id
        self.attributes: Dict[str, str] = {}
        self.status: Optional[str] = None  # completed / failed / cancelled once ended

    @property
    def trace_id(self) -> str:
        return self.context.trace_id

    @property
    def span_id(self) -> str:
        return self.context.span_id

    @property
    def traceparent(self) -> str:
        return self.context.traceparent

    def set_attribute(self, key: str, value: Any) -> None:
        """Add to the span's end event (e.g. rows processed, output location)."""
        self.attributes.update(flatten_attributes({key: value}))

    def set_attributes(self, **values: Any) -> None:
        self.attributes.update(flatten_attributes(values))


class EventLogger:
    """
    Sends events for one service, tenant and environment to one target
    (``kafka`` in production, ``clickhouse`` directly, ``memory`` in tests,
    or your own via register_event_sink()).

    A process serving many tenants creates one logger without tenant ids and
    calls ``bind(tenant_id=..., environment_id=...)`` per tenant; bound loggers
    share the target connection and background thread.
    """

    def __init__(
        self,
        service_name: str,
        tenant_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        *,
        target_type: Optional[str] = None,
        connection: Any = None,
        sink: Optional[BaseEventSink] = None,
        policy: Union[EventPolicy, Mapping[str, Any], None] = None,
        service_version: Optional[str] = None,
        resource_attributes: Optional[Mapping[str, Any]] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        capture_caller: bool = True,
        mode: str = "buffered",
        batch_size: int = 500,
        flush_interval: float = 1.0,
        max_queue_size: int = 100_000,
        max_retries: int = 5,
        retry_backoff: float = 0.5,
        on_drop: Optional[OnDrop] = None,
    ):
        if not service_name or not isinstance(service_name, str):
            raise ConfigurationError("service_name must be a non-empty string")
        if (sink is None) == (target_type is None):
            raise ConfigurationError("pass exactly one of target_type (+ connection) or sink")
        if sink is not None and connection:
            raise ConfigurationError("connection is only used with target_type, not with sink")
        if mode not in ("buffered", "sync"):
            raise ConfigurationError("mode must be 'buffered' or 'sync'")

        self.service_name = service_name
        self.tenant_id = _build.as_uuid(tenant_id, "tenant_id", ConfigurationError)
        self.environment_id = _build.as_uuid(environment_id, "environment_id", ConfigurationError)
        if bool(self.tenant_id) != bool(self.environment_id):
            raise ConfigurationError("pass both tenant_id and environment_id (UUIDs), or neither and use bind()")
        self.policy = policy if isinstance(policy, EventPolicy) else EventPolicy.from_dict(policy)
        self._attributes = _flatten(attributes, ConfigurationError)
        self._resource = _resource_attributes(service_name, service_version, resource_attributes)
        self._capture_caller = capture_caller
        self._mode = mode

        self._owns_sink = sink is None
        if sink is None:
            if connection is None or isinstance(connection, Mapping):
                options = dict(connection or {})
            else:  # a producer / client / connector the host already has
                options = {"connection": connection}
            if target_type.strip().lower() == "kafka":
                options.setdefault("alert_criticalities", tuple(self.policy.alert_criticalities))
            sink = create_event_sink(target_type, **options)
        self.sink: BaseEventSink = sink
        try:
            self.sink.open()  # fail fast: unreachable broker, missing topics or table
        except Exception:
            if self._owns_sink:
                self.sink.close()
            raise

        retry = dict(max_retries=max_retries, retry_backoff=retry_backoff, on_drop=on_drop)
        if mode == "sync":
            self._writer: Union[SyncWriter, BufferedWriter] = SyncWriter(self.sink, **retry)
        else:
            self._writer = BufferedWriter(self.sink, batch_size=batch_size, flush_interval=flush_interval,
                                          max_queue_size=max_queue_size, **retry)
        self._owns_writer = True
        self._closed = False
        self._close_lock = threading.Lock()

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "EventLogger":
        """Build from a plain dict (e.g. a YAML section): the constructor's arguments by name."""
        return cls(**dict(config))

    def bind(self, tenant_id: Optional[str] = None, environment_id: Optional[str] = None,
             *, service_name: Optional[str] = None, **attributes: Any) -> "EventLogger":
        """
        A logger for another tenant / environment / service, with extra default
        attributes, sharing this one's target and background thread. Closing a
        bound logger is a no-op; close the one you created.
        """
        child = object.__new__(EventLogger)
        child.__dict__.update(self.__dict__)
        child.tenant_id = _build.as_uuid(tenant_id, "tenant_id", ConfigurationError) or self.tenant_id
        child.environment_id = (_build.as_uuid(environment_id, "environment_id", ConfigurationError)
                                or self.environment_id)
        if service_name is not None:
            if not service_name:
                raise ConfigurationError("service_name must be a non-empty string")
            child.service_name = service_name
            child._resource = {**self._resource, "service.name": service_name}
        child._attributes = {**self._attributes, **_flatten(attributes, ConfigurationError)}
        child._owns_sink = False
        child._owns_writer = False
        child._closed = False
        child._close_lock = threading.Lock()
        return child

    # ---- events --------------------------------------------------------

    def emit(
        self,
        event_name: str,
        *,
        category: str = "observability",
        severity: str = "INFO",
        body: str = "",
        criticality: Optional[str] = None,
        timestamp: Any = None,
        exc_info: Any = None,
        attributes: Optional[Mapping[str, Any]] = None,
        **extra: Any,
    ) -> Event:
        """
        Send one event and return it.

        ``event_name``   dotted, lower case: ``pipeline.run.failed``, ``ui.button.clicked``
        ``category``     telemetry | observability | audit
        ``severity``     DEBUG | INFO | WARNING | ERROR | CRITICAL
        ``criticality``  P1..P4; normally left to the policy (rules, then severity)
        ``exc_info``     True inside ``except`` (or an exception): adds exception.* attributes
        ``**extra`` / ``attributes``  the key/value data; nested dicts become dotted keys.
                         Use ``attributes={...}`` for keys that clash with these
                         parameters or aren't valid Python names.

        Raises InvalidLogInputError for bad input, before anything is queued.
        Delivery failures never raise here (see on_drop).
        """
        event = self._build(event_name, category=category, severity=severity, body=body,
                            criticality=criticality, timestamp=timestamp, exc_info=exc_info,
                            attributes=attributes, extra=extra)
        self._writer.submit([event])
        return event

    def observe(self, event_name: str, **kwargs: Any) -> Event:
        """An observability event: something the system did."""
        return self.emit(event_name, category="observability", **kwargs)

    def track(self, event_name: str, **kwargs: Any) -> Event:
        """A telemetry event: something a user did (from server code; for the UI see track_client_event)."""
        return self.emit(event_name, category="telemetry", **kwargs)

    def audit(self, event_name: str, *, actor: str, **kwargs: Any) -> Event:
        """An audit event: who (``actor``, stored as ``user.id``) changed what."""
        if not actor or not str(actor).strip():
            raise InvalidLogInputError("audit events need an actor (the user or service that made the change)")
        attributes = {**dict(kwargs.pop("attributes", None) or {}), "user.id": str(actor)}
        return self.emit(event_name, category="audit", attributes=attributes, **kwargs)

    def exception(self, event_name: str, **kwargs: Any) -> Event:
        """An ERROR observability event with the exception being handled (call inside ``except``)."""
        kwargs.setdefault("severity", "ERROR")
        kwargs.setdefault("exc_info", True)
        kwargs.setdefault("category", "observability")
        return self.emit(event_name, **kwargs)

    async def aemit(self, event_name: str, **kwargs: Any) -> Event:
        """``emit()`` for asyncio code. Buffered mode never blocks, so this only hands off
        to a thread in sync mode (where emit() waits for the target)."""
        if self._mode == "sync":
            return await asyncio.to_thread(self.emit, event_name, **kwargs)
        return self.emit(event_name, **kwargs)

    def track_client_event(
        self,
        data: Mapping[str, Any],
        *,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Event:
        """
        Accept a telemetry event sent by the UI (untrusted) — call this from the
        HTTP endpoint the front end posts to, on a logger bound to the tenant /
        environment taken from the *authenticated session*, never from ``data``.

        ``data``: {"event_name": "ui.button.clicked", "timestamp": "...", "severity": "INFO",
                   "body": "...", "attributes": {...}, "traceparent": "00-..."}

        Category is always telemetry and criticality comes from the policy only.
        Attribute count and value length are capped; ``user_id`` / ``session_id``
        from the server override anything the client sent.
        """
        if not isinstance(data, Mapping):
            raise InvalidLogInputError("client event must be a JSON object")
        unknown = set(data) - {"event_name", "timestamp", "severity", "body", "attributes", "traceparent"}
        if unknown:
            raise InvalidLogInputError(f"client event has unknown field(s): {', '.join(sorted(unknown))}")
        severity = str(data.get("severity") or "INFO").upper()
        if severity not in CLIENT_SEVERITIES:
            raise InvalidLogInputError(f"client severity must be one of {', '.join(CLIENT_SEVERITIES)}")
        raw = data.get("attributes") or {}
        if not isinstance(raw, Mapping):
            raise InvalidLogInputError("client attributes must be an object")
        attributes = _flatten(raw, InvalidLogInputError)
        if len(attributes) > CLIENT_MAX_ATTRIBUTES:
            raise InvalidLogInputError(f"client events may carry at most {CLIENT_MAX_ATTRIBUTES} attributes")
        attributes = {k[:256]: v[:CLIENT_MAX_VALUE_LENGTH] for k, v in attributes.items()}
        attributes.update(flatten_attributes({"user.id": user_id, "session.id": session_id}))
        with continue_trace(data.get("traceparent")):
            event = self._build(
                str(data.get("event_name") or ""), category="telemetry", severity=severity,
                body=str(data.get("body") or "")[:CLIENT_MAX_VALUE_LENGTH], criticality=None,
                timestamp=data.get("timestamp"), exc_info=None, attributes=attributes, extra={},
                caller=False,
            )
        self._writer.submit([event])
        return event

    # ---- context and spans ---------------------------------------------

    @contextlib.contextmanager
    def context(self, **attributes: Any) -> Iterator[None]:
        """
        Add attributes to every event sent inside the block, from any EventLogger
        (this thread / asyncio task only):  ``with events.context(request_id=rid): ...``
        """
        token = _ATTRIBUTES.set({**_ATTRIBUTES.get(), **_flatten(attributes, InvalidLogInputError)})
        try:
            yield
        finally:
            _ATTRIBUTES.reset(token)

    @contextlib.contextmanager
    def span(self, name: str, *, category: str = "observability", **attributes: Any) -> Iterator[Span]:
        """
        A unit of work — replaces log_run for events:

            with events.span("pipeline.run", pipeline_id=pid) as run:
                ...

        sends ``<name>.started``, then one of
          * ``<name>.completed``  (INFO)    the block finished
          * ``<name>.failed``     (ERROR)   it raised an Exception (exception.* attributes added)
          * ``<name>.cancelled``  (WARNING) KeyboardInterrupt / task cancellation / SystemExit
        with ``duration_ms``. Events sent inside the block share its TraceId and
        SpanId; a span inside another span continues the same trace. The
        exception is always re-raised.
        """
        _check_event_name(name)
        parent = current_span()
        context = SpanContext(parent.trace_id if parent else secrets.token_hex(16), secrets.token_hex(8),
                              parent.trace_flags if parent else 1)
        span = Span(name, context, parent.span_id if parent else None)
        span_attrs = {**flatten_attributes(attributes), "span.name": name}
        if span.parent_span_id:
            span_attrs["span.parent_id"] = span.parent_span_id
        started = time.monotonic()
        token = _SPAN.set(context)
        try:
            with self.context(**span_attrs):
                self.emit(f"{name}.started", category=category)
                try:
                    yield span
                except Exception as exc:
                    self._end_span(span, started, category, "failed", "ERROR", exc)
                    raise
                except BaseException as exc:  # KeyboardInterrupt, asyncio.CancelledError, SystemExit
                    self._end_span(span, started, category, "cancelled", "WARNING", exc)
                    raise
                else:
                    self._end_span(span, started, category, "completed", "INFO", None)
        finally:
            _SPAN.reset(token)

    # ---- lifecycle -----------------------------------------------------

    def flush(self, timeout: Optional[float] = 10.0) -> bool:
        """Wait until every event sent so far is delivered (or dropped). False on timeout."""
        return self._writer.flush(timeout)

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Deliver what's queued, stop the background thread, release the target (if created here)."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        if not self._owns_writer:
            return
        self._writer.close(timeout)
        if self._owns_sink:
            self.sink.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "EventLogger":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return (f"EventLogger(service_name={self.service_name!r}, tenant_id={self.tenant_id!r}, "
                f"environment_id={self.environment_id!r}, target={self.sink.target_type!r}, closed={self._closed})")

    # ---- internals -----------------------------------------------------

    def _build(self, event_name: str, *, category: str, severity: str, body: Any, criticality: Optional[str],
               timestamp: Any, exc_info: Any, attributes: Optional[Mapping[str, Any]], extra: Mapping[str, Any],
               caller: Optional[bool] = None) -> Event:
        if not self.tenant_id:
            raise ConfigurationError(
                "this EventLogger has no tenant_id/environment_id; create one with "
                "events.bind(tenant_id=..., environment_id=...)"
            )
        _check_event_name(event_name)
        if category not in CATEGORIES:
            raise InvalidLogInputError(f"category must be one of {', '.join(CATEGORIES)}, got {category!r}")
        severity = _severity(severity)
        criticality = self.policy.criticality_for(event_name, severity, criticality)
        observed = utcnow()
        ts = (_build.as_timestamp(timestamp, "timestamp", InvalidLogInputError)
              if timestamp not in (None, "") else observed)

        merged: Dict[str, str] = dict(self._attributes)
        merged.update(_ATTRIBUTES.get())
        if caller if caller is not None else self._capture_caller:
            merged.update(_caller_attributes())
        merged.update(_exception_attributes(exc_info))
        merged.update(_flatten(extra, InvalidLogInputError))
        merged.update(_flatten(attributes, InvalidLogInputError))

        span = current_span()
        return Event(
            event_name=event_name,
            category=category,
            tenant_id=self.tenant_id,
            environment_id=self.environment_id,
            service_name=self.service_name,
            severity_text=severity,
            criticality=criticality,
            retention_days=self.policy.retention_for(category, severity, criticality),
            body="" if body is None else str(body),
            attributes=merged,
            resource_attributes=self._resource,
            trace_id=span.trace_id if span else "",
            span_id=span.span_id if span else "",
            trace_flags=span.trace_flags if span else 0,
            scope_version=_version(),
            timestamp=ts,
            observed_timestamp=observed,
            event_id=new_uuid(),
        )

    def _end_span(self, span: Span, started: float, category: str, status: str, severity: str,
                  exc: Optional[BaseException]) -> None:
        span.status = status
        attributes = {**span.attributes, "duration_ms": int(round((time.monotonic() - started) * 1000)),
                      "span.status": status}
        try:
            self.emit(f"{span.name}.{status}", category=category, severity=severity, exc_info=exc,
                      attributes=attributes)
        except Exception:  # never mask the caller's exception
            _log.exception("c66_logger: could not send end event of span %s", span.name)


def _check_event_name(name: Any) -> None:
    if not isinstance(name, str) or len(name) > MAX_EVENT_NAME or not EVENT_NAME.match(name):
        raise InvalidLogInputError(
            f"event_name must be dotted lower-case words, e.g. 'pipeline.run.failed' "
            f"(letters, digits, _; max {MAX_EVENT_NAME} chars), got {name!r}"
        )


def _severity(value: Any) -> str:
    text = str(value).strip().upper()
    return "CRITICAL" if text == "FATAL" else _build.as_level(text, InvalidLogInputError)


def _flatten(values: Optional[Mapping[str, Any]], error: type) -> Dict[str, str]:
    try:
        return flatten_attributes(values)
    except ValueError as exc:
        raise error(str(exc)) from None


def _caller_attributes() -> Dict[str, str]:
    """Where the event was sent from, with OpenTelemetry semantic-convention keys."""
    where = _build.find_caller()
    thread = threading.current_thread()
    attrs = {
        "code.file.path": where.get("pathname"),
        "code.function.name": where.get("func_name"),
        "code.line.number": where.get("line_no"),
        "thread.id": thread.ident,
        "thread.name": thread.name,
    }
    return {k: attribute_value(v) for k, v in attrs.items() if v is not None}


def _exception_attributes(exc_info: Any) -> Dict[str, str]:
    fields = _build.exception_fields(exc_info)
    if not fields:
        return {}
    attrs = {
        "exception.type": fields.get("exception_type"),
        "exception.message": fields.get("exception_message"),
        "exception.stacktrace": fields.get("exception_traceback"),
    }
    return {k: v for k, v in attrs.items() if v is not None}


def _resource_attributes(service_name: str, service_version: Optional[str],
                         extra: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    """Describes the process sending events (OpenTelemetry Resource), computed once."""
    attrs: Dict[str, Any] = {
        "service.name": service_name,
        "service.version": service_version,
        "host.name": socket.gethostname(),
        "process.pid": os.getpid(),
        "process.runtime.name": platform.python_implementation().lower(),
        "process.runtime.version": platform.python_version(),
        "telemetry.sdk.name": "c66_logger",
        "telemetry.sdk.language": "python",
        "telemetry.sdk.version": _version(),
    }
    resource = flatten_attributes(attrs)
    resource.update(_flatten(extra, ConfigurationError))
    return resource


def _version() -> str:
    from .. import __version__
    return __version__


_otel_trace: Any = None  # opentelemetry.trace once looked up; False if it isn't installed


def _otel_span() -> Optional[SpanContext]:
    """The active OpenTelemetry span, so services already traced with the OTel SDK share its ids."""
    global _otel_trace
    if _otel_trace is None:
        try:
            from opentelemetry import trace as otel_trace
            _otel_trace = otel_trace
        except ImportError:
            _otel_trace = False
    if not _otel_trace:
        return None
    context = _otel_trace.get_current_span().get_span_context()
    if not context.is_valid:
        return None
    return SpanContext(format(context.trace_id, "032x"), format(context.span_id, "016x"), int(context.trace_flags))

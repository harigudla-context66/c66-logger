"""Helpers that turn caller input into LogEntry fields (validation, caller and exception capture)."""

from __future__ import annotations

import contextlib
import datetime as dt
import multiprocessing
import os
import sys
import threading
import traceback
import uuid
from typing import Any, Dict, Mapping, Optional, Tuple, Type

from .record import LEVELS, MAX_LENGTHS

_PKG_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep
_SKIP_FILES = {os.path.abspath(contextlib.__file__)}
_INT8_MAX = 2**63 - 1


def as_uuid(value: Any, field: str, error: Type[Exception]) -> Optional[str]:
    """Validate a UUID column value; returns the canonical string (or None)."""
    if value is None or value == "":
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    try:
        return str(uuid.UUID(str(value).strip()))
    except ValueError:
        raise error(f"{field} must be a UUID, got {value!r}") from None


def as_level(value: Any, error: Type[Exception]) -> str:
    level = str(value).strip().upper()
    if level == "WARN":
        level = "WARNING"
    if level not in LEVELS:
        raise error(f"level must be one of {', '.join(LEVELS)}, got {value!r}")
    return level


def level_from_levelno(levelno: int) -> str:
    """Map any logging levelno (custom ones too) onto the five levels the table allows."""
    for name in ("CRITICAL", "ERROR", "WARNING", "INFO"):
        if levelno >= LEVELS[name]:
            return name
    return "DEBUG"


def as_timestamp(value: Any, field: str, error: Type[Exception]) -> dt.datetime:
    if isinstance(value, dt.datetime):
        ts = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = dt.datetime.fromtimestamp(value, dt.timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            ts = dt.datetime.fromisoformat(text)
        except ValueError:
            raise error(f"{field} must be an ISO-8601 timestamp, got {value!r}") from None
    else:
        raise error(f"{field} must be a datetime, ISO string or epoch seconds, got {type(value).__name__}")
    return ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)  # naive -> UTC


def clip(field: str, value: Optional[str]) -> Optional[str]:
    limit = MAX_LENGTHS.get(field)
    if value is None or limit is None or len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def find_caller() -> Dict[str, Any]:
    """The first stack frame outside c66_logger (and contextlib): where log() was called."""
    frame = sys._getframe(1)
    while frame is not None:
        filename = os.path.abspath(frame.f_code.co_filename)
        if not filename.startswith(_PKG_DIR) and filename not in _SKIP_FILES:
            return location(filename, frame.f_code.co_name, frame.f_lineno)
        frame = frame.f_back
    return {}


def location(pathname: Optional[str], func_name: Optional[str], line_no: Optional[int]) -> Dict[str, Any]:
    module = os.path.splitext(os.path.basename(pathname))[0] if pathname else None
    return {
        "pathname": clip("pathname", pathname),
        "module": clip("module", module),
        "func_name": clip("func_name", func_name),
        "line_no": line_no,
    }


def process_and_thread() -> Dict[str, Any]:
    thread_id = threading.get_ident()
    return {
        "process_id": os.getpid(),
        "process_name": clip("process_name", multiprocessing.current_process().name),
        "thread_id": thread_id if thread_id <= _INT8_MAX else None,
        "thread_name": clip("thread_name", threading.current_thread().name),
    }


def exception_fields(exc_info: Any) -> Dict[str, Any]:
    """exc_info as accepted by logging: True (current exception), an exception, or a (type, value, tb) tuple."""
    if not exc_info:
        return {}
    if exc_info is True:
        exc_info = sys.exc_info()
    elif isinstance(exc_info, BaseException):
        exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
    exc_type, exc_value, tb = exc_info
    if exc_type is None:
        return {}
    qualname = exc_type.__qualname__
    if exc_type.__module__ not in ("builtins", "__main__"):
        qualname = f"{exc_type.__module__}.{qualname}"
    return {
        "exception_type": clip("exception_type", qualname),
        "exception_message": str(exc_value) if exc_value is not None else None,
        "exception_traceback": "".join(traceback.format_exception(exc_type, exc_value, tb)).rstrip(),
    }


def raise_site(exc: BaseException) -> Dict[str, Any]:
    """Location of the innermost frame of an exception's traceback."""
    tb = exc.__traceback__
    if tb is None:
        return {}
    while tb.tb_next is not None:
        tb = tb.tb_next
    code = tb.tb_frame.f_code
    return location(os.path.abspath(code.co_filename), code.co_name, tb.tb_lineno)


def merge_ids(*layers: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Later layers win; None values never override."""
    merged: Dict[str, Any] = {}
    for layer in layers:
        if layer:
            merged.update({k: v for k, v in layer.items() if v is not None})
    return merged


def split_known(item: Dict[str, Any], keys: Tuple[str, ...]) -> Dict[str, Any]:
    return {k: item.pop(k) for k in keys if k in item}

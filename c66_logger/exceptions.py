"""Exceptions raised by c66_logger. All inherit from C66LoggerError."""

from __future__ import annotations

from typing import Any, List, Tuple


class C66LoggerError(Exception):
    """Base class for every error this package raises."""


class ConfigurationError(C66LoggerError, ValueError):
    """The logger or a sink was initialized with missing or conflicting options."""


class UnknownTargetError(ConfigurationError):
    """No sink is registered for the requested target type."""

    def __init__(self, target_type: str, available: List[str]):
        super().__init__(
            f"Unknown target_type {target_type!r}. Available: {', '.join(available)}. "
            "Register your own with c66_logger.register_sink()."
        )
        self.target_type = target_type
        self.available = available


class MissingDependencyError(C66LoggerError, ImportError):
    """The driver for a target isn't installed (they are optional extras)."""

    def __init__(self, target_type: str, extra: str):
        super().__init__(
            f"The {target_type!r} target needs extra dependencies: "
            f"pip install 'c66-logger[{extra}]'"
        )
        self.target_type = target_type
        self.extra = extra


class InvalidLogInputError(C66LoggerError, ValueError):
    """Items passed to log() couldn't be turned into log entries."""


class RejectedRecordsError(C66LoggerError):
    """
    Raised by a sink when specific records can never be written — a foreign
    key, CHECK or NOT NULL violation. Retrying can't fix these, so the writer
    drops exactly these records (via on_drop) and does not retry them; the
    rest of the batch has already been written.
    """

    def __init__(self, rejected: List[Tuple[Any, str]], written: int = 0):
        super().__init__(f"{len(rejected)} record(s) rejected by the target, {written} written")
        self.rejected = rejected
        self.written = written


class RunWriteError(C66LoggerError):
    """A log_run row couldn't be inserted or updated after all retries."""

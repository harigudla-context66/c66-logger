"""
Criticality and retention: how important an event is, and how long it is kept.

Severity (DEBUG..CRITICAL) says what kind of message it is. Criticality
(P1..P4) says how much it matters to the business, and decides:

* routing  — P1 events are also sent to the alerts topic
* retention — how many days ClickHouse keeps the row (stored per row as
  RetentionDays; the table's TTL reads it, so changing the policy needs no
  DDL change)

Criticality is decided in this order:
  1. ``criticality=`` passed to the call
  2. the first rule whose event-name pattern matches (``fnmatch`` globs)
  3. the default for the event's severity

Defaults (proposed, to be confirmed with the architect; all are configurable):

    severity -> criticality   CRITICAL P1, ERROR P2, WARNING P3, INFO P4, DEBUG P4
    criticality -> days       P1 365, P2 180, P3 90, P4 30
    minimum days by category  audit 365, telemetry 90, observability 0
    DEBUG events              7 days (audit excepted)
    alerts                    P1
"""

from __future__ import annotations

import dataclasses
import fnmatch
from collections.abc import Mapping
from typing import Any, Dict, FrozenSet, Optional, Tuple

from ..exceptions import ConfigurationError, InvalidLogInputError
from .model import CATEGORIES, CRITICALITIES, SEVERITY_NUMBERS

DEFAULT_SEVERITY_CRITICALITY = {"CRITICAL": "P1", "ERROR": "P2", "WARNING": "P3", "INFO": "P4", "DEBUG": "P4"}
DEFAULT_RETENTION_DAYS = {"P1": 365, "P2": 180, "P3": 90, "P4": 30}
DEFAULT_CATEGORY_MIN_RETENTION_DAYS = {"audit": 365, "telemetry": 90, "observability": 0}
DEFAULT_DEBUG_RETENTION_DAYS = 7
MAX_RETENTION_DAYS = 65535  # UInt16 column


@dataclasses.dataclass(frozen=True)
class EventPolicy:
    """
    Decides criticality and retention for each event. Build from config with
    ``EventPolicy.from_dict()``:

        EventPolicy.from_dict({
            "rules": {"pipeline.run.failed": "P1", "pipeline.*.error": "P2", "ui.*": "P4"},
            "retention_days": {"P1": 730},          # merged over the defaults
        })
    """

    rules: Tuple[Tuple[str, str], ...] = ()
    severity_criticality: Mapping[str, str] = dataclasses.field(
        default_factory=lambda: dict(DEFAULT_SEVERITY_CRITICALITY))
    retention_days: Mapping[str, int] = dataclasses.field(default_factory=lambda: dict(DEFAULT_RETENTION_DAYS))
    category_min_retention_days: Mapping[str, int] = dataclasses.field(
        default_factory=lambda: dict(DEFAULT_CATEGORY_MIN_RETENTION_DAYS))
    debug_retention_days: int = DEFAULT_DEBUG_RETENTION_DAYS
    alert_criticalities: FrozenSet[str] = frozenset({"P1"})

    def __post_init__(self) -> None:
        rules = []
        for pattern, criticality in self.rules:
            if not isinstance(pattern, str) or not pattern:
                raise ConfigurationError(f"rule patterns must be non-empty strings, got {pattern!r}")
            rules.append((pattern, _check_criticality(criticality, f"rule {pattern!r}")))
        missing = set(SEVERITY_NUMBERS) - set(self.severity_criticality)
        if missing:
            raise ConfigurationError(f"severity_criticality is missing {', '.join(sorted(missing))}")
        # frozen dataclass: store the normalized (upper-case) values
        object.__setattr__(self, "rules", tuple(rules))
        object.__setattr__(self, "severity_criticality", {
            severity: _check_criticality(criticality, f"severity_criticality[{severity!r}]")
            for severity, criticality in self.severity_criticality.items()
        })
        object.__setattr__(self, "alert_criticalities", frozenset(
            _check_criticality(c, "alert_criticalities") for c in self.alert_criticalities))
        missing = set(CRITICALITIES) - set(self.retention_days)
        if missing:
            raise ConfigurationError(f"retention_days is missing {', '.join(sorted(missing))}")
        for name, days in {**self.retention_days, **self.category_min_retention_days,
                           "debug_retention_days": self.debug_retention_days}.items():
            if not isinstance(days, int) or isinstance(days, bool) or not 0 <= days <= MAX_RETENTION_DAYS:
                raise ConfigurationError(f"{name}: retention must be 0..{MAX_RETENTION_DAYS} days, got {days!r}")
        unknown = set(self.category_min_retention_days) - set(CATEGORIES)
        if unknown:
            raise ConfigurationError(f"category_min_retention_days: unknown categories {', '.join(sorted(unknown))}")

    @classmethod
    def from_dict(cls, config: Optional[Mapping[str, Any]]) -> "EventPolicy":
        """Keys: rules (mapping or list of [pattern, criticality]), severity_criticality,
        retention_days, category_min_retention_days, debug_retention_days,
        alert_criticalities. Mappings are merged over the defaults."""
        config = dict(config or {})
        unknown = set(config) - {f.name for f in dataclasses.fields(cls)}
        if unknown:
            raise ConfigurationError(f"unknown policy key(s): {', '.join(sorted(unknown))}")
        rules = config.get("rules") or ()
        rules = tuple(rules.items()) if isinstance(rules, Mapping) else tuple(tuple(r) for r in rules)
        return cls(
            rules=rules,
            severity_criticality={**DEFAULT_SEVERITY_CRITICALITY,
                                  **{k.upper(): v for k, v in (config.get("severity_criticality") or {}).items()}},
            retention_days={**DEFAULT_RETENTION_DAYS, **(config.get("retention_days") or {})},
            category_min_retention_days={**DEFAULT_CATEGORY_MIN_RETENTION_DAYS,
                                         **(config.get("category_min_retention_days") or {})},
            debug_retention_days=config.get("debug_retention_days", DEFAULT_DEBUG_RETENTION_DAYS),
            alert_criticalities=frozenset(config.get("alert_criticalities", ("P1",))),
        )

    def criticality_for(self, event_name: str, severity: str, explicit: Optional[str] = None) -> str:
        if explicit is not None:
            return _check_criticality(explicit, "criticality", InvalidLogInputError)
        for pattern, criticality in self.rules:
            if fnmatch.fnmatchcase(event_name, pattern):
                return criticality
        return self.severity_criticality[severity]

    def retention_for(self, category: str, severity: str, criticality: str) -> int:
        if severity == "DEBUG" and category != "audit":
            return self.debug_retention_days
        return max(self.retention_days[criticality], self.category_min_retention_days.get(category, 0))

    def is_alert(self, criticality: str) -> bool:
        return criticality in self.alert_criticalities


def _check_criticality(value: Any, where: str, error: type = ConfigurationError) -> str:
    text = str(value).strip().upper()
    if text not in CRITICALITIES:
        raise error(f"{where}: criticality must be one of {', '.join(CRITICALITIES)}, got {value!r}")
    return text

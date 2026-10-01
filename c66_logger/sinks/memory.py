from __future__ import annotations

import copy
import threading
from typing import Dict, List, Sequence

from ..record import LogEntry, LogRun
from .base import BaseSink


class MemorySink(BaseSink):
    """
    Keeps entries and runs in memory. Meant for the unit tests of packages that
    embed c66_logger:

        audit = AuditLogger(tenant_id=..., environment_id=..., target_type="memory", mode="sync")
        ...
        assert audit.sink.entries[0].message == "order created"
        assert audit.sink.runs[run.run_id].status == "Completed"
    """

    target_type = "memory"

    def __init__(self) -> None:
        self._entries: List[LogEntry] = []
        self._runs: Dict[str, LogRun] = {}
        self._lock = threading.Lock()

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        with self._lock:
            seen = {e.log_id for e in self._entries}
            self._entries.extend(e for e in entries if e.log_id not in seen)

    def write_run(self, run: LogRun) -> None:
        with self._lock:
            self._runs[run.run_id] = copy.deepcopy(run)  # snapshot, like a table row

    @property
    def entries(self) -> List[LogEntry]:
        with self._lock:
            return list(self._entries)

    @property
    def runs(self) -> Dict[str, LogRun]:
        with self._lock:
            return dict(self._runs)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._runs.clear()

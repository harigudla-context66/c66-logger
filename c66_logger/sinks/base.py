from __future__ import annotations

import abc
from typing import ClassVar, Sequence

from ..record import LogEntry, LogRun


class BaseSink(abc.ABC):
    """
    A destination for log entries and runs. Implement this to add a target.

    * ``open()``         called once by AuditLogger before any write: connect,
                         check the tables/collections exist. Raise here to fail
                         fast on bad connection details.
    * ``write_batch()``  persist LogEntry rows (logs.log_entry). Raise on a
                         transient failure and the writer retries the same
                         batch, so make it idempotent on ``entry.log_id``.
                         Raise RejectedRecordsError for records that can never
                         be written (FK / CHECK violations) — those are dropped,
                         not retried. Called from one background thread.
    * ``write_run()``    upsert one LogRun (logs.log_run) keyed on ``run.run_id``:
                         called with status 'Running' when a run starts and again
                         with its final state when it ends. Called from the
                         caller's thread, synchronously.
    * ``close()``        release connections the sink created itself — never a
                         client/engine the caller passed in.
    """

    target_type: ClassVar[str] = ""

    def open(self) -> None:
        """Optional setup; default does nothing."""

    @abc.abstractmethod
    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        ...

    @abc.abstractmethod
    def write_run(self, run: LogRun) -> None:
        ...

    def close(self) -> None:
        """Optional teardown; default does nothing."""

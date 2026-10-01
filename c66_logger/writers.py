"""
Writers sit between AuditLogger and a sink and decide *when* records are
written:

* BufferedWriter (default) — log() only enqueues; one daemon thread batches
  records and writes them on size or time, so the host app never waits on
  the database.
* SyncWriter — log() writes before returning. For short-lived processes
  (CLI tools, serverless handlers, tests) where a background thread is
  unwanted.

Both retry a failed batch with exponential backoff, then hand each record
to ``on_drop(record, reason)``.
"""

from __future__ import annotations

import atexit
import logging
import queue
import threading
import time
import weakref
from typing import Callable, List, Optional, Sequence

from .exceptions import RejectedRecordsError
from .record import LogEntry
from .sinks.base import BaseSink

_log = logging.getLogger("c66_logger")

OnDrop = Callable[[LogEntry, str], None]

_WAKE = object()  # queue sentinel used to interrupt the writer's idle wait


def _default_on_drop(record: LogEntry, reason: str) -> None:
    _log.warning("c66_logger dropped record %s: %s", record.log_id, reason)


class _RetryingWriter:
    def __init__(
        self,
        sink: BaseSink,
        *,
        max_retries: int,
        retry_backoff: float,
        on_drop: Optional[OnDrop],
    ):
        self._sink = sink
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._on_drop = on_drop or _default_on_drop

    def _write_with_retries(self, batch: Sequence[LogEntry]) -> bool:
        attempt = 0
        while True:
            try:
                self._sink.write_batch(batch)
                return True
            except RejectedRecordsError as exc:
                # Permanent per-record failures (FK / CHECK violations): the sink
                # already wrote the rest; drop exactly these, never retry them.
                _log.error("c66_logger: target rejected %d record(s)", len(exc.rejected))
                for record, reason in exc.rejected:
                    self._drop([record], reason)
                return False
            except Exception as exc:  # noqa: BLE001 - driver errors vary
                attempt += 1
                if attempt > self._max_retries:
                    _log.error(
                        "c66_logger: giving up on %d records after %d attempts: %r",
                        len(batch), attempt, exc,
                    )
                    self._drop(batch, repr(exc))
                    return False
                time.sleep(min(self._retry_backoff * (2 ** (attempt - 1)), 5.0))

    def _drop(self, batch: Sequence[LogEntry], reason: str) -> None:
        for record in batch:
            try:
                self._on_drop(record, reason)
            except Exception:  # a broken callback must not kill the writer
                _log.exception("c66_logger: on_drop callback raised")


class SyncWriter(_RetryingWriter):
    def __init__(self, sink: BaseSink, *, max_retries: int = 3, retry_backoff: float = 0.5,
                 on_drop: Optional[OnDrop] = None):
        super().__init__(sink, max_retries=max_retries, retry_backoff=retry_backoff, on_drop=on_drop)
        self._closed = False

    def submit(self, records: Sequence[LogEntry]) -> None:
        if self._closed:
            self._drop(records, "logger_closed")
            return
        self._write_with_retries(records)

    def flush(self, timeout: Optional[float] = None) -> bool:
        return True

    def close(self, timeout: Optional[float] = None) -> None:
        self._closed = True


class BufferedWriter(_RetryingWriter):
    def __init__(
        self,
        sink: BaseSink,
        *,
        batch_size: int = 500,
        flush_interval: float = 2.0,
        max_queue_size: int = 50_000,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
        on_drop: Optional[OnDrop] = None,
    ):
        super().__init__(sink, max_retries=max_retries, retry_backoff=retry_backoff, on_drop=on_drop)
        if batch_size < 1 or flush_interval <= 0:
            raise ValueError("batch_size must be >= 1 and flush_interval > 0")
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: "queue.Queue[LogEntry]" = queue.Queue(maxsize=max_queue_size)
        self._stop = threading.Event()
        self._flush_requested = threading.Event()
        self._thread = threading.Thread(target=self._run, name="c66-logger-writer", daemon=True)
        self._thread.start()
        _live_writers.add(self)

    def submit(self, records: Sequence[LogEntry]) -> None:
        if self._stop.is_set():
            self._drop(records, "logger_closed")
            return
        for record in records:
            try:
                self._queue.put_nowait(record)
            except queue.Full:
                self._drop([record], "queue_full")

    def flush(self, timeout: Optional[float] = 10.0) -> bool:
        """Block until everything submitted so far is written (or dropped)."""
        if not self._thread.is_alive():
            return self._queue.unfinished_tasks == 0
        self._flush_requested.set()
        self._wake()
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Stop accepting records, write what's queued, stop the thread."""
        if self._stop.is_set():
            return
        self._stop.set()
        self._wake()
        self._thread.join(timeout)
        _live_writers.discard(self)

    def _run(self) -> None:
        batch: List[LogEntry] = []
        deadline = 0.0
        while True:
            urgent = self._stop.is_set() or self._flush_requested.is_set()
            if urgent:
                timeout = 0.0
            elif batch:
                timeout = max(0.0, deadline - time.monotonic())
            else:
                timeout = self._flush_interval  # idle: just wait for work

            had_batch = bool(batch)
            try:
                record = self._take(timeout)
                if record is not None:
                    batch.append(record)
                while len(batch) < self._batch_size:  # grab whatever else is ready
                    record = self._take(0.0)
                    if record is not None:
                        batch.append(record)
            except queue.Empty:
                pass
            if batch and not had_batch:
                deadline = time.monotonic() + self._flush_interval

            if batch and (len(batch) >= self._batch_size or urgent or time.monotonic() >= deadline):
                self._write_with_retries(batch)
                for _ in batch:
                    self._queue.task_done()
                batch = []
                continue

            if not batch and urgent and self._queue.empty():
                self._flush_requested.clear()
                if self._stop.is_set():
                    return

    def _take(self, timeout: float) -> Optional[LogEntry]:
        item = self._queue.get(timeout=timeout) if timeout > 0 else self._queue.get_nowait()
        if item is _WAKE:
            self._queue.task_done()
            return None
        return item

    def _wake(self) -> None:
        """Interrupt an idle wait so flush()/close() act immediately."""
        try:
            self._queue.put_nowait(_WAKE)  # type: ignore[arg-type]
        except queue.Full:
            pass  # thread is busy anyway and will see the flag


# One process-wide exit hook (not one per logger) that drains whatever
# buffered loggers the host application never closed, so queued records
# aren't lost when the interpreter shuts down.
_live_writers: "weakref.WeakSet[BufferedWriter]" = weakref.WeakSet()


@atexit.register
def _close_live_writers() -> None:  # pragma: no cover - runs at interpreter exit
    for writer in list(_live_writers):
        writer.close(timeout=5.0)

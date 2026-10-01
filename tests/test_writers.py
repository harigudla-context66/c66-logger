import threading
import time

from c66_logger.writers import BufferedWriter, SyncWriter

from c66_logger.exceptions import RejectedRecordsError

from .conftest import make_entry


def fast(sink, **kw):
    kw.setdefault("retry_backoff", 0.01)
    return BufferedWriter(sink, **kw)


def test_flushes_when_batch_is_full(sink):
    w = fast(sink, batch_size=3, flush_interval=60)
    try:
        w.submit([make_entry(i=i) for i in range(3)])
        deadline = time.monotonic() + 2
        while not sink.batches and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(sink.entries) == 3
    finally:
        w.close()


def test_flushes_on_interval(sink):
    w = fast(sink, batch_size=1000, flush_interval=0.1)
    try:
        w.submit([make_entry()])
        deadline = time.monotonic() + 2
        while not sink.batches and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(sink.entries) == 1
    finally:
        w.close()


def test_flush_blocks_until_written(sink):
    w = fast(sink, batch_size=1000, flush_interval=60)
    try:
        w.submit([make_entry(i=i) for i in range(10)])
        assert w.flush(timeout=5) is True
        assert len(sink.entries) == 10
    finally:
        w.close()


def test_retries_then_succeeds(sink):
    sink.fail_next(2)
    w = fast(sink, max_retries=3)
    try:
        w.submit([make_entry()])
        assert w.flush(timeout=5)
        assert len(sink.entries) == 1
    finally:
        w.close()


def test_drops_after_max_retries(sink):
    sink.fail_next(100)
    dropped = []
    w = fast(sink, max_retries=1, on_drop=lambda r, reason: dropped.append(reason))
    try:
        w.submit([make_entry(), make_entry()])
        assert w.flush(timeout=5)
        assert len(dropped) == 2 and "simulated" in dropped[0]
        assert sink.entries == []
    finally:
        w.close()


def test_close_writes_what_is_queued(sink):
    w = fast(sink, batch_size=1000, flush_interval=60)
    w.submit([make_entry(), make_entry()])
    w.close()
    assert len(sink.entries) == 2


def test_submit_after_close_is_dropped_not_raised(sink):
    dropped = []
    w = fast(sink, on_drop=lambda r, reason: dropped.append(reason))
    w.close()
    w.submit([make_entry()])
    assert dropped == ["logger_closed"]


def test_queue_full_drops(sink):
    gate = threading.Event()
    original = sink.write_batch
    sink.write_batch = lambda recs: (gate.wait(5), original(recs))
    dropped = []
    w = fast(sink, batch_size=1, flush_interval=60, max_queue_size=1,
             on_drop=lambda r, reason: dropped.append(reason))
    try:
        w.submit([make_entry() for _ in range(20)])
        assert "queue_full" in dropped
    finally:
        gate.set()
        w.close()


def test_idle_writer_does_not_spin(sink):
    """Regression: v0.1 busy-looped once the queue sat empty past the interval."""
    w = fast(sink, flush_interval=0.05)
    try:
        start = time.process_time()
        time.sleep(0.5)
        assert time.process_time() - start < 0.2
    finally:
        w.close()


def test_close_and_flush_are_prompt_with_long_interval(sink):
    """close()/flush() must wake an idle writer, not wait out flush_interval."""
    w = fast(sink, flush_interval=60)
    w.submit([make_entry()])
    start = time.monotonic()
    assert w.flush(timeout=5)
    w.close()
    assert time.monotonic() - start < 1.0
    assert len(sink.entries) == 1


def test_sync_writer_writes_immediately(sink):
    w = SyncWriter(sink, retry_backoff=0.01)
    w.submit([make_entry()])
    assert len(sink.entries) == 1


def test_rejected_records_are_dropped_without_retry(sink):
    """A sink that rejects specific rows (FK violation) must not trigger retries."""
    good, bad = make_entry("good"), make_entry("bad")
    calls = []

    def write_batch(entries):
        calls.append(len(entries))
        sink.batches.append([good])
        raise RejectedRecordsError([(bad, "violates foreign key constraint")], written=1)

    sink.write_batch = write_batch
    dropped = []
    w = SyncWriter(sink, max_retries=5, retry_backoff=0.01, on_drop=lambda r, why: dropped.append((r, why)))
    w.submit([good, bad])
    assert calls == [2]  # no retries
    assert dropped == [(bad, "violates foreign key constraint")]
    assert sink.entries == [good]

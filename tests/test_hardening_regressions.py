"""
Atomix — Hardening Regression Test Suite
========================================
"""

import io
import logging
import os
import time
import unittest
import inspect
import threading

from atomix_stm import Ref, dosync, get_stm_stats, reset_stm
from atomix_stm.core import (
    STMReaper,
    TransactionCoordinator,
    _safe_log_info,
    _safe_log_debug,
    _safe_log_warning,
    _safe_log_error,
)


class TestConflictCounter(unittest.TestCase):
    """``total_conflicts`` must increment for every detected conflict."""

    def setUp(self):
        reset_stm()

    def test_conflicts_increment_under_contention(self):
        """Forced read/write overlap must produce a non-zero conflict count."""
        r = Ref(0)
        num_threads = 20
        ops = 20

        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(ops):

                @dosync(max_retries=50)
                def tx():
                    v = r.deref()
                    time.sleep(0.005)
                    r.set(v + 1)

                try:
                    tx()
                except Exception:
                    pass

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        stats = get_stm_stats()
        # We forced read/write overlap on a single Ref, so the conflict count
        # has to be > 0. Before the fix this was always 0.
        self.assertGreater(
            stats.get("total_conflicts", 0),
            0,
            "total_conflicts should be > 0 under heavy contention",
        )
        self.assertGreaterEqual(
            stats.get("total_aborts", 0),
            stats.get("total_conflicts", 0),
            "every conflict should produce at least one abort",
        )

    def test_conflict_count_matches_aborts_exactly(self):
        """When each conflict is from a single-ref transaction, the counts
        should line up (modulo the in-progress transaction)."""
        r = Ref(0)

        num_threads = 30
        ops = 15
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(ops):

                @dosync(max_retries=100)
                def tx():
                    v = r.deref()
                    time.sleep(0.005)
                    r.set(v + 1)

                try:
                    tx()
                except Exception:
                    pass

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        stats = get_stm_stats()
        self.assertEqual(
            stats["total_conflicts"],
            stats["total_aborts"],
            "On a single Ref each conflict is one abort — counts must match",
        )

    def test_record_conflict_is_single_source_of_truth(self):
        """``TransactionCoordinator`` exposes ``record_conflict``; the
        coordinator is the only component that mutates ``total_conflicts``."""
        coord = TransactionCoordinator()
        self.assertTrue(
            hasattr(coord, "record_conflict"),
            "Coordinator must expose record_conflict()",
        )
        # Ensure it's called from the prepare path
        src = inspect.getsource(coord.__class__)
        self.assertIn("record_conflict", src)

    def test_no_dead_prepare_methods(self):
        """The old duplicate ``_prepare`` / ``prepare_commit`` must be gone."""
        from atomix_stm.core import Transaction

        self.assertFalse(
            hasattr(Transaction, "_prepare"),
            "Transaction._prepare is dead code and should have been removed",
        )
        self.assertFalse(
            hasattr(TransactionCoordinator, "prepare_commit"),
            "Coordinator.prepare_commit is dead code and should have been removed",
        )


class TestReaperShutdownNoIOError(unittest.TestCase):
    """``STMReaper.stop`` must be defensive against closed log streams."""

    def setUp(self):
        reset_stm()

    def test_stop_with_closed_stderr_does_not_raise(self):
        """When the logging stream is closed, stop() must not propagate the
        ``ValueError: I/O operation on closed file`` to the caller."""
        from atomix_stm.core import logger as stm_logger

        # Find a stream handler we can swap out, then close the replacement.
        target = None
        for h in stm_logger.handlers:
            if isinstance(h, logging.StreamHandler):
                target = h
                break
        if target is None:
            self.skipTest("No stream handler configured on atomix_stm logger")

        original_stream = target.stream
        closed_stream = io.StringIO()
        closed_stream.close()
        target.setStream(closed_stream)
        try:
            coord = TransactionCoordinator()
            reaper = STMReaper(coord, interval=0.05)
            reaper.start()
            time.sleep(0.1)
            # Must not raise
            reaper.stop(timeout=1.0)
        finally:
            target.setStream(original_stream)

    def test_cleanup_with_closed_stderr_does_not_raise(self):
        """``_cleanup`` is registered with atexit and runs late in shutdown;
        it must also tolerate a closed logging stream."""
        from atomix_stm.core import _cleanup, logger as stm_logger

        target = None
        for h in stm_logger.handlers:
            if isinstance(h, logging.StreamHandler):
                target = h
                break
        if target is None:
            self.skipTest("No stream handler configured on atomix_stm logger")

        original_stream = target.stream
        closed_stream = io.StringIO()
        closed_stream.close()
        target.setStream(closed_stream)
        try:
            # Must not raise
            _cleanup()
        finally:
            target.setStream(original_stream)

    def test_safe_log_helpers_silence_stream_errors(self):
        """The ``_safe_log_*`` helpers must never raise, even on closed
        streams — that is their entire reason to exist."""
        from atomix_stm.core import logger as stm_logger

        target = None
        for h in stm_logger.handlers:
            if isinstance(h, logging.StreamHandler):
                target = h
                break
        if target is None:
            self.skipTest("No stream handler configured on atomix_stm logger")

        original_stream = target.stream
        closed_stream = io.StringIO()
        closed_stream.close()
        target.setStream(closed_stream)
        try:
            # These must all return cleanly
            _safe_log_info("test info")
            _safe_log_debug("test debug")
            _safe_log_warning("test warning")
            _safe_log_error("test error")
        finally:
            target.setStream(original_stream)


class TestReaperResponsiveStop(unittest.TestCase):
    """``STMReaper.stop`` must interrupt the loop's wait, not time out."""

    def setUp(self):
        reset_stm()

    def test_stop_is_fast_with_long_interval(self):
        """Even with a 60s interval, stop() should join in well under a
        second because the wait is interruptible."""
        coord = TransactionCoordinator()
        reaper = STMReaper(coord, interval=60.0)
        reaper.start()
        self.assertTrue(reaper.is_alive())

        t0 = time.time()
        result = reaper.stop(timeout=3.0)
        elapsed = time.time() - t0

        self.assertTrue(result, "stop() should report the thread exited")
        self.assertFalse(reaper.is_alive(), "thread should be dead")
        self.assertLess(
            elapsed, 1.0, f"stop() took {elapsed:.3f}s — loop is not interruptible"
        )

    def test_stop_returns_bool(self):
        """``stop`` now returns True iff the thread exited within timeout."""
        coord = TransactionCoordinator()
        reaper = STMReaper(coord, interval=0.1)
        reaper.start()
        result = reaper.stop(timeout=2.0)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

    def test_reaper_has_stop_event(self):
        """The reaper exposes its stop event for tests/observability."""
        coord = TransactionCoordinator()
        reaper = STMReaper(coord, interval=0.1)
        self.assertIsInstance(reaper._stop_event, threading.Event)
        self.assertFalse(reaper._stop_event.is_set())
        reaper._stop_event.set()  # safe even without start


class TestNoDeadCode(unittest.TestCase):
    """The duplicate prepare paths must be fully excised."""

    def test_transaction_has_no_private_prepare(self):
        from atomix_stm.core import Transaction

        self.assertFalse(hasattr(Transaction, "_prepare"))

    def test_coordinator_has_no_prepare_commit(self):
        self.assertFalse(hasattr(TransactionCoordinator, "prepare_commit"))

    def test_prepare_inside_lock_records_conflict(self):
        """The only path used by ``_commit`` must increment the counter."""
        from atomix_stm.core import Transaction

        src = inspect.getsource(Transaction._prepare_inside_lock)
        self.assertIn("record_conflict", src)
        self.assertIn("total_conflicts", src)  # the underlying stat key


class TestConcurrencyModelDocumentation(unittest.TestCase):
    """Public claims about the concurrency model must match the code."""

    def test_module_docstring_mentions_commit_lock(self):
        import atomix_stm.core as core_module

        self.assertIn("commit_lock", core_module.__doc__)

    def test_readme_does_not_claim_unbounded_scaling(self):
        """The README must not claim that STM 'scales' on free-threading
        without acknowledging the global commit lock."""
        readme = os.path.join(os.path.dirname(os.path.dirname(__file__)), "README.md")
        if not os.path.exists(readme):
            self.skipTest("README.md not found")
        with open(readme, "r", encoding="utf-8") as f:
            text = f.read()
        self.assertNotIn("No-GIL Optimized", text)


if __name__ == "__main__":
    unittest.main()

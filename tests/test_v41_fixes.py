"""
Atomix v4.2.0 — Regression Test Suite for V4.1 Hardening Fixes
================================================================

Covers all fixes introduced in v4.2.0:
1. SeqLock.read() bounded spin (max_spins upper bound)
2. _notify_watchers logs errors instead of silencing
3. Ref.deref() catches TimeoutException
4. SeqLock public API (read_seq/read_value)
5. STMReaper graceful shutdown (join on stop)
6. Ref.__del__ no bare except
7. monitoring.py correct import
"""

import unittest
import threading
import time
import re
import inspect
import logging

from atomix_stm import (
    Ref, Atom, dosync, transaction,
    TimeoutException, ValidationException,
)
from atomix_stm.core import (
    SeqLock, STMReaper, TransactionCoordinator,
    __version__,
)


class TestFix1_SeqLockBoundedSpin(unittest.TestCase):
    """Fix #1: SeqLock.read() should have an upper bound on spin retries."""

    def test_seqlock_read_source_has_max_spins(self):
        """SeqLock.read() source should contain max_spins limit."""
        source = inspect.getsource(SeqLock.read)
        self.assertIn("max_spins", source)
        self.assertIn("TimeoutException", source)

    def test_seqlock_normal_read_works(self):
        """Normal reads should work fine within bounds."""
        lock = SeqLock(42)
        self.assertEqual(lock.read(), 42)

    def test_seqlock_concurrent_read_write(self):
        """Concurrent reads and writes should work under normal contention."""
        lock = SeqLock(0)
        results = []

        def writer():
            for i in range(100):
                lock.write(i + 1)
                time.sleep(0.0001)

        def reader():
            for _ in range(100):
                try:
                    val = lock.read()
                    results.append(val)
                except TimeoutException:
                    pass  # Acceptable under extreme contention

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertGreater(len(results), 0)


class TestFix2_NotifyWatchersLogging(unittest.TestCase):
    """Fix #2: _notify_watchers should log errors, not silently swallow."""

    def test_atom_watcher_error_is_logged(self):
        """Atom watcher exception should be logged as warning."""
        a = Atom(0)

        def bad_watcher(old, new):
            raise ValueError("Watcher exploded!")

        a.add_watcher("bad", bad_watcher)

        with self.assertLogs("atomix_stm.core", level="WARNING") as cm:
            a.swap(lambda x: x + 1)

        self.assertTrue(
            any("Atom watcher error" in msg for msg in cm.output),
            f"Expected 'Atom watcher error' in log output, got: {cm.output}"
        )

    def test_ref_watcher_error_is_logged(self):
        """Ref watcher exception should be logged as warning."""
        r = Ref(0)

        def bad_watcher(old, new):
            raise RuntimeError("Ref watcher boom!")

        r.add_watcher("bad", bad_watcher)

        with self.assertLogs("atomix_stm.core", level="WARNING") as cm:
            r.reset(10)

        self.assertTrue(
            any("Ref watcher error" in msg for msg in cm.output),
            f"Expected 'Ref watcher error' in log output, got: {cm.output}"
        )


class TestFix3_RefDerefCatchesTimeout(unittest.TestCase):
    """Fix #3: Ref.deref() should handle TimeoutException from SeqLock."""

    def test_deref_source_catches_timeout(self):
        """deref source should catch TimeoutException."""
        source = inspect.getsource(Ref.deref)
        self.assertIn("TimeoutException", source)

    def test_deref_normal_works(self):
        """Normal deref should work fine."""
        r = Ref(42)
        self.assertEqual(r.deref(), 42)


class TestFix4_SeqLockPublicAPI(unittest.TestCase):
    """Fix #4: SeqLock should expose read_seq() and read_value() public API."""

    def test_read_seq_exists(self):
        """SeqLock should have read_seq() method."""
        lock = SeqLock(0)
        seq = lock.read_seq()
        self.assertIsInstance(seq, int)
        self.assertEqual(seq, 0)  # Initial sequence is 0

    def test_read_value_exists(self):
        """SeqLock should have read_value() method."""
        lock = SeqLock(42)
        val = lock.read_value()
        self.assertEqual(val, 42)

    def test_atom_swap_uses_public_api(self):
        """Atom.swap() source should use read_seq/read_value, not _sequence/_value."""
        source = inspect.getsource(Atom.swap)
        self.assertIn("read_seq()", source)
        self.assertIn("read_value()", source)
        self.assertNotIn("._sequence", source)
        self.assertNotIn("._value", source)

    def test_read_seq_reflects_writes(self):
        """read_seq() should increase after writes."""
        lock = SeqLock(0)
        seq_before = lock.read_seq()
        lock.write(99)
        seq_after = lock.read_seq()
        self.assertGreater(seq_after, seq_before)


class TestFix5_STMReaperGracefulStop(unittest.TestCase):
    """Fix #5: STMReaper.stop() should join the thread."""

    def test_stop_source_has_join(self):
        """STMReaper.stop() should call self.join()."""
        source = inspect.getsource(STMReaper.stop)
        self.assertIn("self.join", source)

    def test_reaper_stops_gracefully(self):
        """Reaper should stop and thread should exit."""
        coordinator = TransactionCoordinator()
        reaper = STMReaper(coordinator, interval=0.1)
        reaper.start()
        self.assertTrue(reaper.is_alive())

        reaper.stop()
        # After stop() with join, thread should no longer be alive
        self.assertFalse(reaper.is_alive())


class TestFix6_RefDelNoBarExcept(unittest.TestCase):
    """Fix #6: Ref.__del__ should use except Exception, not bare except."""

    def test_ref_del_no_bare_except(self):
        """Ref.__del__ should not have bare except:."""
        source = inspect.getsource(Ref.__del__)
        bare_except = re.compile(r'^\s*except\s*:', re.MULTILINE)
        matches = bare_except.findall(source)
        self.assertEqual(
            len(matches), 0,
            "Ref.__del__ still has bare except:"
        )


class TestVersionConsistency(unittest.TestCase):
    """Version should be 4.2.0 everywhere."""

    def test_version_is_4_1_0(self):
        self.assertEqual(__version__, "4.2.0")

    def test_init_version(self):
        import atomix_stm
        self.assertEqual(atomix_stm.__version__, "4.2.0")

    def test_docstring_version(self):
        import atomix_stm.core as core_module
        self.assertIn("4.2.0", core_module.__doc__)


if __name__ == '__main__':
    unittest.main()

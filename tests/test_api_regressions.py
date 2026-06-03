"""
Atomix — Regression Test Suite for API and Data Structures
==========================================================
"""

import unittest
import inspect
import threading
import os

from atomix_stm import (
    Ref,
    Atom,
    PersistentHashMap,
    QueueClosedException,
)
from atomix_stm.core import (
    SeqLock,
    __version__,
    _cleanup,
)


class TestCommitValueRace(unittest.TestCase):
    """old_value must be read INSIDE self._lock to avoid race."""

    def test_source_reads_old_value_inside_lock(self):
        """_commit_value source should have old_value = self._value AFTER 'with self._lock'."""
        source = inspect.getsource(Ref._commit_value)
        lines = source.split("\n")

        lock_line = None
        old_val_line = None
        for i, line in enumerate(lines):
            if "with self._lock" in line:
                lock_line = i
            if "old_value = self._value" in line:
                old_val_line = i

        self.assertIsNotNone(
            lock_line, "Could not find 'with self._lock' in _commit_value"
        )
        self.assertIsNotNone(
            old_val_line, "Could not find 'old_value = self._value' in _commit_value"
        )
        self.assertGreater(
            old_val_line,
            lock_line,
            f"old_value read (line {old_val_line}) should be AFTER lock acquisition (line {lock_line})",
        )

    def test_commit_value_basic_correctness(self):
        """Ref._commit_value should still work correctly."""
        r = Ref(10)
        r.reset(20)
        self.assertEqual(r.deref(), 20)

    def test_concurrent_commit_correctness(self):
        """Multiple threads resetting should produce consistent results."""
        r = Ref(0)
        num_threads = 10
        ops = 100

        def worker(val):
            for _ in range(ops):
                r.reset(val)

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        # Value should be one of the thread values
        final = r.deref()
        self.assertIn(final, list(range(num_threads)))


class TestRefReadCatchesTimeout(unittest.TestCase):
    """Ref.read() should catch TimeoutException like deref() does."""

    def test_read_source_has_try_except(self):
        """read() source should contain TimeoutException catch."""
        source = inspect.getsource(Ref.read)
        self.assertIn(
            "TimeoutException", source, "Ref.read() should catch TimeoutException"
        )

    def test_read_works_normally(self):
        """Normal read should work."""
        r = Ref(42)
        self.assertEqual(r.read(), 42)


class TestHistoryManagerRateCalc(unittest.TestCase):
    """Rate calculation should use actual elapsed time."""

    def test_source_uses_elapsed(self):
        """compute_max_history source should compute elapsed from accesses[0]."""
        from atomix_stm.core import HistoryManager

        source = inspect.getsource(HistoryManager.compute_max_history)
        self.assertIn(
            "accesses[0]", source, "Should use accesses[0] for elapsed calculation"
        )
        self.assertNotIn("/ 60.0", source, "Should NOT divide by fixed 60.0")


class TestHashMapNoneValues(unittest.TestCase):
    """HashMap should correctly handle keys with None values."""

    def test_none_value_is_retrievable(self):
        """Keys storing None should be retrievable without KeyError."""
        m = PersistentHashMap.from_dict({"a": None, "b": 1})
        self.assertIsNone(m["a"])
        self.assertEqual(m["b"], 1)

    def test_missing_key_raises_key_error(self):
        """Missing keys should still raise KeyError."""
        m = PersistentHashMap.from_dict({"a": 1})
        with self.assertRaises(KeyError):
            _ = m["missing"]

    def test_source_uses_sentinel(self):
        """__getitem__ source should use sentinel object."""
        source = inspect.getsource(PersistentHashMap.__getitem__)
        self.assertIn(
            "sentinel", source.lower(), "__getitem__ should use sentinel pattern"
        )


class TestSeqLockCasValue(unittest.TestCase):
    """SeqLock should have cas_value() and Atom.compare_and_set should use it."""

    def test_cas_value_exists(self):
        """SeqLock should have cas_value method."""
        self.assertTrue(hasattr(SeqLock, "cas_value"))

    def test_cas_value_success(self):
        """cas_value should succeed when expected matches."""
        sl = SeqLock(10)
        self.assertTrue(sl.cas_value(10, 20))
        self.assertEqual(sl.read(), 20)

    def test_cas_value_failure(self):
        """cas_value should fail when expected doesn't match."""
        sl = SeqLock(10)
        self.assertFalse(sl.cas_value(99, 20))
        self.assertEqual(sl.read(), 10)

    def test_atom_cas_uses_public_api(self):
        """Atom.compare_and_set should use cas_value, not private fields."""
        source = inspect.getsource(Atom.compare_and_set)
        self.assertIn(
            "cas_value", source, "Atom.compare_and_set should use SeqLock.cas_value()"
        )
        self.assertNotIn(
            "_write_lock", source, "Atom.compare_and_set should NOT access _write_lock"
        )
        self.assertNotIn(
            "_sequence", source, "Atom.compare_and_set should NOT access _sequence"
        )

    def test_atom_cas_correctness(self):
        """CAS should still work correctly."""
        a = Atom(10)
        self.assertTrue(a.compare_and_set(10, 20))
        self.assertEqual(a.deref(), 20)
        self.assertFalse(a.compare_and_set(10, 30))
        self.assertEqual(a.deref(), 20)


class TestCleanupLogging(unittest.TestCase):
    """_cleanup should log errors, not silently pass."""

    def test_cleanup_no_bare_except_pass(self):
        """_cleanup source should not contain 'except Exception:\\n        pass'."""
        source = inspect.getsource(_cleanup)
        # Should NOT have bare pass after except
        self.assertNotIn("except Exception:\n        pass", source)
        # Should log — either directly via the logger, or via the safe
        # shutdown-aware helpers (_safe_log_info / _safe_log_debug / ...).
        self.assertTrue(
            "logger" in source
            or "_safe_log_info" in source
            or "_safe_log_debug" in source
            or "_safe_log_warning" in source
            or "_safe_log_error" in source,
            "_cleanup should emit log output (directly or via _safe_log_*)",
        )


class TestQueueClosedExported(unittest.TestCase):
    """QueueClosedException should be importable from atomix_stm."""

    def test_importable(self):
        """QueueClosedException should be importable."""
        self.assertTrue(callable(QueueClosedException))

    def test_in_all(self):
        """QueueClosedException should be in __all__."""
        from atomix_stm import core

        self.assertIn("QueueClosedException", core.__all__)

    def test_is_exception(self):
        """QueueClosedException should be a subclass of Exception."""
        self.assertTrue(issubclass(QueueClosedException, Exception))


class TestInitExports(unittest.TestCase):
    """__init__.py should export all symbols from core.__all__."""

    def test_new_exports_available(self):
        """Critical new exports should be importable from atomix_stm."""
        from atomix_stm import (
            io,
            ensure,
            commute,
        )

        # Just verify they're callable or classes
        self.assertTrue(callable(io))
        self.assertTrue(callable(ensure))
        self.assertTrue(callable(commute))


class TestVersionAPI(unittest.TestCase):
    """Version should be 4.3.0 everywhere."""

    def test_core_version(self):
        self.assertEqual(__version__, "4.3.0")

    def test_init_version(self):
        import atomix_stm

        self.assertEqual(atomix_stm.__version__, "4.3.0")

    def test_docstring_version(self):
        import atomix_stm.core as core_module

        self.assertIn("4.3.0", core_module.__doc__)

    def test_pyproject_version(self):
        pyproject_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "pyproject.toml"
        )
        if os.path.exists(pyproject_path):
            with open(pyproject_path, "r") as f:
                content = f.read()
            self.assertIn('version = "4.3.0"', content)


if __name__ == "__main__":
    unittest.main()

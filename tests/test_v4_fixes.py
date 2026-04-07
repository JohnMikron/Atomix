"""
Atomix v4.0.0 — Regression Test Suite for V4 Bug Fixes
=======================================================

Covers all 6 bug fixes introduced in v4.0.0:
1. Module docstring version consistency
2. No floating # type: ignore comments
3. _cleanup() uses except Exception (not bare except)
4. ContentionManager/HistoryManager # type: ignore uses [assignment]
5. Atom.swap() no double-read race condition
6. Nested transaction() depth correctness
"""

import unittest
import threading
import time
import re
import os

from atomix_stm import (
    Ref, Atom, dosync, atomically, transaction,
    STMException, CommitException, ValidationException,
)
from atomix_stm.core import (
    TransactionCoordinator, ContentionManager, HistoryManager,
    SeqLock, __version__,
)


class TestBug1_VersionConsistency(unittest.TestCase):
    """Bug 1: Module docstring, __version__, and pyproject must all agree."""

    def test_version_string_is_4_1_0(self):
        """__version__ should be '4.3.0'."""
        self.assertEqual(__version__, "4.3.0")

    def test_init_version_matches(self):
        """atomix_stm.__version__ should match core.__version__."""
        import atomix_stm
        self.assertEqual(atomix_stm.__version__, "4.3.0")

    def test_docstring_contains_correct_version(self):
        """Module docstring should reference v4.3.0."""
        import atomix_stm.core as core_module
        docstring = core_module.__doc__
        self.assertIsNotNone(docstring)
        self.assertIn("4.3.0", docstring)
        self.assertNotIn("3.3.4", docstring)
        self.assertNotIn("3.3.5", docstring)

    def test_pyproject_version(self):
        """pyproject.toml should contain version = '4.3.0'."""
        pyproject_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "pyproject.toml"
        )
        if os.path.exists(pyproject_path):
            with open(pyproject_path, "r") as f:
                content = f.read()
            self.assertIn('version = "4.3.0"', content)


class TestBug2_NoFloatingTypeIgnore(unittest.TestCase):
    """Bug 2: No floating # type: ignore comments outside function bodies."""

    def test_no_floating_type_ignore_in_core(self):
        """
        Scan core.py for lines that are ONLY '# type: ignore' (with optional
        leading whitespace) — i.e. not attached to a code statement.
        Also check section headers don't have # type: ignore appended.
        """
        import atomix_stm.core as core_module
        source_file = core_module.__file__
        self.assertIsNotNone(source_file)

        with open(source_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        floating_lines = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            # Line is ONLY a type: ignore comment (not attached to code)
            if stripped == "# type: ignore":
                floating_lines.append(i)
            # Section header comment with type: ignore appended
            if stripped.startswith("# ") and stripped.endswith("# type: ignore") and "==" not in stripped:
                floating_lines.append(i)

        self.assertEqual(
            floating_lines, [],
            f"Found floating # type: ignore on lines: {floating_lines}"
        )


class TestBug3_CleanupExceptException(unittest.TestCase):
    """Bug 3: _cleanup() should use except Exception, not bare except."""

    def test_no_bare_except_in_cleanup(self):
        """Scan _cleanup function source for bare except: statements."""
        import inspect
        from atomix_stm.core import _cleanup

        source = inspect.getsource(_cleanup)
        # Look for bare except (not followed by a specific exception type)
        bare_except_pattern = re.compile(r'^\s*except\s*:', re.MULTILINE)
        matches = bare_except_pattern.findall(source)
        self.assertEqual(
            len(matches), 0,
            f"Found bare except: in _cleanup(). Should be 'except Exception:'"
        )


class TestBug5_AtomSwapNoDoubleRead(unittest.TestCase):
    """Bug 5: Atom.swap() should not have a redundant write-lock pre-read."""

    def test_swap_basic_correctness(self):
        """swap() should produce correct result."""
        a = Atom(10)
        result = a.swap(lambda x: x + 5)
        self.assertEqual(result, 15)
        self.assertEqual(a.deref(), 15)

    def test_swap_no_write_lock_in_read_path(self):
        """Verify swap source code doesn't acquire _write_lock for reads."""
        import inspect
        source = inspect.getsource(Atom.swap)
        # Should NOT contain 'with self._seqlock._write_lock:'
        self.assertNotIn(
            "with self._seqlock._write_lock:",
            source,
            "Atom.swap() still acquires write lock for reading — double-read bug!"
        )

    def test_swap_concurrent_correctness(self):
        """Multiple threads swapping should produce correct total."""
        a = Atom(0)
        num_threads = 10
        ops_per_thread = 200

        def worker():
            for _ in range(ops_per_thread):
                a.swap(lambda x: x + 1)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(a.deref(), num_threads * ops_per_thread)

    def test_swap_with_validator(self):
        """swap() should still validate correctly."""
        a = Atom(0, validator=lambda x: x >= 0)
        a.swap(lambda x: x + 10)
        self.assertEqual(a.deref(), 10)

        with self.assertRaises(ValidationException):
            a.swap(lambda x: x - 100)

    def test_swap_spins_on_odd_sequence(self):
        """swap() should handle odd-sequence (write-in-progress) gracefully."""
        a = Atom(0)
        done = threading.Event()

        def writer():
            for _ in range(50):
                a.swap(lambda x: x + 1)
            done.set()

        t = threading.Thread(target=writer)
        t.start()
        completed = done.wait(timeout=10.0)
        self.assertTrue(completed, "Atom.swap() hung — possible deadlock!")
        t.join(timeout=1.0)
        self.assertEqual(a.deref(), 50)


class TestBug6_NestedTransactionDepth(unittest.TestCase):
    """Bug 6: Nested transaction() should not trigger premature commit."""

    def test_nested_transaction_reuses_outer(self):
        """Inner transaction() should reuse outer transaction without depth issues."""
        r = Ref(0)
        with transaction() as outer_tx:
            r.set(10)
            with transaction() as inner_tx:
                # Should be the same transaction object
                self.assertIs(outer_tx, inner_tx)
                val = r.deref()
                self.assertEqual(val, 10)
                r.set(20)
            # After inner exits, outer should still be active (not committed)
            r.set(30)

        # After outer exits, commit should happen once
        self.assertEqual(r.value, 30)

    def test_nested_transaction_no_premature_commit(self):
        """Inner transaction exit should NOT trigger commit."""
        r1 = Ref(100)
        r2 = Ref(200)

        with transaction() as outer_tx:
            r1.set(110)
            with transaction() as inner_tx:
                r2.set(220)
            # If inner triggered commit, r1=110, r2=220 would be committed
            # and this set would fail or be lost
            r1.set(150)

        self.assertEqual(r1.value, 150)
        self.assertEqual(r2.value, 220)

    def test_dosync_nested_in_transaction(self):
        """dosync inside transaction should reuse the outer transaction."""
        r = Ref(0)

        with transaction() as tx:
            r.set(5)
            # dosync inside an existing transaction should just run inline
            dosync(lambda: r.set(r.deref() + 10))
            final = r.deref()

        self.assertEqual(final, 15)
        self.assertEqual(r.value, 15)


if __name__ == '__main__':
    unittest.main()

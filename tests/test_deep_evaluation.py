"""
Atomix v3.2.7 — Deep Evaluation Test Suite
==========================================
=
Comprehensive tests covering all 5 bug fixes and new improvements.
"""

import unittest
import threading
import time
import sys

from atomix_stm import (
    Ref, Atom, STMAgent, STMQueue, STMVar,
    dosync, atomically, transactional, transaction,
    PersistentVector, PersistentHashMap,
    STMException, RetryException, ConflictException,
    CommitException, TransactionAbortedException,
    TimeoutException,
)


class TestBug1_STMAgentSend(unittest.TestCase):
    """Bug 1: STMAgent.send() was using `with dosync():` which fails because
    dosync() is a decorator/function, not a context manager."""

    def test_agent_send_basic(self):
        """Agent should successfully apply a function to its state."""
        agent = STMAgent(10, name="test_agent")
        agent.send(lambda x: x + 5)
        result = agent.await_value(timeout=5.0)
        self.assertEqual(result, 15)

    def test_agent_send_multiple(self):
        """Multiple sends should all be applied."""
        agent = STMAgent(0, name="multi_agent")
        for i in range(10):
            agent.send(lambda x: x + 1)
        result = agent.await_value(timeout=10.0)
        self.assertEqual(result, 10)

    def test_agent_send_no_errors(self):
        """Successful sends should not produce errors."""
        agent = STMAgent(0, name="no_err_agent")
        agent.send(lambda x: x + 1)
        agent.await_value(timeout=5.0)
        self.assertEqual(len(agent.errors), 0)

    def test_agent_send_with_error(self):
        """Agent should capture errors from failed actions."""
        agent = STMAgent(0, name="err_agent")
        agent.send(lambda x: 1 / 0)  # Division by zero
        time.sleep(1.0)
        errors = agent.errors
        self.assertGreater(len(errors), 0)
        self.assertIsInstance(errors[0], ZeroDivisionError)

    def test_agent_clear_errors(self):
        """clear_errors should return and clear accumulated errors."""
        agent = STMAgent(0, name="clear_err_agent")
        agent.send(lambda x: 1 / 0)
        time.sleep(1.0)
        cleared = agent.clear_errors()
        self.assertGreater(len(cleared), 0)
        self.assertEqual(len(agent.errors), 0)


class TestBug2_AtomSwapDeadlock(unittest.TestCase):
    """Bug 2: Atom.swap() was calling self._seqlock.write() inside
    `with self._seqlock._write_lock:`, causing a deadlock because
    SeqLock.write() also acquires the same non-reentrant Lock."""

    def test_atom_swap_basic(self):
        """Atom.swap() should complete without deadlock."""
        a = Atom(10)
        result = a.swap(lambda x: x + 5)
        self.assertEqual(result, 15)
        self.assertEqual(a.deref(), 15)

    def test_atom_swap_with_timeout(self):
        """Atom.swap() should not hang — verified with a timeout."""
        a = Atom(0)
        done = threading.Event()

        def do_swap():
            a.swap(lambda x: x + 1)
            done.set()

        t = threading.Thread(target=do_swap)
        t.start()
        completed = done.wait(timeout=3.0)
        self.assertTrue(completed, "Atom.swap() appears to have deadlocked!")
        t.join(timeout=1.0)
        self.assertEqual(a.deref(), 1)

    def test_atom_swap_concurrent(self):
        """Multiple threads swapping should produce correct final result."""
        a = Atom(0)
        num_threads = 20
        ops_per_thread = 100

        def worker():
            for _ in range(ops_per_thread):
                a.swap(lambda x: x + 1)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(a.deref(), num_threads * ops_per_thread)

    def test_atom_swap_watcher_notification(self):
        """Watchers should be notified during swap."""
        a = Atom(0)
        notifications = []
        a.add_watcher("test", lambda old, new: notifications.append((old, new)))
        a.swap(lambda x: x + 1)
        self.assertEqual(notifications, [(0, 1)])

    def test_atom_reset(self):
        """Atom.reset() should still work correctly."""
        a = Atom(10)
        a.reset(42)
        self.assertEqual(a.deref(), 42)


class TestBug2_AtomCompareAndSet(unittest.TestCase):
    """New improvement: Atom.compare_and_set() for CAS semantics."""

    def test_cas_success(self):
        a = Atom(10)
        result = a.compare_and_set(10, 20)
        self.assertTrue(result)
        self.assertEqual(a.deref(), 20)

    def test_cas_failure(self):
        a = Atom(10)
        result = a.compare_and_set(99, 20)
        self.assertFalse(result)
        self.assertEqual(a.deref(), 10)

    def test_cas_concurrent_only_one_wins(self):
        """Under contention, exactly one CAS should succeed per round."""
        a = Atom(0)
        successes = []

        def try_cas(thread_id):
            result = a.compare_and_set(0, thread_id + 1)
            if result:
                successes.append(thread_id)

        threads = [threading.Thread(target=try_cas, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one thread should have succeeded
        self.assertEqual(len(successes), 1)
        self.assertEqual(a.deref(), successes[0] + 1)


class TestBug3_TransactionNameCollision(unittest.TestCase):
    """Bug 3: The decorator `transaction()` at L1742 was shadowing the
    context manager `transaction()` at L1662."""

    def test_transaction_context_manager_still_works(self):
        """The context manager `transaction()` should still be available."""
        r = Ref(10)
        with transaction() as tx:
            r.alter(lambda x: x + 5)
        self.assertEqual(r.value, 15)

    def test_transactional_decorator_works(self):
        """The renamed transactional() decorator should work."""
        r = Ref(0)

        @transactional(timeout=5.0, max_retries=10)
        def increment():
            r.alter(lambda x: x + 1)

        increment()
        self.assertEqual(r.value, 1)

    def test_transactional_with_contention(self):
        """transactional decorator should handle retries under contention."""
        r = Ref(0)
        num_threads = 5
        ops_per_thread = 50

        def worker():
            for _ in range(ops_per_thread):
                @transactional()
                def inc():
                    r.alter(lambda x: x + 1)
                inc()

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(r.value, num_threads * ops_per_thread)


class TestBug4_STMQueueRetry(unittest.TestCase):
    """Bug 4: STMQueue.get() called retry() outside any transaction scope,
    which would raise STMException."""

    def test_queue_put_get(self):
        """Basic put/get should work."""
        q = STMQueue(name="basic_q")
        dosync(lambda: q.put("hello"))
        result = q.get(timeout=5.0)
        self.assertEqual(result, "hello")

    def test_queue_get_blocks_then_returns(self):
        """get() should block and then return when item becomes available."""
        q = STMQueue(name="block_q")
        results = []

        def consumer():
            val = q.get(timeout=10.0)
            results.append(val)

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.3)
        self.assertEqual(len(results), 0)  # Should still be blocking

        dosync(lambda: q.put("world"))
        t.join(timeout=5.0)
        self.assertEqual(results, ["world"])

    def test_queue_get_timeout(self):
        """get() should raise TimeoutException when empty and times out."""
        q = STMQueue(name="timeout_q")
        with self.assertRaises(TimeoutException):
            q.get(timeout=0.5)

    def test_queue_fifo_order(self):
        """Items should come out in FIFO order."""
        q = STMQueue(name="fifo_q")
        for i in range(5):
            dosync(lambda i=i: q.put(i))

        results = [q.get(timeout=5.0) for _ in range(5)]
        self.assertEqual(results, [0, 1, 2, 3, 4])


class TestBug5_RefSetDoubleCall(unittest.TestCase):
    """Bug 5: Ref.set() outside transaction used dosync(lambda: write(self, value))()
    which had a double-call pattern issue."""

    def test_ref_set_outside_transaction(self):
        """Ref.set() outside a transaction should work correctly."""
        r = Ref(10)
        r.set(42)
        self.assertEqual(r.value, 42)

    def test_ref_set_inside_transaction(self):
        """Ref.set() inside a transaction should work correctly."""
        r = Ref(10)
        dosync(lambda: r.set(42))
        self.assertEqual(r.value, 42)

    def test_ref_value_setter(self):
        """Ref.value = X should also work (calls set() internally)."""
        r = Ref(10)
        r.value = 99
        self.assertEqual(r.value, 99)

    def test_ref_alter_outside_transaction(self):
        """Ref.alter() outside a transaction should still work."""
        r = Ref(10)
        result = r.alter(lambda x: x * 2)
        self.assertEqual(result, 20)
        self.assertEqual(r.value, 20)


class TestSTMAgentImproved(unittest.TestCase):
    """Tests for STMAgent improvements: errors property, clear_errors, await_value."""

    def test_agent_await_value_waits(self):
        """await_value should actually wait for pending actions to complete."""
        agent = STMAgent(0, name="await_agent")

        def slow_increment(x):
            time.sleep(0.3)
            return x + 1

        agent.send(slow_increment)
        # Without await, value may still be 0
        result = agent.await_value(timeout=5.0)
        self.assertEqual(result, 1)

    def test_agent_concurrent_sends(self):
        """Multiple concurrent sends should all be applied correctly."""
        agent = STMAgent(0, name="concurrent_agent")
        num_sends = 50

        for _ in range(num_sends):
            agent.send(lambda x: x + 1)

        result = agent.await_value(timeout=15.0)
        self.assertEqual(result, num_sends)
        self.assertEqual(len(agent.errors), 0)


class TestSTMVarBinding(unittest.TestCase):
    """Tests for STMVar to ensure it still works after refactoring."""

    def test_stmvar_basic(self):
        v = STMVar(42, name="test_var")
        self.assertEqual(v.deref(), 42)

    def test_stmvar_binding(self):
        v = STMVar(42, name="binding_var")
        with v.binding(99):
            self.assertEqual(v.deref(), 99)
        self.assertEqual(v.deref(), 42)

    def test_stmvar_thread_local(self):
        v = STMVar(0, name="tl_var")
        results = []

        def thread_fn(val):
            with v.binding(val):
                time.sleep(0.1)
                results.append(v.deref())

        threads = [threading.Thread(target=thread_fn, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])


class TestVersionCheck(unittest.TestCase):
    """Verify version is correctly updated."""

    def test_version(self):
        """Test version string."""
        import atomix_stm
        self.assertEqual(atomix_stm.__version__, "3.3.0")


if __name__ == '__main__':
    unittest.main()

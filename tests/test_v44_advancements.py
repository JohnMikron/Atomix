"""
Atomix STM — Advanced Capabilities and High-Performance Concurrency Test Suite
==============================================================================

Verifies fine-grained TL2 concurrent commits on disjoint refs, transient
persistent structures, transactional promises, channels, and savepoint rollbacks.
"""

import threading
import time
import unittest

from atomix_stm import (
    PersistentHashMap,
    PersistentVector,
    QueueClosedException,
    Ref,
    SavepointRollbackException,
    STMChannel,
    STMPromise,
    TimeoutException,
    TransientHashMap,
    TransientVector,
    atomically,
    channel,
    get_stm_stats,
    promise,
    savepoint,
)


class TestFineGrainedParallelCommits(unittest.TestCase):
    """Verify that transactions writing to disjoint refs commit concurrently without serialization."""

    def test_disjoint_concurrent_commits(self) -> None:
        """Multiple threads committing to separate Refs should complete with 100% data integrity."""
        num_refs = 20
        refs = [Ref(0, name=f"ref_{i}") for i in range(num_refs)]
        ops_per_thread = 100

        def worker(ref_idx: int) -> None:
            r = refs[ref_idx]
            for _ in range(ops_per_thread):

                @atomically
                def update() -> None:
                    r.alter(lambda x: x + 1)

                update()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_refs)]
        start_time = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start_time

        # All refs must reach exactly ops_per_thread
        for i, r in enumerate(refs):
            self.assertEqual(
                r.value,
                ops_per_thread,
                f"Ref {i} expected {ops_per_thread}, got {r.value}",
            )

        stats = get_stm_stats()
        self.assertTrue(stats["fine_grained_commits"])
        self.assertGreaterEqual(stats["total_commits"], num_refs * ops_per_thread)
        self.assertLess(elapsed, 10.0, "Concurrent disjoint commits took too long")


class TestTransientPersistentStructures(unittest.TestCase):
    """Verify high-performance transient collections and freezing to immutable structures."""

    def test_transient_vector_operations(self) -> None:
        tv: TransientVector[int] = TransientVector([])
        for i in range(100):
            tv.conj(i)
        self.assertEqual(len(tv), 100)
        self.assertEqual(tv[50], 50)

        tv.assoc(50, 999)
        self.assertEqual(tv[50], 999)

        tv.pop()
        self.assertEqual(len(tv), 99)

        pv = tv.persistent()
        self.assertIsInstance(pv, PersistentVector)
        self.assertEqual(len(pv), 99)
        self.assertEqual(pv[50], 999)

        # Modifying after persistent() must raise RuntimeError
        with self.assertRaises(RuntimeError):
            tv.conj(123)
        with self.assertRaises(RuntimeError):
            tv.assoc(0, 0)
        with self.assertRaises(RuntimeError):
            tv.pop()

    def test_persistent_vector_as_transient(self) -> None:
        pv = PersistentVector((1, 2, 3))
        tv = pv.as_transient()
        tv.conj(4).conj(5)
        new_pv = tv.persistent()
        self.assertEqual(len(new_pv), 5)
        self.assertEqual(list(new_pv), [1, 2, 3, 4, 5])
        # Original remains untouched
        self.assertEqual(list(pv), [1, 2, 3])

    def test_transient_hash_map_operations(self) -> None:
        thm: TransientHashMap[str, int] = TransientHashMap({})
        for i in range(50):
            thm.assoc(f"key_{i}", i)
        self.assertEqual(len(thm), 50)
        self.assertEqual(thm["key_10"], 10)

        thm.dissoc("key_10")
        self.assertIsNone(thm.get("key_10"))
        self.assertEqual(len(thm), 49)

        phm = thm.persistent()
        self.assertIsInstance(phm, PersistentHashMap)
        self.assertEqual(len(phm), 49)
        self.assertFalse(phm.contains("key_10"))
        self.assertEqual(phm["key_0"], 0)

        # Modifying after persistent() must raise RuntimeError
        with self.assertRaises(RuntimeError):
            thm.assoc("fail", 1)
        with self.assertRaises(RuntimeError):
            thm.dissoc("key_0")

    def test_persistent_hash_map_as_transient(self) -> None:
        phm = PersistentHashMap.from_dict({"a": 1, "b": 2})
        thm = phm.as_transient()
        thm.assoc("c", 3).assoc("d", 4)
        new_phm = thm.persistent()
        self.assertEqual(len(new_phm), 4)
        self.assertEqual(new_phm["c"], 3)
        # Original untouched
        self.assertEqual(len(phm), 2)


class TestSTMPromise(unittest.TestCase):
    """Verify transactional promise delivery and blocking deref semantics."""

    def test_promise_delivery_and_deref(self) -> None:
        p: STMPromise[int] = promise("test_promise")
        self.assertFalse(p.is_realized())
        self.assertIsNone(p.value)

        def deliverer() -> None:
            time.sleep(0.05)
            delivered = p.deliver(42)
            self.assertTrue(delivered)

        t = threading.Thread(target=deliverer)
        t.start()

        val = p.deref(timeout=2.0)
        self.assertEqual(val, 42)
        self.assertTrue(p.is_realized())
        self.assertEqual(p.value, 42)
        t.join()

        # Second delivery must return False and not overwrite
        self.assertFalse(p.deliver(999))
        self.assertEqual(p.deref(), 42)

    def test_promise_timeout(self) -> None:
        p: STMPromise[str] = STMPromise("timeout_promise")
        with self.assertRaises(TimeoutException):
            p.deref(timeout=0.01)


class TestSTMChannel(unittest.TestCase):
    """Verify multi-producer multi-consumer transactional channel semantics."""

    def test_channel_send_and_receive(self) -> None:
        ch: STMChannel[int] = channel(maxsize=5, name="test_channel")
        self.assertTrue(ch.empty())

        ch.send(100)
        ch.send(200)
        self.assertEqual(ch.size(), 2)

        val1 = ch.receive(timeout=1.0)
        val2 = ch.receive(timeout=1.0)
        self.assertEqual(val1, 100)
        self.assertEqual(val2, 200)
        self.assertTrue(ch.empty())

    def test_channel_close(self) -> None:
        ch: STMChannel[str] = STMChannel(name="closing_channel")
        ch.send("item1")
        ch.close()
        self.assertTrue(ch.is_closed())

        # Can still drain buffered item
        self.assertEqual(ch.receive(timeout=0.1), "item1")
        # Next get on closed empty channel raises QueueClosedException
        with self.assertRaises(QueueClosedException):
            ch.receive(timeout=0.1)


class TestSavepointsAndPartialRollbacks(unittest.TestCase):
    """Verify transactional savepoints allowing partial rollbacks within a transaction."""

    def test_savepoint_rollback_on_exception(self) -> None:
        r1 = Ref(10, name="r1")
        r2 = Ref(20, name="r2")

        @atomically
        def outer_tx() -> None:
            r1.set(100)

            # Inner sub-operation with savepoint
            try:
                with savepoint():
                    r2.set(200)
                    # Trigger explicit savepoint rollback
                    raise SavepointRollbackException("Abort sub-operation")
            except Exception:
                pass

        outer_tx()
        # r1 was modified before savepoint and committed
        self.assertEqual(r1.value, 100)
        # r2 was rolled back to savepoint state and retained 20
        self.assertEqual(r2.value, 20)

    def test_savepoint_success_commits_all(self) -> None:
        r1 = Ref(10, name="r1")
        r2 = Ref(20, name="r2")

        @atomically
        def outer_tx() -> None:
            r1.set(100)
            with savepoint():
                r2.set(200)

        outer_tx()
        self.assertEqual(r1.value, 100)
        self.assertEqual(r2.value, 200)


class TestTelemetryMetrics(unittest.TestCase):
    """Verify latency percentiles and execution telemetry."""

    def test_stats_include_latencies(self) -> None:
        r = Ref(1)
        for _ in range(10):

            @atomically
            def inc() -> None:
                r.alter(lambda x: x + 1)

            inc()

        stats = get_stm_stats()
        self.assertIn("latency_p50", stats)
        self.assertIn("latency_p99", stats)
        self.assertIn("fine_grained_commits", stats)
        self.assertTrue(stats["fine_grained_commits"])


if __name__ == "__main__":
    unittest.main()

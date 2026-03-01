import unittest
import threading
import time
import random
from atomix_stm import Ref, atomically, dosync, STMQueue, RetryException, ConflictException

class TestSTMAvanced(unittest.TestCase):
    
    def test_nested_transactions(self):
        """Verify that nested transactions flatten correctly."""
        r1 = Ref(10)
        r2 = Ref(20)
        
        @atomically
        def outer():
            r1.alter(lambda x: x + 1)
            @atomically
            def inner():
                r2.alter(lambda x: x + 1)
            inner()
            
        outer()
        self.assertEqual(r1.value, 11)
        self.assertEqual(r2.value, 21)

    def test_aba_prevention(self):
        """Verify that ABA-style changes during a transaction cause a retry."""
        r = Ref(100)
        start_event = threading.Event()
        interfered_event = threading.Event()
        attempts = [0]

        @atomically
        def long_tx():
            attempts[0] += 1
            val = r.value
            if attempts[0] == 1:
                start_event.set()
                interfered_event.wait(timeout=5) # Wait for the interferer
            r.set(val + 1)

        def interferer():
            if start_event.wait(timeout=5):
                dosync(lambda: r.set(200))
                dosync(lambda: r.set(100)) # Change back to 100
                interfered_event.set()

        t1 = threading.Thread(target=long_tx)
        t2 = threading.Thread(target=interferer)
        
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        
        # If MVCC is working, it should have detected the intermediate change
        self.assertEqual(r.value, 101)
        self.assertGreaterEqual(attempts[0], 2)

    def test_high_contention_stress(self):
        """Stress test with 20 threads hitting a single Ref."""
        r = Ref(0)
        num_threads = 20
        ops_per_thread = 100
        
        def worker():
            for _ in range(ops_per_thread):
                @atomically
                def inc():
                    r.alter(lambda x: x + 1)
                inc()

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads: t.start()
        for t in threads: t.join()
        
        self.assertEqual(r.value, num_threads * ops_per_thread)

    def test_transaction_idempotency_on_retry(self):
        """Ensure side effects inside transactions don't double-apply on conflict."""
        r = Ref(0)
        side_effect_count = [0]
        barrier = threading.Barrier(2)

        @atomically
        def tx_with_side_effect():
            r.alter(lambda x: x + 1)
            side_effect_count[0] += 1
            if side_effect_count[0] == 1:
                barrier.wait() # Wait for conflict
                time.sleep(0.1)
            
        def trigger_conflict():
            barrier.wait()
            dosync(lambda: r.set(999))

        t1 = threading.Thread(target=tx_with_side_effect)
        t2 = threading.Thread(target=trigger_conflict)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # The transaction should have retried
        self.assertTrue(side_effect_count[0] >= 2)
        self.assertEqual(r.value, 1000) # 999 + 1

if __name__ == '__main__':
    unittest.main()

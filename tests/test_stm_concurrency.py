import threading
import unittest
from atomix_stm import Ref, atomically


class TestSTMConcurrency(unittest.TestCase):
    """Stress test for transactional operations under high contention."""
    
    def test_lost_updates_under_concurrency(self) -> None:
        """Verify that concurrent writes to a shared Ref do not lose updates."""
        r = Ref(0)
        num_threads = 50
        ops_per_thread = 200
        
        def worker() -> None:
            for _ in range(ops_per_thread):
                @atomically
                def inc() -> None:
                    r.alter(lambda x: x + 1)
                inc()
                
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        expected = num_threads * ops_per_thread
        self.assertEqual(
            r.value, expected,
            f"Lost updates detected! Expected {expected}, got {r.value}"
        )

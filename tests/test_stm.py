import unittest
import threading
import time
from atomix_stm import Ref, dosync, atomically, PersistentVector, PersistentHashMap, retry, STMQueue

class TestAtomixSTM(unittest.TestCase):
    
    def test_basic_transaction(self):
        r = Ref(10)
        dosync(lambda: r.alter(lambda x: x + 5))
        self.assertEqual(r.value, 15)

    def test_conflict_resolution(self):
        r = Ref(100)
        barrier = threading.Barrier(2)
        
        def t1_thread():
            barrier.wait()
            @atomically
            def logic():
                val = r.value
                time.sleep(0.1)  # Force overlap
                r.set(val + 50)
            logic()

        def t2_thread():
            barrier.wait()
            @atomically
            def logic():
                val = r.value
                r.set(val + 20)
            logic()

        threads = [threading.Thread(target=t1_thread), threading.Thread(target=t2_thread)]

        for t in threads: t.start()
        for t in threads: t.join()
        
        # Expected: 100 + 50 + 20 = 170 (One must retry)
        self.assertEqual(r.value, 170)

    def test_persistent_vector_bloat_fix(self):
        v = PersistentVector.empty()
        # Add many items to grow the tree
        for i in range(100):
            v = v.conj(i)
        
        # Pop all items
        for _ in range(100):
            v = v.pop()
        
        # After popping all, it should be empty and shift should be back to 5
        self.assertEqual(len(v), 0)
        self.assertEqual(v._shift, 5)

    def test_queue_retry(self):
        q = STMQueue()
        res = []
        
        @atomically
        def consumer_logic():
            val = q.get()
            res.append(val)
                
        t = threading.Thread(target=consumer_logic)
        t.start()
        
        time.sleep(0.1)
        self.assertEqual(len(res), 0)  # Should be waiting
        
        dosync(lambda: q.put("hello"))
        
        t.join()
        self.assertEqual(res[0], "hello")


if __name__ == '__main__':
    unittest.main()

import unittest
import time
from atomix_stm import (
    PersistentVector, STMQueue, Atom, transaction,
    TransactionCoordinator, reset_stm, dosync
)

class TestAdvancedEdgeCases(unittest.TestCase):
    def setUp(self):
        reset_stm()

    def test_persistent_vector_33_to_32_boundary(self):
        # Conj up to 33 elements
        v = PersistentVector.empty()
        for i in range(33):
            v = v.conj(i)
        self.assertEqual(len(v), 33)
        self.assertEqual(v[32], 32)

        # Pop back to 32
        v = v.pop()
        self.assertEqual(len(v), 32)
        
        # Read all index accesses (the broken tail boundary bug)
        for i in range(32):
            self.assertEqual(v[i], i, f"Failed at index {i}")

    def test_stmqueue_none_sentinel(self):
        q = STMQueue(maxsize=5)
        
        # Put a legitimate None
        self.assertTrue(q.put(None))
        self.assertEqual(q.size(), 1)
        
        # Get should return None legitimately
        item = q.get(timeout=1.0)
        self.assertIsNone(item)
        self.assertEqual(q.size(), 0)

    def test_atom_watcher_reentrant(self):
        a = Atom(0)
        
        def risky_watcher(old_val, new_val):
            # A watcher that does another operation on the atom
            if new_val == 1:
                a.compare_and_set(1, 10)
                
        a.add_watcher("risky", risky_watcher)
        
        a.swap(lambda x: x + 1)
        self.assertEqual(a.deref(), 10)

if __name__ == '__main__':
    unittest.main()

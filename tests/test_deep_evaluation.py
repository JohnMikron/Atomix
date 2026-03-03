import unittest
import threading
import time
from atomix_stm import Ref, dosync, atomically, PersistentVector, PersistentHashMap, retry, STMQueue, transaction, Atom, STMAgent, STMException

class TestDeepEvaluation(unittest.TestCase):
    def test_persistent_vector_large(self):
        v = PersistentVector.empty()
        for i in range(10000):
            v = v.conj(i)

        self.assertEqual(len(v), 10000)
        self.assertEqual(v[5000], 5000)

        # Test Assoc
        v = v.assoc(5000, 9999)
        self.assertEqual(v[5000], 9999)

        # Test Pop
        for _ in range(5000):
            v = v.pop()
        self.assertEqual(len(v), 5000)
        self.assertEqual(v[4999], 4999)

    def test_persistent_hash_map(self):
        m = PersistentHashMap.empty()
        for i in range(1000):
            m = m.assoc(f"key_{i}", i)

        self.assertEqual(len(m), 1000)
        self.assertEqual(m["key_500"], 500)

        m = m.assoc("key_500", 9999)
        self.assertEqual(m["key_500"], 9999)

        m = m.dissoc("key_500")
        self.assertEqual(len(m), 999)
        with self.assertRaises(KeyError):
            _ = m["key_500"]

    def test_agent_error_handling(self):
        agent = STMAgent(0)
        def faulty_action(state, val):
            if val == "error":
                raise ValueError("Intentional Error")
            return state + val

        agent.send(faulty_action, 5)
        time.sleep(0.1)
        self.assertEqual(agent.deref(), 5)

        agent.send(faulty_action, "error")
        time.sleep(0.1)
        self.assertEqual(agent.deref(), 5) # Value unchanged
        self.assertEqual(len(agent._errors), 1)

    def test_atom_watchers(self):
        a = Atom(10)
        notified = []
        def watcher(old, new):
            notified.append((old, new))

        a.add_watcher("test", watcher)
        a.swap(lambda x: x + 5)
        self.assertEqual(a.value, 15)
        self.assertEqual(notified, [(10, 15)])

        a.reset(20)
        self.assertEqual(notified, [(10, 15), (15, 20)])

    def test_ref_validators(self):
        def is_positive(val):
            return val > 0

        r = Ref(10, validator=is_positive)

        @atomically
        def update_ref():
            r.set(20)

        update_ref()
        self.assertEqual(r.value, 20)

        with self.assertRaises(Exception): # should be ValidationException but we catch base
            @atomically
            def bad_update():
                r.set(-5)
            bad_update()

        self.assertEqual(r.value, 20)

    def test_timeout_exception(self):
        r = Ref(10)

        with self.assertRaises(STMException):
            @atomically
            def long_tx():
                val = r.value
                time.sleep(0.2)
                r.set(val + 1)
            # Use smaller timeout manually via transaction context decorator
            @transaction(timeout=0.1)
            def timeout_tx():
                long_tx()
            timeout_tx()

    def test_commute(self):
        r = Ref(10)

        def slow_inc(x):
            time.sleep(0.01)
            return x + 1

        @atomically
        def do_commute():
            r.commute(slow_inc)

        threads = [threading.Thread(target=do_commute) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(r.value, 15)

if __name__ == '__main__':
    unittest.main()

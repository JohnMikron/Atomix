from atomix_stm import Ref, atomically, dosync, STMQueue
import threading
import time

# Example 1: Bank Transfer (Classic STM use case)
def bank_example():
    print("\n--- Bank Transfer Example ---")
    acc_a = Ref(1000, name="Acc-A")
    acc_b = Ref(500, name="Acc-B")

    @atomically
    def transfer(amount):
        if acc_a.value < amount:
            raise ValueError("Insufficient funds")
        acc_a.alter(lambda x: x - amount)
        acc_b.alter(lambda x: x + amount)

    print(f"Initial: A={acc_a.value}, B={acc_b.value}")
    transfer(200)
    print(f"After transfer: A={acc_a.value}, B={acc_b.value}")

# Example 2: Concurrent Producer/Consumer with STMQueue
def queue_example():
    print("\n--- STMQueue Example ---")
    q = STMQueue(maxsize=5)
    
    def producer():
        for i in range(10):
            dosync(lambda i=i: q.put(f"Msg-{i}"))
            print(f"Produced Msg-{i}")
            time.sleep(0.05)

    def consumer():
        for _ in range(10):
            msg = dosync(lambda: q.get())
            print(f"Consumed {msg}")

    p = threading.Thread(target=producer)
    c = threading.Thread(target=consumer)
    p.start(); c.start()
    p.join(); c.join()

if __name__ == "__main__":
    bank_example()
    queue_example()

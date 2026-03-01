import time
import threading
import statistics
from atomix_stm import Ref, atomically, dosync

# --- Configuration ---
NUM_THREADS = [1, 2, 4, 8, 16]
ITERATIONS = 100_000

def bench_lock(num_threads):
    counter = 0
    lock = threading.Lock()
    
    def worker():
        nonlocal counter
        for _ in range(ITERATIONS):
            with lock:
                counter += 1

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    return time.time() - start

def bench_stm(num_threads):
    ref = Ref(0)
    
    @atomically
    def increment():
        ref.alter(lambda x: x + 1)

    def worker():
        for _ in range(ITERATIONS):
            increment()

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    return time.time() - start

def run_suite():
    print(f"{'Threads':<10} | {'Lock (s)':<12} | {'STM (s)':<12} | {'Ratio (STM/Lock)':<15}")
    print("-" * 55)
    
    for t in NUM_THREADS:
        # Warmup
        bench_lock(1)
        bench_stm(1)
        
        l_time = bench_lock(t)
        s_time = bench_stm(t)
        ratio = s_time / l_time
        print(f"{t:<10} | {l_time:<12.4f} | {s_time:<12.4f} | {ratio:<15.2f}")

if __name__ == "__main__":
    print("Atomix STM vs Threading.Lock Performance Comparison")
    print(f"Iterations per thread: {ITERATIONS}")
    print("Note: STM overhead is expected to be higher on low thread counts,")
    print("but provides safety and scalability on high contention / No-GIL.\n")
    run_suite()

import time
import threading
from atomix_stm import Ref, atomically, dosync

# --- Benchmark 1: High Contention Writes ---
def benchmark_contention(num_threads=10, iterations=1000):
    counter = Ref(0)
    
    @atomically
    def increment():
        val = counter.value
        counter.set(val + 1)

    start_time = time.time()
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=lambda: [increment() for _ in range(iterations)])
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    end_time = time.time()
    print(f"--- High Contention Benchmark ---")
    print(f"Threads: {num_threads}, Iterations per thread: {iterations}")
    print(f"Total logical increments: {num_threads * iterations}")
    print(f"Final value: {counter.value}")
    print(f"Time taken: {end_time - start_time:.4f}s")
    print(f"Ops/sec: {(num_threads * iterations) / (end_time - start_time):.2f}")

# --- Benchmark 2: Read Heavy Workload ---
def benchmark_reads(num_readers=8, num_writers=2, iterations=5000):
    data = Ref(100)
    
    def reader():
        for _ in range(iterations):
            _ = data.value

    def writer():
        for _ in range(iterations // 10):
            @atomically
            def _write():
                data.alter(lambda x: x + 1)
            _write()

    start_time = time.time()
    threads = []
    for _ in range(num_readers):
        threads.append(threading.Thread(target=reader))
    for _ in range(num_writers):
        threads.append(threading.Thread(target=writer))
        
    for t in threads: t.start()
    for t in threads: t.join()
    
    end_time = time.time()
    print(f"\n--- Read/Write Ratio Benchmark ---")
    print(f"Readers: {num_readers}, Writers: {num_writers}")
    print(f"Time taken: {end_time - start_time:.4f}s")

if __name__ == "__main__":
    benchmark_contention()
    benchmark_reads()

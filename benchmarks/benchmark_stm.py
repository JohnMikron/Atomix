import threading
import time
from atomix_stm import Ref, atomically


# --- Benchmark 1: High Contention Writes ---
def benchmark_contention(num_threads: int = 10, iterations: int = 1000) -> None:
    counter = Ref(0)

    @atomically
    def increment() -> None:
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
    print("--- High Contention Benchmark ---")
    print(f"Threads: {num_threads}, Iterations per thread: {iterations}")
    print(f"Total logical increments: {num_threads * iterations}")
    print(f"Final value: {counter.value}")
    print(f"Time taken: {end_time - start_time:.4f}s")
    print(f"Ops/sec: {(num_threads * iterations) / (end_time - start_time):.2f}")


# --- Benchmark 2: Read Heavy Workload ---
def benchmark_reads(
    num_readers: int = 8, num_writers: int = 2, iterations: int = 5000
) -> None:
    data = Ref(100)

    def reader() -> None:
        for _ in range(iterations):
            _ = data.value

    def writer() -> None:
        for _ in range(iterations // 10):

            @atomically
            def _write() -> None:
                data.alter(lambda x: x + 1)

            _write()

    start_time = time.time()
    threads = []
    for _ in range(num_readers):
        threads.append(threading.Thread(target=reader))
    for _ in range(num_writers):
        threads.append(threading.Thread(target=writer))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    end_time = time.time()
    print("\n--- Read/Write Ratio Benchmark ---")
    print(f"Readers: {num_readers}, Writers: {num_writers}")
    print(f"Time taken: {end_time - start_time:.4f}s")


if __name__ == "__main__":
    benchmark_contention()
    benchmark_reads()

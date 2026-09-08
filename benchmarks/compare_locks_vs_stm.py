import threading
import time
from typing import List
from atomix_stm import Ref, atomically, transactional

# --- Configuration ---
NUM_THREADS = [1, 2, 4, 8, 16]
ITERATIONS = 500


def bench_lock(num_threads: int, iterations: int = ITERATIONS) -> float:
    counter = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal counter
        for _ in range(iterations):
            with lock:
                counter += 1

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - start


def bench_stm_hotspot(num_threads: int, iterations: int = ITERATIONS) -> float:
    ref = Ref(0)

    @transactional(max_retries=5000)
    def increment() -> None:
        ref.alter(lambda x: x + 1)

    def worker() -> None:
        for _ in range(iterations):
            increment()

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - start


def bench_stm_disjoint(num_threads: int, iterations: int = ITERATIONS) -> float:
    refs: List[Ref[int]] = [Ref(0) for _ in range(num_threads)]

    def worker(idx: int) -> None:
        target = refs[idx]

        @atomically
        def increment() -> None:
            target.alter(lambda x: x + 1)

        for _ in range(iterations):
            increment()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - start


def run_suite() -> None:
    print(
        f"{'Threads':<8} | {'Lock (s)':<10} | {'STM Hotspot (s)':<16} | {'STM Disjoint (s)':<17} | {'Disjoint Ops/s':<14}",
        flush=True,
    )
    print("-" * 75, flush=True)

    for t in NUM_THREADS:
        # Warmup
        bench_lock(1, iterations=100)
        bench_stm_hotspot(1, iterations=100)

        l_time = bench_lock(t)
        s_hot_time = bench_stm_hotspot(t)
        s_disj_time = bench_stm_disjoint(t)
        total_ops = t * ITERATIONS
        ops_sec = total_ops / s_disj_time if s_disj_time > 0 else 0.0

        print(
            f"{t:<8} | {l_time:<10.4f} | {s_hot_time:<16.4f} | {s_disj_time:<17.4f} | {ops_sec:<14.1f}",
            flush=True,
        )


if __name__ == "__main__":
    print("Atomix STM vs Threading.Lock Performance Comparison", flush=True)
    print(f"Iterations per thread: {ITERATIONS}", flush=True)
    print("STM Hotspot: all threads contention on a single shared Ref.", flush=True)
    print(
        "STM Disjoint: each thread commits to independent Refs (parallel TL2 commits).\n",
        flush=True,
    )
    run_suite()

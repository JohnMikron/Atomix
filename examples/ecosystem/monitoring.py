"""
Atomix STM Monitoring & Instrumentation Example

This example demonstrates how to add "Ecosystem" features like monitoring,
logging, and transaction metrics to Atomix STM.

While Atomix targets performance, observability is key for top-tier libs.
We show how to wrap `dosync` to track:
1. Transaction duration.
2. Retry counts (conflicts).
3. Success/Failure rates.
"""

import time
import random
import threading
from atomix_stm import Atom, dosync, ConflictException

# Global metrics (In a real app, use Prometheus or OpenTelemetry)
metrics = {
    "total_tx": Atom(0),
    "total_retries": Atom(0),
    "total_failures": Atom(0),
    "latencies": Atom([])  # Simple list for demo
}

def instrumented_dosync(tx_func, *args, **kwargs):
    """
    A wrapper around dosync that provides instrumentation.
    In a professional library, this could be a decorator or a hook system.
    """
    start_time = time.perf_counter()
    retries = 0
    
    def wrapped_tx():
        nonlocal retries
        # We can't easily count retries from inside dosync without modifying core,
        # but we can track attempts if we implement our own loop or use hooks.
        # Here we simulate the concept.
        return tx_func(*args, **kwargs)

    try:
        # Note: Atomix's internal dosync handles retries.
        # To track internal retries, we would ideally have a hook in core.py.
        # For now, we track top-level success.
        result = dosync(wrapped_tx)
        
        duration = time.perf_counter() - start_time
        
        # Update metrics atomically
        def update_metrics():
            metrics["total_tx"].swap(lambda x: x + 1)
            # Simulate adding to a latency distribution
            curr_lat = metrics["latencies"].deref()
            metrics["latencies"].reset((curr_lat + [duration])[-100:]) # Keep last 100
            
        dosync(update_metrics)
        return result
    except Exception as e:
        dosync(lambda: metrics["total_failures"].swap(lambda x: x + 1))
        raise e

# --- Demo ---

shared_counter = Atom(0)

def worker(worker_id):
    for _ in range(10):
        time.sleep(random.uniform(0.01, 0.1))
        try:
            instrumented_dosync(lambda: shared_counter.swap(lambda x: x + 1))
        except Exception:
            pass

def reporter():
    while True:
        time.sleep(1)
        tx = metrics["total_tx"].deref()
        fails = metrics["total_failures"].deref()
        lats = metrics["latencies"].deref()
        avg_lat = sum(lats)/len(lats) if lats else 0
        
        print(f"\n[METRICS] TX: {tx} | Fails: {fails} | Avg Latency: {avg_lat:.6f}s")
        print(f"[STATE] Counter: {shared_counter.deref()}")

if __name__ == "__main__":
    print("Starting Instrumented Atomix Demo...")
    
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    report_thread = threading.Thread(target=reporter, daemon=True)
    
    report_thread.start()
    for t in threads: t.start()
    for t in threads: t.join()
    
    time.sleep(2)
    print("\nFinal Report:")
    print(f"Total Transactions: {metrics['total_tx'].deref()}")
    print(f"Final Count: {shared_counter.deref()}")

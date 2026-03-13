"""
Atomix STM + Structured Concurrency (AnyIO/Trio) Example

This example demonstrates how Atomix STM integrates with structured concurrency
libraries like AnyIO or Trio. 

STM is the perfect companion for structured concurrency because it provides 
a safe way for different tasks (fibers/coroutines) to share state without 
the risks of traditional locks or the limitations of single-threaded event loops.
"""

import anyio
from atomix_stm import Atom, dosync, retry
import time

# Shared state: A work queue and a results counter
work_queue = Atom([])
results_processed = Atom(0)

async def producer():
    """Produces work items periodically."""
    for i in range(10):
        print(f"  [Producer] Adding job {i}")
        # swap() is thread-safe and can be called from async via to_thread
        # but for simple Atoms, calling it directly usually works if the library 
        # is thread-safe (which Atomix is).
        await anyio.to_thread.run_sync(lambda: work_queue.swap(lambda q: q + [f"job_{i}"]))
        await anyio.sleep(0.5)

async def worker(worker_id):
    """Consumes work items from the STM queue."""
    while True:
        def get_job():
            q = work_queue.deref()
            if not q:
                # This causes the transaction to block until work_queue is touched
                # by another thread (like the producer).
                retry()
            
            job = q[0]
            work_queue.reset(q[1:])
            return job

        # Wait for a job using STM coordination
        job = await anyio.to_thread.run_sync(dosync, get_job)
        print(f"    [Worker {worker_id}] Processing {job}")
        
        # Simulate work
        await anyio.sleep(1)
        
        # Update result counter atomically
        await anyio.to_thread.run_sync(lambda: results_processed.swap(lambda x: x + 1))

async def main():
    print("Starting Atomix STM + AnyIO Demo...")
    
    async with anyio.create_task_group() as tg:
        tg.start_soon(producer)
        tg.start_soon(worker, 1)
        tg.start_soon(worker, 2)
        
        # Stop after some time
        await anyio.sleep(8)
        print("\nDemo finished.")
        print(f"Total Results Processed: {results_processed.deref()}")
        tg.cancel_scope.cancel()

if __name__ == "__main__":
    try:
        anyio.run(main)
    except (KeyboardInterrupt, SystemExit):
        pass

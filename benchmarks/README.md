# Atomix STM Benchmarks 📊

This directory contains scripts to verify the performance claims of Atomix STM.

## 🏃 How to Run

1. **Basic Comparison**:
   Compare STM performance against traditional `threading.Lock`.
   ```bash
   python benchmarks/compare_locks_vs_stm.py
   ```

2. **High Contention Test**:
   Stress test the Adaptive Reaper and Contention Manager.
   ```bash
   python benchmarks/benchmark_stm.py
   ```

## 📈 Expected Results

### Python < 3.13 (with GIL)
On standard Python, `threading.Lock` will often be faster for simple increments because the GIL serializes everything anyway. STM provides **safety** and **semantic clarity** here, but the performance gains are limited by the GIL.

### Python 3.13+ (Free-threading / No-GIL)
This is where Atomix STM shines. 
- **Readers never block writers**: In a read-heavy workload, STM can outperform Locks by 3x-5x because it uses MVCC snapshots.
- **Scalability**: As thread counts increase, the overhead of "Lock Contention" (threads waiting for a single lock) grows exponentially, while STM's optimistic concurrency scales linearly with available CPU cores.

## 🔍 Understanding the "Overhead"
STM is a "Software" implementation of transactional memory. It records reads and writes to ensure atomicity. This bookkeeping has a cost (~2x-4x slower than a raw, uncontended lock). You use STM when you need **correctness** in complex state changes where multiple locks would cause deadlocks.

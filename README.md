# Atomix STM (v4.4.0)

**Software Transactional Memory for Python 3.13+ (No-GIL Ready)**

Atomix STM provides a thread-safe way to manage shared state without the complexity of deadlocks, race conditions, or explicit locking.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

---

## Why Atomix?

Coordinating shared state with manual locking can be difficult, often leading to deadlocks, priority inversion, or race conditions. **Atomix STM** addresses these challenges by providing:

- **Atomic Transactions**: State transitions are all-or-nothing (ACID semantics in-memory).
- **Consistent Reads**: Transactions observe a consistent snapshot of the state without blocking writers (MVCC).
- **Isolated State**: Transactions execute in isolation with speculative write-buffers.
- **Free-Threading Safe & Parallel Commits**: Built for Python 3.13+ free-threading. Read paths are lock-free and fully concurrent. Commits employ a fine-grained, TL2-style ordered lock strategy: transactions touching disjoint `Ref` sets commit concurrently in parallel across all CPU cores without blocking each other.

---

## Quick Start

```python
from atomix_stm import Ref, dosync, atomically

# 1. Define shared state
balance_a = Ref(1000)
balance_b = Ref(500)

# 2. Perform atomic operations
@atomically
def transfer():
    if balance_a.value < 200:
        raise ValueError("Insufficient funds")
    balance_a.alter(lambda x: x - 200)
    balance_b.alter(lambda x: x + 200)

# 3. Safe, concurrent execution
transfer()

print(f"Balance A: {balance_a.value}")  # 800
print(f"Balance B: {balance_b.value}")  # 700
```

---

## Installation

```bash
pip install atomix-stm
```

> [!NOTE]
> **Compatibility**: Atomix STM requires **Python 3.13+** as it is built to utilize the concurrency features of free-threaded Python.

---

## Features

- **MVCC (Multi-Version Concurrency Control)**: Lock-free readers never block writers.
- **TL2 Fine-Grained Parallel Commits**: Canonical ordered acquisition of locks allows disjoint transactions to commit simultaneously across CPU cores.
- **`Ref`**: Transactional reference coordinating multiple values atomically.
- **`Atom`**: Independent atomic reference supporting lock-free compare-and-set (CAS) and watchers.
- **`STMPromise`**: Write-once atomic transactional promises with timeouts.
- **`STMChannel`**: Transactional bounded message-passing channels with backpressure (Go/core.async style).
- **Savepoints & Partial Rollback**: `savepoint()` context manager for nested transaction scoping and error isolation.
- **Persistent & Transient Data Structures**: Immutable `PersistentVector` and `PersistentHashMap` with $O(1)$ mutable `TransientVector` and `TransientHashMap` batch builders.
- **`STMAgent`**: Asynchronous state manager with thread pool and error tracking.
- **`STMQueue`**: Transactional FIFO queue with blocking retrieval and retry semantics.
- **`STMVar`**: Thread-local dynamic variable bindings.
- **Advanced Diagnostics**: Built-in latency metrics (P50, P99, avg latency, conflict/abort counters) via `get_stm_stats()`.

---

## Core API & Examples

### Transactions with `Ref`

```python
from atomix_stm import Ref, atomically, dosync

counter = Ref(0)

# Decorator form
@atomically
def increment():
    counter.alter(lambda x: x + 1)
increment()

# Function form
dosync(lambda: counter.alter(lambda x: x + 1))

print(counter.value)  # 2
```

### Savepoints & Partial Rollback

Isolate sub-operations or implement speculative transaction branches without aborting the parent transaction:

```python
from atomix_stm import Ref, atomically, savepoint, SavepointRollbackException

account = Ref(100)
log = Ref([])

@atomically
def risky_operation():
    account.alter(lambda x: x - 50)
    
    # Try a speculative branch
    with savepoint():
        account.alter(lambda x: x - 1000)  # Overdraft
        # Rolled back automatically on SavepointRollbackException
        raise SavepointRollbackException("Branch cancelled")
    
    log.alter(lambda l: l + ["Branch handled safely"])

risky_operation()
print(account.value)  # 50 (overdraft was undone, top-level change persisted)
```

### Transient Collections (High-Performance Batch Mutations)

Perform bulk modifications in $O(1)$ operations before freezing into persistent data structures:

```python
from atomix_stm import PersistentVector, PersistentHashMap

# Batch mutating a vector
vec = PersistentVector((1, 2, 3))
trans = vec.as_transient()
for i in range(4, 1000):
    trans.conj(i)
fast_vec = trans.persistent()

# Batch mutating a hash map
hmap = PersistentHashMap.from_dict({"a": 1})
m_trans = hmap.as_transient()
for i in range(1000):
    m_trans.assoc(f"key_{i}", i)
fast_map = m_trans.persistent()
```

### Promises (`STMPromise`)

Thread-safe, write-once atomic resolution:

```python
from atomix_stm import promise, run_concurrent

p = promise()

def worker():
    p.deliver("computed result")

run_concurrent([worker])
result = p.deref(timeout=2.0)
print(result)  # "computed result"
```

### Channels (`STMChannel`)

Transactional message passing with bounded buffers:

```python
from atomix_stm import channel

chan = channel(maxsize=10)
chan.send("job_payload")
item = chan.receive()
print(item)  # "job_payload"
```

### Atoms (Lock-Free CAS)

```python
from atomix_stm import Atom

a = Atom(0)
a.swap(lambda x: x + 1)
a.compare_and_set(1, 42)
a.add_watcher("log", lambda old, new: print(f"{old} -> {new}"))
```

### Agents (Async State)

```python
from atomix_stm import STMAgent

agent = STMAgent(0)
agent.send(lambda x: x + 10)
result = agent.await_value(timeout=5.0)
print(result)  # 10
print(agent.errors)  # []
```

### Telemetry & Latency Profiling

```python
from atomix_stm import get_stm_stats

stats = get_stm_stats()
print(f"Commits: {stats['commits']}")
print(f"Aborts: {stats['total_aborts']}")
print(f"Average Latency: {stats['avg_latency_ms']:.3f} ms")
print(f"P50 Latency: {stats['p50_latency_ms']:.3f} ms")
print(f"P99 Latency: {stats['p99_latency_ms']:.3f} ms")
```

---

## Performance & Benchmarks

Measured on Python 3.13 comparing standard `threading.Lock` against Atomix STM under both single-ref hotspot contention and multi-ref parallel disjoint commits (500 operations per thread):

| Threads | Standard Lock | STM Hotspot | STM Disjoint | Disjoint Throughput |
| :---: | :---: | :---: | :---: | :---: |
| **1** | 0.0005 s | 0.0286 s | 0.0291 s | **17,197 ops/s** |
| **2** | 0.0009 s | 0.0694 s | 0.0571 s | **17,518 ops/s** |
| **4** | 0.0016 s | 0.1816 s | 0.2568 s | **7,787 ops/s** |
| **8** | 0.0048 s | 0.6149 s | 0.2531 s | **15,801 ops/s** |
| **16** | 0.0058 s | 2.6449 s | 0.8598 s | **9,304 ops/s** |

- **Hotspot Contention**: When multiple threads contend on a single shared `Ref`, Atomix resolves conflicts with adaptive contention backoff, guaranteeing zero lost updates.
- **Disjoint Concurrency**: When threads operate across independent `Ref` sets, the TL2 commit engine commits in parallel across cores without serializing, achieving up to 17,500 operations per second in pure Python.

---

## ⚠️ Known Characteristics & Design Trade-offs

While Atomix STM is designed for coordinating shared state in concurrent Python programs (including free-threaded Python 3.13+), keep in mind:

1. **Fine-Grained Commit Synchronization**: Disjoint transactions commit concurrently in parallel. Transactions writing to the exact same `Ref` instances will acquire ordered per-ref locks, preventing lost updates and deadlocks.
2. **Memory Footprint**: Multi-version concurrency control keeps historical versions of values to support read-only transactions. While a background reaper cleans up stale versions, memory usage is higher than in-place mutations.
3. **Pure Python Implementation**: Atomix is written in 100% pure Python with zero external C/Rust binary dependencies, maximizing portability and compatibility across all platforms while leveraging `__slots__`, fast bitwise indexing, and Python 3.13 specialization.
4. **Side Effects**: Transactions may be retried multiple times before committing. Do not perform side effects (such as network calls or console I/O) directly inside `dosync` blocks unless wrapped in the `io()` decorator.

---

## Licensing

This project is licensed under the **MIT License**. See [LICENSE](https://github.com/JohnMikron/Atomix/blob/main/LICENSE) for details.

---

## Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/JohnMikron/Atomix/blob/main/CONTRIBUTING.md) for dev setup, testing, and style guide details.

Please also review our [Code of Conduct](https://github.com/JohnMikron/Atomix/blob/main/CODE_OF_CONDUCT.md) and [Security Policy](https://github.com/JohnMikron/Atomix/blob/main/SECURITY.md).

---

© 2026 John Mikron.

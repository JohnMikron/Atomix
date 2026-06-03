# Atomix STM (v4.3.0)

**Software Transactional Memory for Python 3.13+ (No-GIL Ready)**

Atomix STM provides a thread-safe way to manage shared state without the complexity of deadlocks, race conditions, or explicit locking.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

---

## Why Atomix?

Coordinating shared state with manual locking can be difficult, often leading to deadlocks, priority inversion, or race conditions. **Atomix STM** addresses these challenges by providing:

- **Atomic Transactions**: State transitions are all-or-nothing.
- **Consistent Reads**: Transactions observe a consistent snapshot of the state.
- **Isolated State**: Transactions execute in isolation.
- **Free-Threading Safe**: Optimized for Python 3.13+ with free-threading. Read paths are lock-free and fully concurrent. Writes serialize on a single global coordinator lock to guarantee consistent MVCC version stamping and history maintenance.

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

- **MVCC (Multi-Version Concurrency Control)**: Readers do not block writers.
- **`Ref`**: Transactional reference coordinating multiple values atomically.
- **`Atom`**: Independent atomic reference supporting compare-and-set (CAS).
- **`STMAgent`**: Async state manager with error tracking.
- **`STMQueue`**: Transactional FIFO queue with blocking retrieval.
- **`STMVar`**: Thread-local dynamic variable bindings.
- **Persistent Data Structures**: Immutable `PersistentVector` and `PersistentHashMap`.
- **Diagnostics**: Built-in stats via `get_stm_stats()`.

---

## Core API

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

### Atoms

```python
from atomix_stm import Atom

a = Atom(0)
a.swap(lambda x: x + 1)
a.compare_and_set(1, 42)
a.add_watcher("log", lambda old, new: print(f"{old} -> {new}"))
```

### Agents (async state)

```python
from atomix_stm import STMAgent
import time

agent = STMAgent(0)
agent.send(lambda x: x + 10)
result = agent.await_value(timeout=5.0)
print(result)  # 10
print(agent.errors)  # []
```

### STMQueue

```python
from atomix_stm import STMQueue, dosync

q = STMQueue()
dosync(lambda: q.put("hello"))
val = q.get(timeout=5.0)
print(val)  # "hello"
```

---

## ⚠️ Known Limitations & What this is NOT for

While Atomix STM is suitable for coordinating shared state in concurrent Python programs (including free-threaded Python 3.13+), you should be aware of the following design trade-offs:

1. **Serialized Commits**: Although transactions read values concurrently without locks (using MVCC), they must serialize their write phase on a single global coordinator lock. This ensures a total order of transactions and avoids lost updates under contention.
2. **Memory Footprint**: Multi-version concurrency control keeps historical versions of values to support read-only transactions. While a background reaper cleans up stale versions, memory usage will be higher than simple in-place mutations.
3. **Python Overhead**: The library is implemented in pure Python. While optimized for minimum overhead (e.g. using `__slots__` and SeqLocks), it is not a replacement for native C-level concurrent data structures when raw performance is the only metric.
4. **Side Effects**: Transactions may be retried multiple times before committing. Do not perform side effects (such as network calls or console I/O) directly inside `dosync` blocks unless wrapped in the `io()` decorator to delay execution until after the transaction succeeds.

---

## Licensing

This project is licensed under the **MIT License**. See [LICENSE](https://github.com/JohnMikron/Atomix/blob/main/LICENSE) for details.

---

## Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/JohnMikron/Atomix/blob/main/CONTRIBUTING.md) for dev setup, testing, and style guide details.

Please also review our [Code of Conduct](https://github.com/JohnMikron/Atomix/blob/main/CODE_OF_CONDUCT.md) and [Security Policy](https://github.com/JohnMikron/Atomix/blob/main/SECURITY.md).

---

© 2026 John Mikron.

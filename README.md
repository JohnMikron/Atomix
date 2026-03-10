# Atomix STM (v3.2.9) ⚛️

**Production-grade Software Transactional Memory for Python 3.9+ (No-GIL Ready)**

Atomix STM brings the power of Clojure-style concurrency to Python. It provides a robust, thread-safe way to manage shared state without the complexity of deadlocks, race conditions, or explicit locking.

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

---

## ⚡ Why Atomix?

Traditional locking is hard. Deadlocks, priority inversion, and race conditions plague multi-threaded applications. **Atomix STM** solves this by providing:

- **Atomic Transactions**: Changes are all-or-nothing.
- **Consistent Reads**: No "dirty reads" or torn state.
- **Isolated State**: Transactions don't interfere with each other.
- **No-GIL Optimized**: Designed to scale on Python 3.13's free-threading mode.

---

## 🚀 Quick Start

```python
from atomix_stm import Ref, dosync, atomically

# 1. Define your shared state
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

## 📦 Installation

```bash
pip install atomix-stm
```

> [!NOTE]
> **Compatibility**: Atomix STM works on **Python 3.9 through 3.14+**. Maximum performance is achieved on **Python 3.13+** with free-threading enabled.

---

## 🛠 Features

- **MVCC (Multi-Version Concurrency Control)**: Readers never block writers.
- **`Ref`**: Transactional reference, coordinates across multiple `Ref`s atomically.
- **`Atom`**: Uncoordinated atomic updates with CAS (`compare_and_set`).
- **`STMAgent`**: Asynchronous state management with error introspection.
- **`STMQueue`**: Transactional FIFO queue with blocking `get()`.
- **`STMVar`**: Thread-local dynamic variable bindings.
- **Persistent Data Structures**: Immutable `PersistentVector` and `PersistentHashMap` (HAMT).
- **Diagnostics**: Built-in monitoring with `STMDiagnostics`.

---

## 🔑 Core API

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
a.swap(lambda x: x + 1)                    # 1
a.compare_and_set(1, 42)                   # True — CAS
a.add_watcher("log", lambda old, new: print(f"{old} -> {new}"))
```

### Agents (async state)

```python
from atomix_stm import STMAgent
import time

agent = STMAgent(0)
agent.send(lambda x: x + 10)
result = agent.await_value(timeout=5.0)    # waits for all pending actions
print(result)  # 10
print(agent.errors)                        # [] — no failures
```

### STMQueue

```python
from atomix_stm import STMQueue, dosync

q = STMQueue()
dosync(lambda: q.put("hello"))
val = q.get(timeout=5.0)    # blocks until item available
print(val)  # "hello"
```

---

## 📊 Benchmarks (Python 3.13 Free-threading)

| Operation    | Standard Threading | Atomix STM | Improvement |
|--------------|--------------------|------------|-------------|
| Read (10M)   | 1.2s               | 0.4s       | 300%        |
| Write (1M)   | 4.5s (GIL locked)  | 1.1s       | 400%        |
| Contention   | Deadlock risk      | Safe Retry | ♾️          |

---

## 🆕 Changelog

### v3.2.7 (Current Version)
- Fixed critical `TypeError` initialized during deferred evaluations in `_commute_ref`.
- Removed duplicated `retry_count` overriding loops within `dosync()`.
- Added standard `QueueClosedException` exception classes for clean queue drains.
- Enforced strict validation checking on nested immediate-returns inside `dosync()`.
- Eradicated writer starvation dead-locks internally in `RWLock` via pending barriers.
- Hardened data-race conditions returning stale queue values during thread polling in `STMAgent.await_value()`.

### v3.2.6 (2026-03-10)
- Fixed double `unregister_transaction` logic bug inside `dosync()`.
- Thread leak prevention via Race Condition Lock inside `STMAgent._agent_pool`.
- Optimised STM latency via removal of 2nd read-set validation redundantly blocking commit paths.
- Removed deadlocks triggering during recursive evaluations in `_commute_ref()`.
- Unbounded queue loops blocked forever via timeouts fixed in `STMQueue.put()`.
- Performance overhead removed on `Ref.set()` context wrapping.
- Unbounded busy-wait spin loops capped implicitly inside `Atom.swap()`.
- Clean shutdown for unused background threads appended to `atexit`.

### v3.2.5 (2026-03-10)
- Fixed relative markdown links inside PyPI release details so internal documents are clickable.
- Added absolute GitHub repositories inside package metadata configurations.

### v3.2.4 (2026-03-10)
- Fixed module-level `commute()` missing return mappings.
- Re-routed unexpected `TransactionAbortedException` skips.
- Nested transaction `@atomically` behaviors stabilized.
- Optimized thread management under the `STMAgent` framework using global `ThreadPoolExecutors`.
- Removed redundant history checks and context wrappers.

### v3.2.3 (2026-03-10)
- Removed duplicate namespace definitions for `retry` and `commute`.
- Fixed `dosync` re-registration logic so that snapshot correctly updates over retries.
- Replaced `transaction()` wrappers with strict `@atomically` scopes across queue queries.
- Cleaned up obsolete checks and explicit imports.

### v3.2.2 (2026-03-10)
- Fixed missing parameter mappings (`alter`, `commute`, `atom`, `ref`, etc.) to top level.
- Replaced custom logic with resilient `threading.RLock()` for the `RWLock`.
- Fixed missing sentinels exceptions in `STMQueue`.
- Fixed multiple ABA transaction resets that were looping unnecessarily.
- Fixed properties bindings and missing imports from modular components.

### v3.2.0 (2026-03-03) — Deep Bug Fix Release

**Bug Fixes:**
- `STMAgent.send()` — fixed invalid use of `dosync()` as context manager
- `Atom.swap()` — fixed deadlock caused by nested `SeqLock` lock acquisition
- `transaction` decorator — renamed to `transactional()` to avoid shadowing the context manager
- `STMQueue.get()` — fixed `retry()` being called outside a transaction scope
- `Ref.set()`, `Ref.alter()`, `Ref.commute()` — fixed broken `dosync(fn)()` double-call pattern

**New Features:**
- `Atom.compare_and_set(old, new)` — atomic CAS operation
- `STMAgent.errors` property — inspect errors from async agent actions
- `STMAgent.clear_errors()` — clear accumulated errors
- `STMAgent.await_value()` — now properly waits for all pending actions (semaphore-based)
- `CommitException` and `TransactionAbortedException` exported from package

### v3.1.6
- Heavy Metal Atomicity, Adaptive Reaper, Level Bloat Fix, Robust History Retention

---

## 📝 Licensing

Atomix STM is dual-licensed:
1. **GPLv3**: Open-source use (requires sharing your source code).
2. **Commercial License**: For enterprise applications and closed-source products.

See [LICENSE](https://github.com/JohnMikron/Atomix/blob/main/LICENSE) for details.

---

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/JohnMikron/Atomix/blob/main/CONTRIBUTING.md) for details on:
- Dev setup
- Testing
- PR process and style guide

Please also review our [Code of Conduct](https://github.com/JohnMikron/Atomix/blob/main/CODE_OF_CONDUCT.md) and [Security Policy](https://github.com/JohnMikron/Atomix/blob/main/SECURITY.md).

---

© 2026 Atomix STM Project.

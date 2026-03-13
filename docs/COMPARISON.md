# Concurrency Models: Why STM?

Choosing the right concurrency model for your Python application is critical. This guide compares **Atomix STM** with common alternatives: **Locks**, **Asyncio**, and the **Actor Model**.

## 1. STM vs. Locks (threading.Lock)

Locks are the default tool for multi-threaded Python, but they suffer from significant scalability and usability issues.

| Feature | Locks (Pessimistic) | Atomix STM (Optimistic) |
| :--- | :--- | :--- |
| **Philosophy** | "Nobody enter until I'm done." | "Try change, retry if conflict." |
| **Deadlocks** | High risk (lock ordering issues). | **Impossible**. |
| **Composability** | Impossible (locks leak abstraction). | **Native** (transactions compose). |
| **Throughput** | Lower (blocking threads). | **Higher** (especially on No-GIL). |
| **Ease of Use** | Error-prone (forgetting 1 lock). | Intent-based (wrap in transaction). |

**Scenario**: Transferring money between accounts. With locks, you must ensure you always lock Account A and Account B in a fixed order (e.g., by ID) to avoid deadlocks. With Atomix, you just wrap the logic in `dosync()`.

## 2. STM vs. Asyncio

Asyncio is great for I/O-bound tasks but doesn't solve data race issues for shared state in high-concurrency environments, especially with the introduction of Free-Threading (No-GIL).

| Feature | Asyncio | Atomix STM |
| :--- | :--- | :--- |
| **Parallelism** | Single-threaded (event loop). | **True Multi-threading** (No-GIL). |
| **State Protection** | Cooperative (yield points). | Transactional (Atoms). |
| **CPU-Bound**| Blocks the event loop. | Scales across cores. |
| **Complexity** | "Colored functions" (async/await). | Standard Python functions. |

**When to use which?**
- Use **Asyncio** for massive numbers of network connections where most time is spent waiting.
- Use **Atomix STM** for CPU-intensive shared-state coordination and high-frequency data updates.

## 3. STM vs. Actor Model (e.g., Ray, Pykka)

Actors encapsulate state and communicate via message passing.

| Feature | Actor Model | Atomix STM |
| :--- | :--- | :--- |
| **Data Locality** | State is private to actor. | State is shared (Atoms). |
| **Overhead** | High (message serialization). | **Extremely Low** (memory pointers). |
| **Consistency** | Eventual / Message-order. | **Strictly Atomic**. |
| **Complexity** | Distributed system mindset. | Shared memory mindset. |

**Conclusion**: Actors are better for distributed systems. Atomix STM is superior for high-performance, single-machine shared state management where low latency is critical.

---

## The Verdict

**Atomix STM** is the "sweet spot" for modern Python 3.13+ development:
- It provides the **strict consistency** of locks without the deadlocks.
- It provides the **parallelism** of No-GIL without the complexity of raw memory management.
- It provides the **composability** that makes building complex systems manageable.

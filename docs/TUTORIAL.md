# Atomix STM: The Definitive Tutorial

Welcome to Atomix, a production-grade Software Transactional Memory (STM) implementation for Python 3.13+. This guide will walk you through the core concepts and modern patterns for using STM effectively.

## Why STM?

In traditional concurrent programming, we use **Locks**. However, locks are:
1. **Pessimistic**: They assume conflict will happen.
2. **Error-prone**: Forget one lock and you have a race condition. Grab them in the wrong order and you have a deadlock.
3. **Non-composable**: You cannot easily combine two thread-safe functions into a third thread-safe function without exposing their internal locking logic.

**Atomix STM** is **Optimistic**. It allows multiple threads to attempt changes in isolation. If they conflict, the transaction automatically retries. It is **composable**, **deadlock-free**, and leverages Python 3.13's No-GIL capabilities.

---

## 1. The Core Concept: Atoms

An `Atom` is a container for a shared value. You don't update it directly; you update it within a transaction.

```python
from atomix_stm import Atom, dosync

# Initialize shared state
balance = Atom(1000)

def deposit(amount):
    def tx():
        current = balance.deref()  # Get current value
        balance.reset(current + amount)  # Propose new value
    dosync(tx)

deposit(500)
print(f"New Balance: {balance.deref()}") # 1500
```

## 2. Transactions and Composition

This is where STM shines. Let's transfer money between two accounts.

```python
from atomix_stm import Atom, dosync

acc1 = Atom(1000)
acc2 = Atom(500)

def transfer(from_acc, to_acc, amount):
    def tx():
        v1 = from_acc.deref()
        if v1 < amount:
            raise ValueError("Insufficient funds")
        
        v2 = to_acc.deref()
        
        from_acc.reset(v1 - amount)
        to_acc.reset(v2 + amount)
    
    # This whole block is ATOMIC. 
    # Either both balances update, or neither does.
    dosync(tx)

transfer(acc1, acc2, 200)
```

## 3. Atomic Updates with `swap`

For simple updates, `swap` is a shorthand for `deref` -> `calculate` -> `reset`.

```python
counter = Atom(0)

# Atomic increment
counter.swap(lambda x: x + 1)
```

## 4. Coordination with `retry`

STM allows for elegant coordination. Suppose you want to wait until a condition is met.

```python
from atomix_stm import Atom, dosync, retry

queue = Atom([])

def produce(item):
    queue.swap(lambda q: q + [item])

def consume():
    def tx():
        q = queue.deref()
        if not q:
            # This transaction will block until 'queue' is modified
            # by another thread, then it will automatically retry.
            retry() 
        
        item = q[0]
        queue.reset(q[1:])
        return item
    
    return dosync(tx)
```

## 5. Invariants and Validation

You can attach validators to Atoms to ensure they never enter an invalid state.

```python
def must_be_positive(val):
    if val < 0:
        raise ValueError("Must be positive")
    return True

balance = Atom(100, validator=must_be_positive)

# This will raise ValueError and roll back the transaction
# balance.swap(lambda x: x - 200) 
```

---

## Best Practices

1. **Keep Transactions Short**: The longer a transaction, the higher the chance of conflict and retries.
2. **Side-Effect Free**: Never perform I/O (printing, network calls, DB writes) inside a transaction. Transactions may run multiple times due to retries.
3. **Prefer `swap`**: It's more concise and less prone to "read-then-write" errors than manual `deref` followed by `reset`.

## Real-world Architectures

For more advanced examples, check out:
- [FastAPI Integration](../examples/web_frameworks/fastapi_app.py)
- [Instrumentation hooks](../examples/ecosystem/monitoring.py)

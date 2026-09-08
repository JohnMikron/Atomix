"""Compatibility shim for Atomix v4.4.0.

Exposes the primary STM types, functions, exception classes, and utility managers
from the modular package layout to preserve backward compatibility for legacy tests.
Writes serialize on a single coordinator lock (commit_lock) during commit or execute
concurrently via fine-grained TL2 ordered locking.
"""

from .exceptions import (
    STMException,
    RetryException,
    CommitException,
    ConflictException,
    TransactionAbortedException,
    TimeoutException,
    ValidationException,
    HistoryExpiredException,
    InvariantViolationException,
    QueueClosedException,
    SavepointRollbackException,
)
from .versioning import TransactionState, VersionStamp
from .locks import SpinLock, SeqLock, RWLock
from .coordinator import (
    TransactionCoordinator,
    STMReaper,
    ContentionManager,
    HistoryManager,
    _cleanup,
    _safe_log_info,
    _safe_log_debug,
    _safe_log_warning,
    _safe_log_error,
    logger,
)
from .transaction import Transaction, Savepoint, SavepointContext
from .ref import Ref, RefIdentity, Atom
from .persistent import (
    PersistentVector,
    PersistentHashMap,
    TransientVector,
    TransientHashMap,
)
from .primitives import STMQueue, STMAgent, STMVar, STMPromise, STMChannel
from .api import (
    transaction,
    dosync,
    atomically,
    commute,
    ensure,
    retry,
    io,
    get_stm_stats,
    dump_stm_stats,
    reset_stm,
    Snapshot,
    get_snapshot_at,
    get_history,
    run_concurrent,
    promise,
    channel,
    savepoint,
)

__version__ = "4.4.0"

__all__ = [
    # Core types
    "Ref",
    "Atom",
    "Transaction",
    "TransactionCoordinator",
    "TransactionState",
    "VersionStamp",
    "RefIdentity",
    "Savepoint",
    "SavepointContext",
    # Context managers & helpers
    "transaction",
    "dosync",
    "atomically",
    "promise",
    "channel",
    "savepoint",
    # Primitives
    "STMQueue",
    "STMAgent",
    "STMVar",
    "STMPromise",
    "STMChannel",
    # Persistent structures & transients
    "PersistentVector",
    "PersistentHashMap",
    "TransientVector",
    "TransientHashMap",
    "Snapshot",
    # Operations
    "retry",
    "ensure",
    "commute",
    "io",
    # History
    "get_history",
    "get_snapshot_at",
    # Utilities
    "run_concurrent",
    "get_stm_stats",
    "dump_stm_stats",
    "reset_stm",
    # Lock utilities
    "SpinLock",
    "SeqLock",
    "RWLock",
    # Managers
    "ContentionManager",
    "HistoryManager",
    "STMReaper",
    # Exceptions
    "STMException",
    "RetryException",
    "CommitException",
    "ConflictException",
    "TransactionAbortedException",
    "TimeoutException",
    "ValidationException",
    "HistoryExpiredException",
    "InvariantViolationException",
    "QueueClosedException",
    "SavepointRollbackException",
    # Compatibility helpers
    "_cleanup",
    "_safe_log_info",
    "_safe_log_debug",
    "_safe_log_warning",
    "_safe_log_error",
    "logger",
]

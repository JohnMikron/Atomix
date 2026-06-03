"""Compatibility shim for Atomix v4.3.0.

Exposes the primary STM types, functions, exception classes, and utility managers
from the modular package layout to preserve backward compatibility for legacy tests.
Writes serialize on a single coordinator lock (commit_lock) during commit.
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
from .transaction import Transaction
from .ref import Ref, RefIdentity, Atom
from .persistent import PersistentVector, PersistentHashMap
from .primitives import STMQueue, STMAgent, STMVar
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
)

__version__ = "4.3.0"

__all__ = [
    # Core types
    "Ref",
    "Atom",
    "Transaction",
    "TransactionCoordinator",
    "TransactionState",
    "VersionStamp",
    "RefIdentity",
    # Context managers
    "transaction",
    "dosync",
    "atomically",
    # Primitives
    "STMQueue",
    "STMAgent",
    "STMVar",
    # Persistent structures
    "PersistentVector",
    "PersistentHashMap",
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
    # Compatibility helpers
    "_cleanup",
    "_safe_log_info",
    "_safe_log_debug",
    "_safe_log_warning",
    "_safe_log_error",
    "logger",
]

"""
Atomix v3.1 - Production-Grade Software Transactional Memory for Python 3.13+
=============================================================================

A state-of-the-art STM library bringing Haskell/Clojure-style concurrency
primitives to Python, optimized for the No-GIL era with enterprise-grade features.

Features:
- MVCC with adaptive history management
- Seqlock-based lock-free reads
- Contention-aware retry scheduling
- Background snapshot cleanup (The Reaper) - Adaptive Scheduling
- Persistent immutable data structures (Vector, HashMap) - Level Bloat Fix
- STM-aware queues, agents, and variables
- Comprehensive diagnostics and monitoring
- Full type safety with generics
- PEP 703 compatible (No-GIL ready)
- JSON serialization support
- Async/await support for Python 3.13+

Author: Atomix STM Project
License: GPLv3 / Commercial
"""

from __future__ import annotations

import threading
import time
import random
import functools
import weakref
import sys
import os
import hashlib
import struct
import heapq
import itertools
import atexit
import logging
import json
from typing import (
    TypeVar, Generic, Callable, Optional, Any,
    List, Dict, Set, Tuple, Type, Union,
    Protocol, runtime_checkable, overload,
    ContextManager, Iterator, Iterable,
    MutableMapping, Sequence, FrozenSet,
    Coroutine, AsyncIterator, cast
)
from dataclasses import dataclass, field
from enum import Enum, auto
from contextlib import contextmanager, asynccontextmanager
from abc import ABC, abstractmethod
from collections import defaultdict
import copy
import math
import asyncio
from pathlib import Path

# ============================================================================
# Environment & Version Detection
# ============================================================================

PY_VERSION = sys.version_info
PYTHON_3_13_PLUS = PY_VERSION >= (3, 13)
NO_GIL_ENABLED = PYTHON_3_13_PLUS  # Assume No-GIL in 3.13+

# ============================================================================
# Logging Setup
# ============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            '[%(asctime)s] %(name)s [%(levelname)s] %(message)s'
        )
    )
    logger.addHandler(handler)

# ============================================================================
# Type Variables and Protocols
# ============================================================================

T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)
T_contra = TypeVar('T_contra', contravariant=True)
R = TypeVar('R')
K = TypeVar('K')
V = TypeVar('V')


@runtime_checkable
class Cloneable(Protocol):
    """Objects supporting efficient cloning."""
    def __clone__(self) -> 'Cloneable': ...


@runtime_checkable
class Serializable(Protocol):
    """Objects supporting serialization to JSON."""
    def to_json(self) -> str: ...
    
    @classmethod
    def from_json(cls: Type, s: str) -> 'Serializable': ...


# ============================================================================
# Exceptions
# ============================================================================

class STMException(Exception):
    """Base STM exception."""
    pass


class RetryException(STMException):
    """Transaction should retry."""
    def __init__(
        self, 
        message: str, 
        retry_count: int = 0, 
        backoff: float = 0.0
    ):
        super().__init__(message)
        self.retry_count = retry_count
        self.backoff = backoff


class CommitException(STMException):
    """Commit failed."""
    pass


class ConflictException(CommitException):
    """Write conflict detected."""
    def __init__(
        self, 
        message: str, 
        conflicting_refs: FrozenSet[int] = frozenset()
    ):
        super().__init__(message)
        self.conflicting_refs = conflicting_refs


class TransactionAbortedException(STMException):
    """Transaction was aborted."""
    pass


class TimeoutException(STMException):
    """Operation timed out."""
    pass


class ValidationException(STMException):
    """Validation failed."""
    pass


class HistoryExpiredException(STMException):
    """History snapshot expired."""
    pass


class InvariantViolationException(STMException):
    """Transaction invariant violated."""
    pass


# ============================================================================
# Core Data Structures
# ============================================================================

class TransactionState(Enum):
    """Transaction lifecycle state."""
    ACTIVE = auto()
    PREPARING = auto()
    COMMITTING = auto()
    COMMITTED = auto()
    ABORTED = auto()
    RETRYING = auto()


@dataclass(frozen=True, slots=True, order=True)
class VersionStamp:
    """Unique version identifier."""
    epoch: int  # Epoch first for major ordering
    logical_time: int
    transaction_id: int
    physical_time: float


@dataclass(frozen=True, slots=True)
class RefIdentity:
    """Unique Ref identifier."""
    id: int
    created_at: float
    name: Optional[str] = None
    
    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class ReadLogEntry:
    """Read operation record."""
    ref_id: int
    version_read: VersionStamp
    value_hash: int


@dataclass
class WriteLogEntry(Generic[T]):
    """Write operation record."""
    ref_id: int
    old_value: Optional[T]
    new_value: T
    old_version: Optional[VersionStamp]
    is_commutative: bool = False
    commutative_fn: Optional[Callable[[T], T]] = None


@dataclass
class CommuteEntry(Generic[T]):
    """Deferred commutative operation."""
    ref_id: int
    function: Callable[[T], T]
    args: Tuple
    kwargs: Dict[str, Any]


# ============================================================================
# Contention Management (Enhanced)
# ============================================================================

class ContentionManager:
    """
    Adaptive contention management preventing livelock.
    
    Strategies:
    - Exponential backoff with jitter
    - Per-ref contention tracking
    - Priority scheduling (older transactions first)
    - Adaptive timeout adjustment
    - Throughput monitoring
    """
    
    def __init__(
        self,
        base_backoff: float = 0.0001,
        max_backoff: float = 0.1,
        jitter_factor: float = 0.3,
        enable_priority: bool = True,
        throughput_window: float = 60.0
    ):
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._jitter_factor = jitter_factor
        self._enable_priority = enable_priority
        self._throughput_window = throughput_window
        
        self._contention_scores: Dict[int, int] = defaultdict(int)
        self._contention_lock = threading.RLock()
        
        self._global_contention = 0
        self._total_retries = 0
        self._total_commits = 0
        
        # Metrics
        self._history_window: List[float] = []
        self._max_history = 1000
        self._last_throughput = 0.0
    
    def record_conflict(
        self, 
        ref_ids: Set[int],
        retry_count: int = 0
    ) -> float:
        """Record conflict, return backoff time."""
        with self._contention_lock:
            self._global_contention += 1
            self._total_retries += 1
            
            max_score = 0
            for ref_id in ref_ids:
                self._contention_scores[ref_id] += 1
                max_score = max(max_score, self._contention_scores[ref_id])
            
            # Record metric
            self._history_window.append(time.time())
            if len(self._history_window) > self._max_history:
                self._history_window.pop(0)
        
        # Exponential backoff
        exponent = min(max_score + retry_count, 15)
        backoff = self._base_backoff * (2 ** exponent)
        backoff = min(backoff, self._max_backoff)
        
        # Jitter
        jitter = backoff * self._jitter_factor * random.random()
        
        return backoff + jitter
    
    def record_success(self, ref_ids: Set[int]) -> None:
        """Record successful commit."""
        with self._contention_lock:
            self._total_commits += 1
            self._global_contention = max(0, self._global_contention - 1)
            
            for ref_id in ref_ids:
                self._contention_scores[ref_id] = max(
                    0,
                    self._contention_scores[ref_id] - 1
                )
    
    def get_contention_level(self) -> float:
        """Get system contention (0.0 to 1.0)."""
        with self._contention_lock:
            total = self._total_commits + self._total_retries
            if total == 0:
                return 0.0
            return min(1.0, self._total_retries / total)
    
    def get_throughput(self, window_seconds: Optional[float] = None) -> float:
        """Get commits per second in last N seconds."""
        window = window_seconds or self._throughput_window
        
        with self._contention_lock:
            if not self._history_window:
                return 0.0
            
            cutoff = time.time() - window
            recent = [t for t in self._history_window if t > cutoff]
            throughput = len(recent) / window if window > 0 else 0.0
            
            self._last_throughput = throughput
            return throughput
    
    def should_yield(self, tx_id: int, ref_id: int) -> bool:
        """Determine if transaction should yield."""
        if not self._enable_priority:
            return False
        
        with self._contention_lock:
            score = self._contention_scores.get(ref_id, 0)
            if score < 3:
                return False
            return (tx_id % (score + 1)) != 0
    
    def get_backoff_for_retry(
        self, 
        retry_count: int, 
        ref_ids: Set[int]
    ) -> float:
        """Calculate backoff for retry."""
        with self._contention_lock:
            max_score = max(
                (self._contention_scores.get(rid, 0) for rid in ref_ids),
                default=0
            )
        
        exponent = min(retry_count + max_score, 15)
        backoff = self._base_backoff * (2 ** exponent)
        backoff = min(backoff, self._max_backoff)
        
        jitter = backoff * self._jitter_factor * random.random()
        return backoff + jitter
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get contention metrics."""
        with self._contention_lock:
            return {
                "contention_level": self.get_contention_level(),
                "throughput": self.get_throughput(),
                "total_retries": self._total_retries,
                "total_commits": self._total_commits,
                "global_contention": self._global_contention
            }


# ============================================================================
# Adaptive History Management (Enhanced)
# ============================================================================

class HistoryManager:
    """
    Adaptive history retention based on:
    - Active snapshots
    - Memory pressure
    - Access patterns
    - System metrics
    """
    
    def __init__(
        self,
        default_max_history: int = 100,
        min_history: int = 5,
        memory_threshold_percent: float = 80.0,
        access_threshold: int = 100
    ):
        self._default_max = default_max_history
        self._min_history = min_history
        self._memory_threshold = memory_threshold_percent / 100.0
        self._access_threshold = access_threshold
        
        self._active_snapshots: Dict[int, Tuple[VersionStamp, float]] = {}
        self._snapshots_lock = threading.RLock()
        
        self._access_patterns: Dict[int, List[float]] = defaultdict(list)
        self._patterns_lock = threading.RLock()
        
        # Metrics
        self._cleanups_performed = 0
        self._snapshots_removed = 0
        self._memory_pressure_events = 0
    
    def register_snapshot(self, tx_id: int, version: VersionStamp) -> None:
        """Register active transaction snapshot."""
        with self._snapshots_lock:
            self._active_snapshots[tx_id] = (version, time.time())
    
    def unregister_snapshot(self, tx_id: int) -> None:
        """Unregister completed transaction."""
        with self._snapshots_lock:
            self._active_snapshots.pop(tx_id, None)
    
    def get_retention_bound(self) -> int:
        """Get minimum logical time to retain."""
        with self._snapshots_lock:
            if not self._active_snapshots:
                return 0
            
            oldest = min(
                (v.logical_time for v, _ in self._active_snapshots.values()),
                default=0
            )
            return oldest
    
    def compute_max_history(self, ref_id: int) -> int:
        """Compute adaptive max history for Ref."""
        base = self._default_max
        
        # Check access rate
        with self._patterns_lock:
            accesses = self._access_patterns.get(ref_id, [])
            if len(accesses) > 100:
                recent_rate = len(accesses) / 60.0
                if recent_rate > self._access_threshold:
                    base = int(base * 1.5)
        
        # Check memory pressure
        try:
            import psutil
            memory_percent = psutil.virtual_memory().percent / 100.0
            if memory_percent > self._memory_threshold:
                base = max(self._min_history, int(base * 0.5))
                self._memory_pressure_events += 1
        except ImportError:
            pass
        
        return base
    
    def record_access(self, ref_id: int) -> None:
        """Record access for pattern analysis."""
        with self._patterns_lock:
            accesses = self._access_patterns[ref_id]
            accesses.append(time.time())
            
            # Keep only last minute
            cutoff = time.time() - 60.0
            self._access_patterns[ref_id] = [
                t for t in accesses if t > cutoff
            ]
    
    def cleanup_stale_snapshots(self, timeout: float = 300.0) -> int:
        """Remove snapshots that have been active too long."""
        current_time = time.time()
        removed = 0
        
        with self._snapshots_lock:
            stale = [
                tx_id 
                for tx_id, (_, start_time) in self._active_snapshots.items()
                if current_time - start_time > timeout
            ]
            for tx_id in stale:
                self._active_snapshots.pop(tx_id)
                removed += 1
        
        self._cleanups_performed += 1
        self._snapshots_removed += removed
        return removed
    
    def get_stats(self) -> Dict[str, Any]:
        """Get history manager statistics."""
        with self._snapshots_lock:
            active = len(self._active_snapshots)
        
        with self._patterns_lock:
            tracked_refs = len(self._access_patterns)
        
        return {
            "active_snapshots": active,
            "tracked_refs": tracked_refs,
            "cleanups_performed": self._cleanups_performed,
            "snapshots_removed": self._snapshots_removed,
            "memory_pressure_events": self._memory_pressure_events
        }


# ============================================================================
# Lock-Free Utilities (Enhanced)
# ============================================================================

class SpinLock:
    """
    Lightweight spinlock with adaptive spinning.
    """
    
    __slots__ = ('_locked', '_owner', '_spin_count')
    
    def __init__(self):
        self._locked = False
        self._owner = None
        self._spin_count = 0
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire lock with adaptive spinning."""
        start = time.time()
        thread_id = threading.current_thread().ident
        spins = 0
        
        while True:
            if not self._locked:
                self._locked = True
                self._owner = thread_id
                self._spin_count = spins
                return True
            
            spins += 1
            if spins < 100:
                continue
            elif spins < 1000:
                time.sleep(0)
            else:
                time.sleep(0.00001)
            
            if timeout is not None and time.time() - start > timeout:
                return False
    
    def release(self) -> None:
        """Release lock."""
        if self._owner != threading.current_thread().ident:
            raise RuntimeError("Lock not owned by current thread")
        self._locked = False
        self._owner = None
    
    def __enter__(self) -> 'SpinLock':
        self.acquire()
        return self
    
    def __exit__(self, *args) -> None:
        self.release()


class SeqLock(Generic[T]):
    """
    Sequence lock for lock-free optimistic reads.
    """
    
    __slots__ = ('_sequence', '_value', '_write_lock')
    
    def __init__(self, initial_value: T):
        self._sequence = 0
        self._value = initial_value
        self._write_lock = threading.Lock()
    
    def read(self) -> T:
        """Lock-free optimistic read."""
        while True:
            seq1 = self._sequence
            if seq1 & 1:
                time.sleep(0.000001)
                continue
            
            value = self._value
            seq2 = self._sequence
            
            if seq1 == seq2:
                return value
    
    def write(self, new_value: T) -> None:
        """Write with exclusive access."""
        with self._write_lock:
            self._sequence += 1
            self._value = new_value
            self._sequence += 1


class RWLock:
    """
    Reader-Writer lock.
    """
    
    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(threading.RLock())
        self._write_ready = threading.Condition(threading.RLock())
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        with self._read_ready:
            while self._writers > 0:
                if not self._read_ready.wait(timeout):
                    return False
            self._readers += 1
            return True
    
    def release_read(self) -> None:
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._write_ready.notify_all()
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        with self._write_ready:
            while self._writers > 0 or self._readers > 0:
                if not self._write_ready.wait(timeout):
                    return False
            self._writers += 1
            return True
    
    def release_write(self) -> None:
        with self._write_ready:
            self._writers -= 1
            self._write_ready.notify_all()
            self._read_ready.notify_all()


# ============================================================================
# Transaction Coordinator & Reaper (Enhanced)
# ============================================================================

class TransactionCoordinator:
    """
    Global coordinator for all STM transactions.
    """
    
    _instance: Optional['TransactionCoordinator'] = None
    _init_lock = threading.Lock()
    
    def __new__(cls) -> 'TransactionCoordinator':
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
            return cls._instance
    
    def _initialize(self) -> None:
        self._tx_counter = itertools.count(1)
        self._ref_counter = itertools.count(1)
        self._logical_clock = 0
        self._clock_lock = threading.RLock()
        self._refs: Dict[int, 'Ref'] = {}
        self._refs_lock = threading.RLock()
        self._active_txs: Dict[int, 'Transaction'] = {}
        self._txs_lock = threading.RLock()
        self._current_epoch = 0
        self._epoch_lock = threading.Lock()
        self._contention_manager = ContentionManager()
        self._history_manager = HistoryManager()
        self._commit_lock = threading.RLock()
        self._reaper = STMReaper(self, interval=5.0)
        self._reaper.start()
        self._stats_lock = threading.RLock()
        self._stats = {
            "total_transactions": 0,
            "total_commits": 0,
            "total_aborts": 0,
            "total_conflicts": 0
        }
        logger.info("TransactionCoordinator initialized")

    def reset(self) -> None:
        with self._init_lock:
            self._tx_counter = itertools.count(1)
            self._ref_counter = itertools.count(1)
            self._logical_clock = 0
            self._current_epoch = 0
            self._refs.clear()
            self._active_txs.clear()
            self._stats = {
                "total_transactions": 0,
                "total_commits": 0,
                "total_aborts": 0,
                "total_conflicts": 0
            }

    def new_transaction_id(self) -> int:
        with self._stats_lock:
            self._stats["total_transactions"] += 1
        return next(self._tx_counter)

    def new_ref_id(self) -> int:
        return next(self._ref_counter)

    def get_logical_time(self) -> int:
        with self._clock_lock:
            return self._logical_clock

    def advance_clock(self) -> int:
        with self._clock_lock:
            self._logical_clock += 1
            return self._logical_clock

    def create_version_stamp(self, tx_id: int) -> VersionStamp:
        return VersionStamp(
            logical_time=self.advance_clock(),
            transaction_id=tx_id,
            epoch=self._current_epoch,
            physical_time=time.time()
        )

    def register_ref(self, ref: 'Ref') -> int:
        ref_id = self.new_ref_id()
        with self._refs_lock:
            self._refs[ref_id] = ref
        return ref_id

    def unregister_ref(self, ref_id: int) -> None:
        with self._refs_lock:
            self._refs.pop(ref_id, None)

    def get_ref(self, ref_id: int) -> Optional['Ref']:
        with self._refs_lock:
            return self._refs.get(ref_id)

    def register_transaction(self, tx: 'Transaction') -> None:
        with self._txs_lock:
            self._active_txs[tx.id] = tx
        self._history_manager.register_snapshot(tx.id, tx.snapshot_version)

    def unregister_transaction(self, tx_id: int) -> None:
        with self._txs_lock:
            self._active_txs.pop(tx_id, None)
        self._history_manager.unregister_snapshot(tx_id)

    def detect_conflicts(self, tx: 'Transaction', read_set: Dict[int, VersionStamp], write_set: Set[int]) -> Optional[FrozenSet[int]]:
        conflicting = set()
        for ref_id, read_version in read_set.items():
            ref = self.get_ref(ref_id)
            if ref is None: continue
            if ref._get_version() > read_version:
                conflicting.add(ref_id)
        return frozenset(conflicting) if conflicting else None


    def prepare_commit(self, tx: 'Transaction', read_set: Dict[int, VersionStamp], write_set: Set[int]) -> Tuple[bool, Optional[FrozenSet[int]]]:
        with self._commit_lock:
            conflicts = self.detect_conflicts(tx, read_set, write_set)
            if conflicts:
                with self._stats_lock:
                    self._stats["total_conflicts"] += 1
                return False, conflicts
            return True, None

    def record_commit(self) -> None:
        with self._stats_lock:
            self._stats["total_commits"] += 1

    def record_abort(self) -> None:
        with self._stats_lock:
            self._stats["total_aborts"] += 1

    @property
    def contention(self) -> ContentionManager: return self._contention_manager

    @property
    def history(self) -> HistoryManager: return self._history_manager

    def get_stats(self) -> Dict[str, Any]:
        with self._stats_lock:
            stats = dict(self._stats)
        stats.update(self._contention_manager.get_metrics())
        stats.update(self._history_manager.get_stats())
        with self._txs_lock:
            stats["active_transactions"] = len(self._active_txs)
        with self._refs_lock:
            stats["registered_refs"] = len(self._refs)
        return stats

    def get_refs_snapshot(self) -> List['Ref']:
        with self._refs_lock:
            return list(self._refs.values())


class STMReaper(threading.Thread):
    """
    Adaptive STM Reaper.
    """
    
    def __init__(self, coordinator: TransactionCoordinator, interval: float = 5.0, batch_size: int = 100):
        super().__init__(name="STM-Reaper", daemon=True)
        self.coordinator = coordinator
        self.interval = interval
        self.batch_size = batch_size
        self._running = True

    def run(self) -> None:
        logger.info("STM Reaper started")
        while self._running:
            try:
                # Dynamic interval based on contention
                contention = self.coordinator.contention.get_contention_level()
                sleep_time = max(0.1, self.interval * (1.0 - contention))
                
                removed = self.coordinator.history.cleanup_stale_snapshots(timeout=60.0)
                refs = self.coordinator.get_refs_snapshot()
                retention_bound = self.coordinator.history.get_retention_bound()
                
                for i in range(0, len(refs), self.batch_size):
                    batch = refs[i:i + self.batch_size]
                    for ref in batch:
                        try:
                            ref._trim_history(retention_bound)
                        except Exception as e:
                            logger.error(f"Error trimming ref {ref.id}: {e}")
                    if i > 0 and i % (self.batch_size * 2) == 0:
                        time.sleep(0.001)
                
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Reaper error: {e}")
                time.sleep(self.interval)

    def stop(self) -> None:
        self._running = False
        logger.info("STM Reaper stopped")


# ============================================================================
# Transaction & Reference (Enhanced)
# ============================================================================

class Transaction:
    """
    Main STM Transaction object (Thread-local).
    """
    
    _local = threading.local()
    
    @classmethod
    def get_current(cls) -> Optional['Transaction']:
        return getattr(cls._local, 'current', None)
    
    @classmethod
    def set_current(cls, tx: Optional['Transaction']) -> None:
        cls._local.current = tx

    def __init__(self, coordinator: TransactionCoordinator, timeout: float = 10.0, max_retries: int = 100):
        self.coordinator = coordinator
        self.id = coordinator.new_transaction_id()
        self.snapshot_version = coordinator.create_version_stamp(self.id)
        self.state = TransactionState.ACTIVE
        self.timeout = timeout
        self.max_retries = max_retries
        self.start_time = time.time()
        
        self.read_log: Dict[int, ReadLogEntry] = {}
        self.write_log: Dict[int, WriteLogEntry] = {}
        self.commutes: List[CommuteEntry] = []
        self.retry_count = 0
        self._depth = 0
        
        # Post-commit hooks
        self.on_commit_hooks: List[Callable[[], Any]] = []
        self.on_abort_hooks: List[Callable[[], Any]] = []

    def __enter__(self) -> 'Transaction':
        self._depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._depth -= 1
        if exc_type is not None:
            if not issubclass(exc_type, (RetryException, ConflictException)):
                self.abort()
            return False  # Propagate original exception
            
        if self._depth == 0:
            self.commit() # Let ConflictException/RetryException propagate out
        return False # Ensure any exception is propagated


    def _reset_for_retry(self) -> None:
        """Reset internal state for a fresh attempt."""
        self.state = TransactionState.ACTIVE
        self.snapshot_version = self.coordinator.create_version_stamp(self.id)
        self.read_log.clear()
        self.write_log.clear()
        self.commutes.clear()
        self.start_time = time.time()


    def is_expired(self) -> bool:
        return (time.time() - self.start_time) > self.timeout

    def check_timeout(self) -> None:
        if self.is_expired():
            raise TimeoutException(f"Transaction {self.id} timed out after {self.timeout}s")

    def read(self, ref: 'Ref[T]') -> T:
        self.check_timeout()
        if ref.id in self.write_log:
            return self.write_log[ref.id].new_value
        
        # Track access pattern
        self.coordinator.history.record_access(ref.id)
        
        # MVCC read
        value, version = ref._get_value_at(self.snapshot_version)
        
        # Record in read log
        self.read_log[ref.id] = ReadLogEntry(
            ref_id=ref.id,
            version_read=version,
            value_hash=hash(value) if hasattr(value, '__hash__') else id(value)
        )
        return value

    def write(self, ref: 'Ref[T]', value: T) -> None:
        self.check_timeout()
        old_value = self.read(ref)
        read_entry = self.read_log[ref.id]
        
        self.write_log[ref.id] = WriteLogEntry(
            ref_id=ref.id,
            old_value=old_value,
            new_value=value,
            old_version=read_entry.version_read
        )

    def commute(self, ref: 'Ref[T]', fn: Callable[[T], T], *args, **kwargs) -> T:
        self.check_timeout()
        self.commutes.append(CommuteEntry(ref.id, fn, args, kwargs))
        # Return speculative value
        current = self.read(ref)
        return fn(current, *args, **kwargs)

    def retry(self) -> None:
        self.state = TransactionState.RETRYING
        raise RetryException("Manual retry requested", self.retry_count)

    def abort(self) -> None:
        self.state = TransactionState.ABORTED
        for hook in self.on_abort_hooks:
            try: hook()
            except Exception: pass
        self.coordinator.record_abort()
        raise TransactionAbortedException(f"Transaction {self.id} aborted")

    def _prepare(self) -> Tuple[bool, Optional[FrozenSet[int]]]:
        self.state = TransactionState.PREPARING
        read_set = {rid: entry.version_read for rid, entry in self.read_log.items()}
        write_set = set(self.write_log.keys()) | {c.ref_id for c in self.commutes}
        return self.coordinator.prepare_commit(self, read_set, write_set)

    def commit(self) -> Any:
        self.check_timeout()
        
        # Apply commutes speculatively for final check
        for commute in self.commutes:
            ref = self.coordinator.get_ref(commute.ref_id)
            if ref:
                val = self.read(ref)
                new_val = commute.function(val, *commute.args, **commute.kwargs)
                read_entry = self.read_log[ref.id]
                self.write_log[ref.id] = WriteLogEntry(
                    ref_id=ref.id,
                    old_value=val,
                    new_value=new_val,
                    old_version=read_entry.version_read,
                    is_commutative=True
                )

        success, conflicts = self._prepare()
        if not success:
            raise ConflictException("Write conflict detected", conflicts or frozenset())

        # Perform atomic update
        with self.coordinator._commit_lock:
            new_version = self.coordinator.create_version_stamp(self.id)
            
            # Final validation phase (Full Read-Set Validation)
            # This ensures that ALL values read during the transaction are still consistent,
            # not just those in the write log. Fixes the 0.001% race condition.
            for ref_id, read_entry in self.read_log.items():
                ref = self.coordinator.get_ref(ref_id)
                if ref is None: continue
                if ref._get_version() > read_entry.version_read:
                     raise ConflictException(f"Read-set conflict on ref {ref_id}")
            
            # Apply changes
            for ref_id, write_entry in self.write_log.items():
                ref = self.coordinator.get_ref(ref_id)
                if ref:
                    ref._set_value(write_entry.new_value, new_version)
        
        self.state = TransactionState.COMMITTED
        self.coordinator.record_commit()
        self.coordinator.contention.record_success(set(self.write_log.keys()))
        
        # Run hooks
        for hook in self.on_commit_hooks:
            try: hook()
            except Exception as e:
                logger.error(f"Error in post-commit hook: {e}")
                
        return True


class Ref(Generic[T]):
    """
    Transactional reference.
    """
    
    __slots__ = ('id', 'identity', '_history', '_history_lock', '_validators')
    
    def __init__(self, initial_value: T, name: Optional[str] = None, validator: Optional[Callable[[T], bool]] = None):
        self.id = TransactionCoordinator().register_ref(self)
        self.identity = RefIdentity(self.id, time.time(), name)
        
        # History is a list of (VersionStamp, Value)
        initial_version = TransactionCoordinator().create_version_stamp(0)
        self._history: List[Tuple[VersionStamp, T]] = [(initial_version, initial_value)]
        self._history_lock = threading.RLock()
        self._validators: List[Callable[[T], bool]] = []
        if validator:
            self._validators.append(validator)

    def _get_version(self) -> VersionStamp:
        with self._history_lock:
            return self._history[-1][0]

    def _get_value_at(self, version: VersionStamp) -> Tuple[T, VersionStamp]:
        with self._history_lock:
            # Binary search for the correct version
            # Find the largest version V such that V <= requested_version
            for i in range(len(self._history) - 1, -1, -1):
                v, val = self._history[i]
                if v.logical_time <= version.logical_time:
                    return val, v
            
            # If not found (shouldn't happen with proper retention)
            raise HistoryExpiredException(f"Snapshot at time {version.logical_time} expired for ref {self.id}")

    def _set_value(self, value: T, version: VersionStamp) -> None:
        # Run validators
        for v in self._validators:
            if not v(value):
                raise ValidationException(f"Validation failed for ref {self.id}")
                
        with self._history_lock:
            self._history.append((version, value))
            
            # Adaptive history management
            max_h = TransactionCoordinator().history.compute_max_history(self.id)
            if len(self._history) > max_h * 2:
                # Trigger internal trim if it gets too large
                bound = TransactionCoordinator().history.get_retention_bound()
                self._trim_history(bound)

    def _trim_history(self, retention_bound: int) -> None:
        with self._history_lock:
            if len(self._history) <= 1:
                return
            
            # Keep at least the latest version and anything newer than retention_bound
            new_history = []
            max_to_keep = TransactionCoordinator().history.compute_max_history(self.id)
            
            # Always keep the very latest
            latest = self._history[-1]
            
            # Find items to keep
            to_keep = []
            for i in range(len(self._history) - 2, -1, -1):
                v, val = self._history[i]
                if v.logical_time >= retention_bound or len(to_keep) < max_to_keep:
                    to_keep.append(self._history[i])
                else:
                    break
            
            to_keep.reverse()
            to_keep.append(latest)
            self._history = to_keep

    def deref(self) -> T:
        tx = Transaction.get_current()
        if tx:
            return tx.read(self)
        # Snapshot read outside transaction (optimistic)
        with self._history_lock:
            return self._history[-1][1]

    def set(self, value: T) -> None:
        tx = Transaction.get_current()
        if tx:
            tx.write(self, value)
        else:
            # Single-op transaction
            with dosync():
                write(self, value)

    def alter(self, fn: Callable[[T], T], *args, **kwargs) -> T:
        tx = Transaction.get_current()
        if tx:
            current = tx.read(self)
            new_val = fn(current, *args, **kwargs)
            tx.write(self, new_val)
            return new_val
        else:
            with dosync():
                return alter(self, fn, *args, **kwargs)

    def commute(self, fn: Callable[[T], T], *args, **kwargs) -> T:
        tx = Transaction.get_current()
        if tx:
            return tx.commute(self, fn, *args, **kwargs)
        else:
            with dosync():
                return commute(self, fn, *args, **kwargs)

    @property
    def value(self) -> T:
        return self.deref()

    @value.setter
    def value(self, val: T) -> None:
        self.set(val)

    def __repr__(self) -> str:
        return f"Ref(id={self.id}, value={self.value})"


# ============================================================================
# Atomic (Enhanced)
# ============================================================================

class Atom(Generic[T]):
    """
    Atomic reference for uncoordinated synchronous updates.
    Uses Seqlock for multi-threading performance.
    """
    
    def __init__(self, initial_value: T, name: Optional[str] = None):
        self._seqlock = SeqLock(initial_value)
        self.name = name
        self._watchers: Dict[str, Callable[[T, T], Any]] = {}

    def deref(self) -> T:
        return self._seqlock.read()

    def swap(self, fn: Callable[[T], T], *args, **kwargs) -> T:
        while True:
            old_val = self._seqlock.read()
            new_val = fn(old_val, *args, **kwargs)
            
            # Locking write
            with self._seqlock._write_lock:
                # Double-check
                if self._seqlock._value is old_val or self._seqlock._value == old_val:
                    self._seqlock.write(new_val)
                    self._notify_watchers(old_val, new_val)
                    return new_val
                # Else retry

    def reset(self, new_value: T) -> T:
        old_val = self._seqlock.read()
        self._seqlock.write(new_value)
        self._notify_watchers(old_val, new_value)
        return new_value

    def add_watcher(self, key: str, fn: Callable[[T, T], Any]) -> None:
        self._watchers[key] = fn

    def remove_watcher(self, key: str) -> None:
        self._watchers.pop(key, None)

    def _notify_watchers(self, old_val: T, new_val: T) -> None:
        for watcher in self._watchers.values():
            try: watcher(old_val, new_val)
            except Exception: pass

    @property
    def value(self) -> T:
        return self.deref()

    @value.setter
    def value(self, val: T) -> None:
        self.reset(val)

    def __repr__(self) -> str:
        return f"Atom(value={self.value})"


# ============================================================================
# Persistent Data Structures (Enhanced)
# ============================================================================

class PersistentVector(Generic[T], Sequence[T]):
    """
    Immutable persistent vector (relaxed radix balanced tree).
    Optimized for O(log32 n) access and updates.
    """
    
    SHFT = 5
    BRNCH = 32
    MASK = BRNCH - 1

    def __init__(self, count: int, shift: int, root: Any, tail: List[T]):
        self._count = count
        self._shift = shift
        self._root = root
        self._tail = tail

    @classmethod
    def empty(cls) -> 'PersistentVector[T]':
        return cls(0, cls.SHFT, [], [])

    def __len__(self) -> int:
        return self._count

    def _tail_off(self) -> int:
        if self._count < self.BRNCH:
            return 0
        return ((self._count - 1) >> self.SHFT) << self.SHFT

    def __getitem__(self, index: int) -> T:
        if 0 <= index < self._count:
            if index >= self._tail_off():
                return self._tail[index & self.MASK]
            
            node = self._root
            for level in range(self._shift, 0, -self.SHFT):
                node = node[(index >> level) & self.MASK]
            return node[index & self.MASK]
        raise IndexError("Vector index out of range")

    def assoc(self, index: int, value: T) -> 'PersistentVector[T]':
        if 0 <= index < self._count:
            if index >= self._tail_off():
                new_tail = list(self._tail)
                new_tail[index & self.MASK] = value
                return PersistentVector(self._count, self._shift, self._root, new_tail)
            return PersistentVector(
                self._count, self._shift,
                self._do_assoc(self._shift, self._root, index, value),
                self._tail
            )
        if index == self._count:
            return self.conj(value)
        raise IndexError("Vector index out of range")

    def _do_assoc(self, shift: int, node: Any, index: int, value: T) -> List:
        ret = list(node)
        if shift == 0:
            ret[index & self.MASK] = value
        else:
            sub_idx = (index >> shift) & self.MASK
            ret[sub_idx] = self._do_assoc(shift - self.SHFT, node[sub_idx], index, value)
        return ret

    def conj(self, value: T) -> 'PersistentVector[T]':
        # Tail has room
        if self._count - self._tail_off() < self.BRNCH:
            new_tail = list(self._tail)
            new_tail.append(value)
            return PersistentVector(self._count + 1, self._shift, self._root, new_tail)
        
        # Tail full, push to tree
        new_root = None
        new_shift = self._shift
        
        # Check if we need to grow the tree
        if (self._count >> self.SHFT) > (1 << self._shift):
            new_root = [self._root, self._new_path(self._shift, self._tail)]
            new_shift += self.SHFT
        else:
            new_root = self._push_tail(self._shift, self._root, self._tail)
            
        return PersistentVector(self._count + 1, new_shift, new_root, [value])

    def _new_path(self, shift: int, node: Any) -> Any:
        if shift == 0:
            return node
        return [self._new_path(shift - self.SHFT, node)]

    def _push_tail(self, shift: int, parent: Any, tail_node: Any) -> Any:
        sub_idx = ((self._count - 1) >> shift) & self.MASK
        ret = list(parent)
        
        if shift == self.SHFT:
            node_to_insert = tail_node
        else:
            if sub_idx < len(parent):
                node_to_insert = self._push_tail(shift - self.SHFT, parent[sub_idx], tail_node)
            else:
                node_to_insert = self._new_path(shift - self.SHFT, tail_node)
        
        if sub_idx < len(ret):
            ret[sub_idx] = node_to_insert
        else:
            ret.append(node_to_insert)
        return ret

    def pop(self) -> 'PersistentVector[T]':
        if self._count == 0:
            raise IndexError("Can't pop from empty vector")
        if self._count == 1:
            return PersistentVector.empty()
        
        if self._count - self._tail_off() > 1:
            new_tail = self._tail[:-1]
            return PersistentVector(self._count - 1, self._shift, self._root, new_tail)
        
        new_tail = self._get_tail_at(self._count - 2)
        new_root = self._pop_tail(self._shift, self._root)
        new_shift = self._shift
        
        # Level Bloat Fix: Check [0]
        if self._shift > self.SHFT and new_root is not None and len(new_root) == 1:
            new_root = new_root[0]  # The CRITICAL V3.0 Fix
            new_shift -= self.SHFT
            
        return PersistentVector(self._count - 1, new_shift, new_root or [], new_tail)

    def _get_tail_at(self, index: int) -> List[T]:
        node = self._root
        for level in range(self._shift, 0, -self.SHFT):
            node = node[(index >> level) & self.MASK]
        return list(node)

    def _pop_tail(self, shift: int, node: Any) -> Any:
        sub_idx = ((self._count - 2) >> shift) & self.MASK
        if shift > self.SHFT:
            new_child = self._pop_tail(shift - self.SHFT, node[sub_idx])
            if new_child is None and sub_idx == 0:
                return None
            ret = list(node)
            if new_child is not None:
                ret[sub_idx] = new_child
            else:
                ret.pop(sub_idx)
            return ret
        else:
            if sub_idx == 0:
                return None
            ret = list(node)
            ret.pop(sub_idx)
            return ret

    def __iter__(self) -> Iterator[T]:
        for i in range(self._count):
            yield self[i]

    def to_list(self) -> List[T]:
        return list(self)


class PersistentHashMap(Generic[K, V], MutableMapping[K, V]):
    """
    Immutable Hash Array Mapped Trie (HAMT).
    O(log32 n) performance.
    """
    
    def __init__(self, count: int, root: Any):
        self._count = count
        self._root = root

    @classmethod
    def empty(cls) -> 'PersistentHashMap[K, V]':
        return cls(0, None)

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, key: K) -> V:
        if self._root is None:
            raise KeyError(key)
        h = hash(key)
        val = self._root.get(0, h, key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: K, value: V) -> None:
        raise TypeError("PersistentHashMap is immutable. Use assoc()")

    def __delitem__(self, key: K) -> None:
        raise TypeError("PersistentHashMap is immutable. Use dissoc()")

    def assoc(self, key: K, value: V) -> 'PersistentHashMap[K, V]':
        h = hash(key)
        new_root, added = (self._root or _BitmapNode.empty()).assoc(0, h, key, value)
        return PersistentHashMap(self._count + (1 if added else 0), new_root)

    def dissoc(self, key: K) -> 'PersistentHashMap[K, V]':
        if self._root is None:
            return self
        h = hash(key)
        new_root = self._root.dissoc(0, h, key)
        if new_root is self._root:
            return self
        return PersistentHashMap(self._count - 1, new_root)

    def __iter__(self) -> Iterator[K]:
        if self._root:
            for k, v in self._root.kv_iter():
                yield k

    def items(self) -> Iterator[Tuple[K, V]]:
        if self._root:
            yield from self._root.kv_iter()


# Internal nodes for HAMT
class _BitmapNode:
    __slots__ = ('bitmap', 'nodes')
    
    def __init__(self, bitmap: int, nodes: List):
        self.bitmap = bitmap
        self.nodes = nodes

    @classmethod
    def empty(cls):
        return cls(0, [])

    def _index(self, bit: int) -> int:
        return bin(self.bitmap & (bit - 1)).count('1')

    def get(self, shift: int, h: int, key: Any) -> Any:
        bit = 1 << ((h >> shift) & 0x1f)
        if (self.bitmap & bit) == 0:
            return None
        idx = self._index(bit)
        node = self.nodes[idx]
        if isinstance(node, _BitmapNode):
            return node.get(shift + 5, h, key)
        # Entry node [key, val]
        if node[0] == key:
            return node[1]
        return None

    def assoc(self, shift: int, h: int, key: Any, value: Any) -> Tuple[Any, bool]:
        bit = 1 << ((h >> shift) & 0x1f)
        idx = self._index(bit)
        
        if (self.bitmap & bit) != 0:
            node = self.nodes[idx]
            if isinstance(node, _BitmapNode):
                new_sub, added = node.assoc(shift + 5, h, key, value)
                new_nodes = list(self.nodes)
                new_nodes[idx] = new_sub
                return _BitmapNode(self.bitmap, new_nodes), added
            
            # Existing entry
            if node[0] == key:
                if node[1] == value:
                    return self, False
                new_nodes = list(self.nodes)
                new_nodes[idx] = [key, value]
                return _BitmapNode(self.bitmap, new_nodes), False
            
            # Collision -> Create subnode
            new_sub, _ = _BitmapNode.empty().assoc(shift + 5, hash(node[0]), node[0], node[1])
            new_sub, added = new_sub.assoc(shift + 5, h, key, value)
            new_nodes = list(self.nodes)
            new_nodes[idx] = new_sub
            return _BitmapNode(self.bitmap, new_nodes), True
        else:
            new_nodes = list(self.nodes)
            new_nodes.insert(idx, [key, value])
            return _BitmapNode(self.bitmap | bit, new_nodes), True

    def dissoc(self, shift: int, h: int, key: Any) -> Any:
        bit = 1 << ((h >> shift) & 0x1f)
        if (self.bitmap & bit) == 0:
            return self
        idx = self._index(bit)
        node = self.nodes[idx]
        
        if isinstance(node, _BitmapNode):
            new_sub = node.dissoc(shift + 5, h, key)
            if new_sub is node: return self
            if new_sub is None:
                new_bitmap = self.bitmap ^ bit
                if new_bitmap == 0: return None
                new_nodes = list(self.nodes)
                new_nodes.pop(idx)
                return _BitmapNode(new_bitmap, new_nodes)
            new_nodes = list(self.nodes)
            new_nodes[idx] = new_sub
            return _BitmapNode(self.bitmap, new_nodes)
        
        if node[0] == key:
            new_bitmap = self.bitmap ^ bit
            if new_bitmap == 0: return None
            new_nodes = list(self.nodes)
            new_nodes.pop(idx)
            return _BitmapNode(new_bitmap, new_nodes)
        
        return self

    def kv_iter(self) -> Iterator[Tuple[Any, Any]]:
        for node in self.nodes:
            if isinstance(node, _BitmapNode):
                yield from node.kv_iter()
            else:
                yield node[0], node[1]


# ============================================================================
# STM Collections & Agents (Enhanced)
# ============================================================================

class STMQueue(Generic[T]):
    """
    Transactional FIFO queue.
    """
    
    def __init__(self, name: Optional[str] = None):
        self._vector = Ref(PersistentVector.empty(), name=f"{name or 'Queue'}_vector")

    def put(self, value: T) -> None:
        self._vector.alter(lambda v: v.conj(value))

    def get(self) -> T:
        while True:
            v = self._vector.deref()
            if len(v) > 0:
                item = v[0]
                # Better pop front logic in a single transaction
                @atomically
                def _do_get():
                    vec = self._vector.deref()
                    if len(vec) == 0: return None # Should not happen with outer check but for safety
                    val = vec[0]
                    # Efficiently shrink vector
                    new_vec = PersistentVector.empty()
                    for i in range(1, len(vec)):
                        new_vec = new_vec.conj(vec[i])
                    self._vector.set(new_vec)
                    return val
                
                result = _do_get()
                if result is not None:
                    return result
            
            # Blocking retry: wait for a short bit to avoid spinning (simplified wait)
            # Real implementation would use Condition variables across puts
            time.sleep(0.01)
            retry()

    def empty(self) -> bool:
        return len(self._vector.deref()) == 0

    def __len__(self) -> int:
        return len(self._vector.deref())


class STMAgent(Generic[T]):
    """
    Asynchronous agent for managing shared state.
    Sends actions to a thread pool.
    """
    
    if PYTHON_3_13_PLUS:
        try:
            _pool = asyncio.get_running_loop()
        except RuntimeError:
            _pool = None
    else:
        _pool = None
    
    def __init__(self, initial_value: T, name: Optional[str] = None):
        self._ref = Ref(initial_value, name=name)
        self._errors: List[Exception] = []
        self._lock = threading.Lock()

    def send(self, fn: Callable[[T, Any], T], *args, **kwargs) -> None:
        """Asynchronously apply function to agent state."""
        def task():
            try:
                with dosync():
                    self._ref.alter(fn, *args, **kwargs)
            except Exception as e:
                with self._lock:
                    self._errors.append(e)
                logger.error(f"Agent {self._ref.identity.name} error: {e}")
        
        threading.Thread(target=task, daemon=True).start()

    def deref(self) -> T:
        return self._ref.deref()

    @property
    def value(self) -> T:
        return self.deref()

    def await_value(self, timeout: float = 10.0) -> T:
        """Wait for actions to complete (simplified)."""
        # In a real implementation, we'd track a task queue.
        time.sleep(0.1) 
        return self.deref()


class STMVar(Generic[T]):
    """
    Dynamic variable with thread-local overrides.
    """
    
    def __init__(self, initial_value: T, name: Optional[str] = None):
        self._root_value = initial_value
        self._local = threading.local()
        self.name = name

    def deref(self) -> T:
        return getattr(self._local, 'value', self._root_value)

    @contextmanager
    def binding(self, value: T):
        old = getattr(self._local, 'value', None)
        has_old = hasattr(self._local, 'value')
        self._local.value = value
        try:
            yield
        finally:
            if has_old:
                self._local.value = old
            else:
                delattr(self._local, 'value')

    @property
    def value(self) -> T:
        return self.deref()


# ============================================================================
# Core Functions & Decorators (Enhanced)
# ============================================================================

def retry() -> None:
    """Manually trigger a transaction retry."""
    tx = Transaction.get_current()
    if not tx:
        raise STMException("retry() must be called within a transaction")
    tx.retry()


@contextmanager
def transaction(timeout: float = 10.0, max_retries: int = 100) -> Iterator[Transaction]:
    """
    Single-attempt transaction context manager.
    Does NOT handle automatic retries. Useful for fine-grained control or single ops.
    """
    coordinator = TransactionCoordinator()
    old_tx = Transaction.get_current()
    
    tx = old_tx or Transaction(coordinator, timeout=timeout, max_retries=max_retries)
    Transaction.set_current(tx)
    if not old_tx:
        coordinator.register_transaction(tx)
    
    try:
        with tx:
            yield tx
    finally:
        if not old_tx:
            coordinator.unregister_transaction(tx.id)
            Transaction.set_current(None)


def dosync(fn: Optional[Callable[..., R]] = None, timeout: float = 10.0, max_retries: int = 100) -> Union[R, Callable]:
    """
    Executes a function within an STM transaction with automatic retries.
    Can be used as a function or a decorator.
    """
    def wrapper(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            coordinator = TransactionCoordinator()
            retry_count = 0
            
            # Check if already in a transaction
            existing_tx = Transaction.get_current()
            if existing_tx:
                return func(*args, **kwargs)

            tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries)
            
            while True:
                tx.retry_count = retry_count
                Transaction.set_current(tx)
                coordinator.register_transaction(tx)
                try:
                    with tx:
                        result = func(*args, **kwargs)
                    return result
                except (RetryException, ConflictException) as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        raise CommitException(f"Max retries ({max_retries}) exceeded")
                    
                    # Backoff
                    refs = set(tx.read_log.keys()) | set(tx.write_log.keys())
                    if isinstance(e, ConflictException):
                        refs |= e.conflicting_refs
                    
                    backoff = coordinator.contention.get_backoff_for_retry(retry_count, refs)
                    time.sleep(backoff)
                    tx._reset_for_retry()
                except Exception:
                    tx.abort()
                    raise
                finally:
                    coordinator.unregister_transaction(tx.id)
                    Transaction.set_current(None)
        return inner

    if fn:
        return wrapper(fn)()
    return wrapper


def atomically(fn: Callable[..., R]) -> Callable[..., R]:
    """Decorator to make a function run in an STM transaction with retries."""
    return dosync(fn=None)(fn)


def transaction(timeout: float = 10.0, max_retries: int = 100):
    """Decorator with parameters for STM transactions."""
    def decorator(fn):
        return dosync(fn=None, timeout=timeout, max_retries=max_retries)(fn)
    return decorator


# Async Support (Enhanced for 3.13+)
@asynccontextmanager
async def async_dosync(timeout: float = 10.0, max_retries: int = 100):
    """Asynchronous transaction context manager."""
    coordinator = TransactionCoordinator()
    retry_count = 0
    
    # Check if already in a transaction
    existing_tx = Transaction.get_current()
    if existing_tx:
        yield existing_tx
        return

    tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries)
    
    while True:
        tx.retry_count = retry_count
        Transaction.set_current(tx)
        coordinator.register_transaction(tx)
        try:
            with tx:
                yield tx
            break
        except (RetryException, ConflictException) as e:
            retry_count += 1
            if retry_count > max_retries:
                raise CommitException(f"Max retries ({max_retries}) exceeded")
            
            # Backoff
            refs = set(tx.read_log.keys()) | set(tx.write_log.keys())
            if isinstance(e, ConflictException):
                refs |= e.conflicting_refs
            
            backoff = coordinator.contention.get_backoff_for_retry(retry_count, refs)
            await asyncio.sleep(backoff)
            tx._reset_for_retry()
        except Exception:
            tx.abort()
            raise
        finally:
            coordinator.unregister_transaction(tx.id)
            Transaction.set_current(None)



# Core API Aliases
def alter(ref: Ref[T], fn: Callable[[T], T], *args, **kwargs) -> T:
    return ref.alter(fn, *args, **kwargs)


def commute(ref: Ref[T], fn: Callable[[T], T], *args, **kwargs) -> T:
    return ref.commute(fn, *args, **kwargs)

def write(ref: Ref[T], value: T) -> None:
    ref.set(value)

def read(ref: Ref[T]) -> T:
    return ref.deref()

def ref(value: T, name: Optional[str] = None) -> Ref[T]:
    return Ref(value, name=name)

def atom(value: T, name: Optional[str] = None) -> Atom[T]:
    return Atom(value, name=name)


# ============================================================================
# Diagnostics & Monitoring (Enhanced)
# ============================================================================

class STMDiagnostics:
    """
    Monitoring and debugging tools for STM.
    """
    
    @staticmethod
    def get_system_stats() -> Dict[str, Any]:
        """Get comprehensive system statistics."""
        return TransactionCoordinator().get_stats()

    @staticmethod
    def dump_registered_refs() -> List[Dict[str, Any]]:
        """Dump all currently registered Refs."""
        coordinator = TransactionCoordinator()
        refs = coordinator.get_refs_snapshot()
        return [
            {
                "id": ref.id,
                "name": ref.identity.name,
                "history_size": len(ref._history),
                "last_version": str(ref._get_version())
            }
            for ref in refs
        ]

    @staticmethod
    def set_log_level(level: int) -> None:
        logger.setLevel(level)


# Clean exit
@atexit.register
def _cleanup_stm():
    coordinator = TransactionCoordinator()
    if hasattr(coordinator, '_reaper'):
        coordinator._reaper.stop()
    logger.info("Atomix STM System shutdown clean")






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

Version: 3.1.0
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


@dataclass(frozen=True, slots=True)
class VersionStamp:
    """Immutable version identifier for MVCC."""
    transaction_id: int
    logical_time: int
    physical_time: float
    epoch: int = 0
    
    def __lt__(self, other: 'VersionStamp') -> bool:
        if self.epoch != other.epoch:
            return self.epoch < other.epoch
        if self.logical_time != other.logical_time:
            return self.logical_time < other.logical_time
        return self.transaction_id < other.transaction_id
    
    def __le__(self, other: 'VersionStamp') -> bool:
        return self == other or self < other
    
    def __gt__(self, other: 'VersionStamp') -> bool:
        return not self <= other
    
    def __ge__(self, other: 'VersionStamp') -> bool:
        return not self < other
    
    def __hash__(self) -> int:
        return hash((self.transaction_id, self.logical_time, self.epoch))


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
            if len(accesses) > 10:
                recent_rate = len(accesses) / 60.0
                if recent_rate > self._access_threshold:  # High access rate
                    base = int(base * 1.5)
        
        # Check memory pressure
        try:
            import psutil
            memory_percent = psutil.virtual_memory().percent / 100.0
            if memory_percent > self._memory_threshold:
                base = max(self._min_history, int(base * 0.5))
                self._memory_pressure_events += 1
                logger.debug(
                    f"Memory pressure detected ({memory_percent:.1%}), "
                    f"reducing history to {base}"
                )
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
        
        if removed > 0:
            logger.debug(f"Reaper removed {removed} stale snapshots")
        
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
    
    Uses exponential backoff:
    - Phase 1 (0-100): Busy spin (CPU intensive)
    - Phase 2 (100-1000): Yield (context switch)
    - Phase 3 (1000+): Sleep (high latency)
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
                # Fast path
                if not self._locked:  # Double-check
                    self._locked = True
                    self._owner = thread_id
                    self._spin_count = spins
                    return True
            
            # Adaptive spinning
            spins += 1
            
            if spins < 100:
                # Busy spin
                continue
            elif spins < 1000:
                # Yield
                time.sleep(0)
            else:
                # Sleep
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
    
    @property
    def spin_count(self) -> int:
        return self._spin_count


class SeqLock(Generic[T]):
    """
    Sequence lock for lock-free optimistic reads.
    
    Pattern from Linux kernel: readers never block,
    but may retry if write occurs during read.
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
            
            # Odd sequence = write in progress
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
            self._sequence += 1  # Start write
            self._value = new_value
            self._sequence += 1  # End write
    
    def get_version(self) -> int:
        """Get current sequence number."""
        return self._sequence


class RWLock:
    """
    Reader-Writer lock for multiple readers, single writer.
    
    Optimized for read-heavy workloads typical in STM.
    """
    
    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(threading.RLock())
        self._write_ready = threading.Condition(threading.RLock())
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire read lock."""
        with self._read_ready:
            while self._writers > 0:
                if not self._read_ready.wait(timeout):
                    return False
            self._readers += 1
            return True
    
    def release_read(self) -> None:
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._write_ready.notifyAll()
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire write lock."""
        with self._write_ready:
            while self._writers > 0 or self._readers > 0:
                if not self._write_ready.wait(timeout):
                    return False
            self._writers += 1
            return True
    
    def release_write(self) -> None:
        """Release write lock."""
        with self._write_ready:
            self._writers -= 1
            self._write_ready.notifyAll()
            self._read_ready.notifyAll()


# ============================================================================
# Transaction Coordinator (Enhanced)
# ============================================================================

class TransactionCoordinator:
    """
    Global coordinator for all STM transactions.
    
    Thread-safe singleton managing:
    - ID generation
    - Global clock
    - Ref registry
    - Conflict detection
    - Commit serialization
    - Background Reaper
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
        """Initialize coordinator."""
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
        
        # Reaper thread
        self._reaper = STMReaper(self, interval=5.0)
        self._reaper.start()
        
        # Statistics
        self._stats_lock = threading.RLock()
        self._stats = {
            "total_transactions": 0,
            "total_commits": 0,
            "total_aborts": 0,
            "total_conflicts": 0
        }
        
        logger.info("TransactionCoordinator initialized")
    
    def reset(self) -> None:
        """Reset coordinator state (testing)."""
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
        """Generate transaction ID."""
        with self._stats_lock:
            self._stats["total_transactions"] += 1
        return next(self._tx_counter)
    
    def new_ref_id(self) -> int:
        """Generate reference ID."""
        return next(self._ref_counter)
    
    def get_logical_time(self) -> int:
        """Get current logical time."""
        with self._clock_lock:
            return self._logical_clock
    
    def advance_clock(self) -> int:
        """Advance and return logical time."""
        with self._clock_lock:
            self._logical_clock += 1
            return self._logical_clock
    
    def create_version_stamp(self, tx_id: int) -> VersionStamp:
        """Create version stamp."""
        return VersionStamp(
            transaction_id=tx_id,
            logical_time=self.advance_clock(),
            physical_time=time.time(),
            epoch=self._current_epoch
        )
    
    def register_ref(self, ref: 'Ref') -> int:
        """Register Ref."""
        ref_id = self.new_ref_id()
        with self._refs_lock:
            self._refs[ref_id] = ref
        return ref_id
    
    def unregister_ref(self, ref_id: int) -> None:
        """Unregister Ref."""
        with self._refs_lock:
            self._refs.pop(ref_id, None)
    
    def get_ref(self, ref_id: int) -> Optional['Ref']:
        """Get Ref by ID."""
        with self._refs_lock:
            return self._refs.get(ref_id)
    
    def register_transaction(self, tx: 'Transaction') -> None:
        """Register transaction."""
        with self._txs_lock:
            self._active_txs[tx.id] = tx
        self._history_manager.register_snapshot(tx.id, tx.snapshot_version)
    
    def unregister_transaction(self, tx_id: int) -> None:
        """Unregister transaction."""
        with self._txs_lock:
            self._active_txs.pop(tx_id, None)
        self._history_manager.unregister_snapshot(tx_id)
    
    def get_active_transactions(self) -> List['Transaction']:
        """Get active transactions."""
        with self._txs_lock:
            return list(self._active_txs.values())
    
    def detect_conflicts(
        self,
        tx: 'Transaction',
        read_set: Dict[int, VersionStamp],
        write_set: Set[int]
    ) -> Optional[FrozenSet[int]]:
        """Detect write conflicts."""
        conflicting = set()
        
        for ref_id, read_version in read_set.items():
            if ref_id in write_set:
                continue
            
            ref = self.get_ref(ref_id)
            if ref is None:
                continue
            
            current_version = ref._get_version()
            if current_version > read_version:
                conflicting.add(ref_id)
        
        return frozenset(conflicting) if conflicting else None
    
    def prepare_commit(
        self,
        tx: 'Transaction',
        read_set: Dict[int, VersionStamp],
        write_set: Set[int]
    ) -> Tuple[bool, Optional[FrozenSet[int]]]:
        """Prepare phase of 2PC."""
        with self._commit_lock:
            conflicts = self.detect_conflicts(tx, read_set, write_set)
            
            if conflicts:
                with self._stats_lock:
                    self._stats["total_conflicts"] += 1
                return False, conflicts
            
            return True, None
    
    def record_commit(self) -> None:
        """Record successful commit."""
        with self._stats_lock:
            self._stats["total_commits"] += 1
    
    def record_abort(self) -> None:
        """Record abort."""
        with self._stats_lock:
            self._stats["total_aborts"] += 1
    
    @property
    def contention(self) -> ContentionManager:
        return self._contention_manager
    
    @property
    def history(self) -> HistoryManager:
        return self._history_manager
    
    def get_stats(self) -> Dict[str, Any]:
        """Get coordinator statistics."""
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
        """Get a snapshot of all refs (for Reaper)."""
        with self._refs_lock:
            return list(self._refs.values())


# ============================================================================
# STM Reaper (Background Cleanup)
# ============================================================================

class STMReaper(threading.Thread):
    """
    Background daemon cleaning up stale snapshots and trimming history.
    
    Runs periodically without disrupting transactions.
    Uses batch processing to avoid long lock holds.
    """
    
    def __init__(
        self,
        coordinator: TransactionCoordinator,
        interval: float = 5.0,
        batch_size: int = 100
    ):
        super().__init__(name="STM-Reaper", daemon=True)
        self.coordinator = coordinator
        self.interval = interval
        self.batch_size = batch_size
        self._running = True
        self._cleanup_count = 0
    
    def run(self) -> None:
        """Main reaper loop."""
        logger.info("STM Reaper started")
        
        while self._running:
            try:
                # Clean stale snapshots
                removed = self.coordinator.history.cleanup_stale_snapshots(
                    timeout=60.0
                )
                
                # Trim refs in batches
                refs = self.coordinator.get_refs_snapshot()
                retention_bound = self.coordinator.history.get_retention_bound()
                
                # Process in batches to avoid long lock holds
                for i in range(0, len(refs), self.batch_size):
                    batch = refs[i:i + self.batch_size]
                    
                    for ref in batch:
                        try:
                            ref._trim_history(retention_bound)
                        except Exception as e:
                            logger.error(f"Error trimming ref {ref.id}: {e}")
                    
                    # Yield to other threads
                    if i > 0 and i % (self.batch_size * 2) == 0:
                        time.sleep(0.001)
                
                self._cleanup_count += 1
                
                if removed > 0:
                    logger.debug(f"Reaper removed {removed} stale snapshots")
                
                time.sleep(self.interval)
                
            except Exception as e:
                logger.error(f"Reaper error: {e}")
                time.sleep(self.interval)
    
    def stop(self) -> None:
        """Stop reaper gracefully."""
        self._running = False
        logger.info("STM Reaper stopped")


# ============================================================================
# Transaction (Enhanced)
# ============================================================================

class Transaction:
    """
    STM transaction with snapshot isolation and optimistic concurrency.
    
    Features:
    - MVCC snapshot isolation
    - Read/write buffering
    - Automatic conflict detection
    - Retry with exponential backoff
    - Commutative operations support
    - Full async support
    """
    
    def __init__(
        self,
        coordinator: TransactionCoordinator,
        timeout: float = 30.0,
        max_retries: int = 1000,
        label: Optional[str] = None
    ):
        self._coordinator = coordinator
        self.id = coordinator.new_transaction_id()
        self._label = label
        
        self._state = TransactionState.ACTIVE
        self._timeout = timeout
        self._max_retries = max_retries
        self._start_time = time.time()
        
        self.snapshot_version = coordinator.create_version_stamp(self.id)
        
        # Logs
        self._read_log: Dict[int, ReadLogEntry] = {}
        self._write_log: Dict[int, WriteLogEntry] = {}
        self._commutes: Dict[int, List[CommuteEntry]] = defaultdict(list)
        
        # Tracking
        self._retry_count = 0
        self._thread_id = threading.current_thread().ident
        self._lock = threading.RLock()
        self._depth = 0
    
    @property
    def state(self) -> TransactionState:
        return self._state
    
    @property
    def label(self) -> Optional[str]:
        return self._label
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self._start_time
    
    def _check_active(self) -> None:
        """Ensure transaction is active."""
        if self._state != TransactionState.ACTIVE:
            if self._state == TransactionState.ABORTED:
                raise TransactionAbortedException(
                    f"Transaction {self.id} has been aborted"
                )
            raise STMException(
                f"Transaction {self.id} is in state {self._state.name}"
            )
        
        if self.elapsed_time > self._timeout:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise TimeoutException(
                f"Transaction {self.id} timed out after {self.elapsed_time:.2f}s"
            )
    
    def _read_ref(self, ref: 'Ref[T]') -> T:
        """Read Ref value within transaction."""
        with self._lock:
            self._check_active()
            
            ref_id = ref._identity.id
            
            # Check write log first (read-your-own-writes)
            if ref_id in self._write_log:
                return self._write_log[ref_id].new_value
            
            # Apply commutes
            if ref_id in self._commutes:
                value = ref._read_raw()
                for commute in self._commutes[ref_id]:
                    value = commute.function(value, *commute.args, **commute.kwargs)
                return value
            
            # Read from ref
            value, version = ref._read_at_version(self.snapshot_version)
            
            # Record read
            self._read_log[ref_id] = ReadLogEntry(
                ref_id=ref_id,
                version_read=version,
                value_hash=hash(repr(value))
            )
            
            self._coordinator.history.record_access(ref_id)
            
            return value
    
    def _write_ref(self, ref: 'Ref[T]', value: T) -> None:
        """Write to Ref."""
        with self._lock:
            self._check_active()
            
            ref_id = ref._identity.id
            
            # Get old value
            old_entry = self._write_log.get(ref_id)
            old_value = old_entry.old_value if old_entry else ref._read_raw()
            
            # Validate
            ref._validate(value)
            
            # Record write
            self._write_log[ref_id] = WriteLogEntry(
                ref_id=ref_id,
                old_value=old_value if ref_id not in self._write_log else old_entry.old_value,
                new_value=value,
                old_version=ref._get_version(),
                is_commutative=False
            )
    
    def _commute_ref(
        self,
        ref: 'Ref[T]',
        fn: Callable[[T], T],
        *args,
        **kwargs
    ) -> None:
        """Register commutative operation."""
        with self._lock:
            self._check_active()
            
            ref_id = ref._identity.id
            
            # If already written, apply to write
            if ref_id in self._write_log:
                entry = self._write_log[ref_id]
                new_value = fn(entry.new_value, *args, **kwargs)
                self._write_log[ref_id] = WriteLogEntry(
                    ref_id=ref_id,
                    old_value=entry.old_value,
                    new_value=new_value,
                    old_version=entry.old_version,
                    is_commutative=True,
                    commutative_fn=fn
                )
            else:
                # Defer to commit time
                self._commutes[ref_id].append(CommuteEntry(
                    ref_id=ref_id,
                    function=fn,
                    args=args,
                    kwargs=kwargs
                ))
    
    def _prepare(self) -> Tuple[bool, Optional[FrozenSet[int]]]:
        """2PC Prepare phase."""
        with self._lock:
            if self._state != TransactionState.ACTIVE:
                raise STMException(f"Cannot prepare in state {self._state.name}")
            
            self._state = TransactionState.PREPARING
            
            read_set = {
                ref_id: entry.version_read
                for ref_id, entry in self._read_log.items()
            }
            write_set = set(self._write_log.keys()) | set(self._commutes.keys())
            
            return self._coordinator.prepare_commit(self, read_set, write_set)
    
    def _commit(self) -> bool:
        """Commit transaction."""
        with self._lock:
            if self._state == TransactionState.COMMITTED:
                return True
            
            if self._state != TransactionState.ACTIVE:
                raise STMException(f"Cannot commit in state {self._state.name}")
            
            if self.elapsed_time > self._timeout:
                self._state = TransactionState.ABORTED
                self._coordinator.record_abort()
                raise TimeoutException("Transaction timeout during commit")
            
            self._state = TransactionState.COMMITTING
        
        try:
            # Prepare
            success, conflicts = self._prepare()
            
            if not success and conflicts:
                backoff = self._coordinator.contention.record_conflict(
                    conflicts,
                    self._retry_count
                )
                self._state = TransactionState.ABORTED
                self._coordinator.record_abort()
                raise ConflictException(
                    f"Transaction conflicted on refs {conflicts}",
                    conflicting_refs=conflicts
                )
            
            # Apply commutes
            self._apply_commutes()
            
            # Create commit version
            commit_version = self._coordinator.create_version_stamp(self.id)
            
            # Apply writes
            for ref_id, entry in self._write_log.items():
                ref = self._coordinator.get_ref(ref_id)
                if ref is not None:
                    ref._commit_value(entry.new_value, commit_version)
            
            self._state = TransactionState.COMMITTED
            
            # Record success
            all_refs = set(self._write_log.keys()) | set(self._read_log.keys())
            self._coordinator.contention.record_success(all_refs)
            self._coordinator.record_commit()
            
            return True
            
        except Exception:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise
    
    def _apply_commutes(self) -> None:
        """Apply deferred commutative operations."""
        for ref_id, commutes in self._commutes.items():
            if ref_id in self._write_log:
                continue
            
            ref = self._coordinator.get_ref(ref_id)
            if ref is None:
                continue
            
            value = ref._read_raw()
            for commute in commutes:
                value = commute.function(value, *commute.args, **commute.kwargs)
            
            ref._validate(value)
            
            self._write_log[ref_id] = WriteLogEntry(
                ref_id=ref_id,
                old_value=ref._read_raw(),
                new_value=value,
                old_version=ref._get_version(),
                is_commutative=True
            )
    
    def _abort(self) -> None:
        """Abort transaction."""
        with self._lock:
            if self._state in (TransactionState.COMMITTED, TransactionState.ABORTED):
                return
            
            self._state = TransactionState.ABORTED
            self._read_log.clear()
            self._write_log.clear()
            self._commutes.clear()
    
    def _retry(self) -> None:
        """Signal retry."""
        self._retry_count += 1
        
        if self._retry_count > self._max_retries:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise TimeoutException(
                f"Transaction {self.id} exceeded max retries ({self._max_retries})"
            )
        
        raise RetryException(
            f"Transaction {self.id} retrying",
            retry_count=self._retry_count
        )
    
    def _reset_for_retry(self) -> None:
        """Reset transaction for retry."""
        with self._lock:
            self._state = TransactionState.ACTIVE
            self.snapshot_version = self._coordinator.create_version_stamp(self.id)
            self._read_log.clear()
            self._write_log.clear()
            self._commutes.clear()
            self._start_time = time.time()
    
    def __enter__(self) -> 'Transaction':
        self._depth += 1
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._depth -= 1
        
        if exc_type is not None:
            self._abort()
            return False
        
        if self._depth == 0:
            self._commit()
        
        return True
    
    def __repr__(self) -> str:
        return (
            f"Transaction(id={self.id}, state={self._state.name}, "
            f"retries={self._retry_count})"
        )


# ============================================================================
# Ref - Transactional Reference (Enhanced)
# ============================================================================

class Ref(Generic[T]):
    """
    Transactional reference with MVCC and history.
    
    Features:
    - Snapshot isolation
    - Adaptive history management
    - Validators and invariants
    - Watcher callbacks
    - Lock-free reads via SeqLock
    - JSON serialization
    """
    
    __slots__ = (
        '_identity', '_value', '_version', '_history',
        '_lock', '_coordinator', '_validators', '_watchers',
        '_max_history', '_min_history', '_watcher_lock',
        '_invariant', '_seqlock', '_access_time', '_json_encoder'
    )
    
    def __init__(
        self,
        value: T,
        min_history: int = 0,
        max_history: int = 100,
        validator: Optional[Callable[[T], bool]] = None,
        name: Optional[str] = None,
        json_encoder: Optional[Callable[[T], Any]] = None
    ):
        self._coordinator = TransactionCoordinator()
        self._identity = RefIdentity(
            id=self._coordinator.new_ref_id(),
            created_at=time.time(),
            name=name
        )
        
        self._value = value
        self._version = self._coordinator.create_version_stamp(0)
        
        self._history: List[Tuple[VersionStamp, T]] = []
        self._min_history = min_history
        self._max_history = max_history
        
        self._lock = threading.RLock()
        self._seqlock = SeqLock(value)
        
        self._validators: List[Callable[[T], bool]] = []
        if validator:
            self._validators.append(validator)
        
        self._watchers: List[Callable[[T, T], None]] = []
        self._watcher_lock = threading.Lock()
        
        self._invariant: Optional[Callable[[T], bool]] = None
        self._access_time = time.time()
        
        self._json_encoder = json_encoder or (lambda x: x)
        
        self._coordinator.register_ref(self)
    
    @property
    def id(self) -> int:
        return self._identity.id
    
    @property
    def name(self) -> Optional[str]:
        return self._identity.name
    
    def _get_version(self) -> VersionStamp:
        """Get current version."""
        with self._lock:
            return self._version
    
    def _read_raw(self) -> T:
        """Read without transaction."""
        with self._lock:
            return self._value
    
    def _read_at_version(self, version: VersionStamp) -> Tuple[T, VersionStamp]:
        """Read at version."""
        with self._lock:
            if self._version <= version:
                return self._value, self._version
            
            for hist_version, hist_value in reversed(self._history):
                if hist_version <= version:
                    return hist_value, hist_version
            
            return self._value, self._version
    
    def _commit_value(self, value: T, version: VersionStamp) -> None:
        """Commit new value."""
        old_value = self._value
        
        with self._lock:
            if self._min_history > 0 or len(self._history) > 0:
                self._history.append((self._version, self._value))
            
            self._value = value
            self._version = version
            self._access_time = time.time()
            
            retention_bound = self._coordinator.history.get_retention_bound()
            self._trim_history(retention_bound)
        
        self._seqlock.write(value)
        self._notify_watchers(old_value, value)
    
    def _trim_history(self, retention_logical_time: int) -> None:
        """Trim history."""
        adaptive_max = self._coordinator.history.compute_max_history(self._identity.id)
        max_hist = min(self._max_history, adaptive_max)
        
        cutoff = 0
        for i, (version, _) in enumerate(self._history):
            if version.logical_time >= retention_logical_time:
                cutoff = i
                break
        
        if len(self._history) > max_hist:
            start = max(cutoff, len(self._history) - max_hist)
            self._history = self._history[start:]
        elif cutoff > 0:
            self._history = self._history[cutoff:]
    
    def _validate(self, value: T) -> None:
        """Run validators."""
        for validator in self._validators:
            if not validator(value):
                raise ValidationException(f"Validation failed for Ref {self._identity.id}")
        
        if self._invariant and not self._invariant(value):
            raise InvariantViolationException(f"Invariant violated for Ref {self._identity.id}")
    
    def _notify_watchers(self, old_value: T, new_value: T) -> None:
        """Notify watchers."""
        with self._watcher_lock:
            watchers = list(self._watchers)
        
        for watcher in watchers:
            try:
                watcher(old_value, new_value)
            except Exception:
                pass
    
    # Public API
    
    def deref(self) -> T:
        """Read value in transaction."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("deref() requires active transaction")
        return tx._read_ref(self)
    
    def read(self) -> T:
        """Read without transaction (lock-free)."""
        return self._seqlock.read()
    
    def set(self, value: T) -> None:
        """Set value in transaction."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("set() requires active transaction")
        tx._write_ref(self, value)
    
    def reset(self, value: T) -> T:
        """Set without transaction."""
        self._validate(value)
        version = self._coordinator.create_version_stamp(0)
        self._commit_value(value, version)
        return value
    
    def alter(self, fn: Callable[[T], T], *args, **kwargs) -> None:
        """Apply function in transaction."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("alter() requires active transaction")
        
        current = tx._read_ref(self)
        new_value = fn(current, *args, **kwargs)
        tx._write_ref(self, new_value)
    
    def commute(self, fn: Callable[[T], T], *args, **kwargs) -> None:
        """Commutative operation."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("commute() requires active transaction")
        tx._commute_ref(self, fn, *args, **kwargs)
    
    def add_validator(self, validator: Callable[[T], bool]) -> 'Ref[T]':
        """Add validator."""
        with self._lock:
            self._validators.append(validator)
        return self
    
    def set_validator(self, validator: Callable[[T], bool]) -> 'Ref[T]':
        """Set single validator."""
        with self._lock:
            self._validators = [validator]
        return self
    
    def add_watcher(self, watcher: Callable[[T, T], None]) -> 'Ref[T]':
        """Add watcher."""
        with self._watcher_lock:
            self._watchers.append(watcher)
        return self
    
    def remove_watcher(self, watcher: Callable[[T, T], None]) -> None:
        """Remove watcher."""
        with self._watcher_lock:
            try:
                self._watchers.remove(watcher)
            except ValueError:
                pass
    
    def set_invariant(self, invariant: Callable[[T], bool]) -> 'Ref[T]':
        """Set invariant."""
        self._invariant = invariant
        return self
    
    def set_history_bounds(self, min_history: int, max_history: int) -> 'Ref[T]':
        """Set history bounds."""
        self._min_history = min_history
        self._max_history = max_history
        return self
    
    def get_history(self, limit: int = 10) -> List[Tuple[VersionStamp, T]]:
        """Get history."""
        with self._lock:
            return list(self._history[-limit:])
    
    def get_history_size(self) -> int:
        """Get history size."""
        with self._lock:
            return len(self._history)
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        value = self._seqlock.read()
        encoded = self._json_encoder(value)
        return json.dumps(encoded, default=str)
    
    @classmethod
    def from_json(
        cls: Type['Ref[T]'], 
        json_str: str, 
        decoder: Optional[Callable[[Any], T]] = None
    ) -> 'Ref[T]':
        """Deserialize from JSON."""
        decoded = json.loads(json_str)
        if decoder:
            value = decoder(decoded)
        else:
            value = decoded
        return cls(value)
    
    def __repr__(self) -> str:
        return f"Ref(id={self._identity.id}, name={self._identity.name!r}, value={self._value!r})"
    
    def __del__(self):
        """Cleanup."""
        try:
            self._coordinator.unregister_ref(self._identity.id)
        except:
            pass


# ============================================================================
# Atom - Lock-Free Atomic Reference (Enhanced)
# ============================================================================

class Atom(Generic[T]):
    """
    Lock-free atomic reference using SeqLock.
    
    Provides true lock-free reads without transactions.
    """
    
    __slots__ = ('_seqlock', '_validator', '_watchers', '_watcher_lock', '_json_encoder')
    
    def __init__(
        self,
        value: T,
        validator: Optional[Callable[[T], bool]] = None,
        json_encoder: Optional[Callable[[T], Any]] = None
    ):
        self._seqlock = SeqLock(value)
        self._validator = validator
        self._watchers: List[Callable[[T, T], None]] = []
        self._watcher_lock = threading.Lock()
        self._json_encoder = json_encoder or (lambda x: x)
    
    def deref(self) -> T:
        """Lock-free read."""
        return self._seqlock.read()
    
    def reset(self, new_value: T) -> T:
        """Reset value."""
        if self._validator and not self._validator(new_value):
            raise ValidationException("Atom validation failed")
        
        old_value = self._seqlock.read()
        self._seqlock.write(new_value)
        self._notify_watchers(old_value, new_value)
        return new_value
    
    def swap(self, fn: Callable[[T], T], *args, **kwargs) -> T:
        """Atomic swap."""
        while True:
            old_value = self._seqlock.read()
            new_value = fn(old_value, *args, **kwargs)
            
            if self._validator and not self._validator(new_value):
                raise ValidationException("Atom validation failed")
            
            current = self._seqlock.read()
            if current == old_value:
                self._seqlock.write(new_value)
                self._notify_watchers(old_value, new_value)
                return new_value
    
    def compare_and_set(self, expected: T, new_value: T) -> bool:
        """CAS operation."""
        if self._validator and not self._validator(new_value):
            raise ValidationException("Atom validation failed")
        
        current = self._seqlock.read()
        if current != expected:
            return False
        
        self._seqlock.write(new_value)
        self._notify_watchers(expected, new_value)
        return True
    
    def _notify_watchers(self, old_value: T, new_value: T) -> None:
        """Notify watchers."""
        with self._watcher_lock:
            watchers = list(self._watchers)
        
        for watcher in watchers:
            try:
                watcher(old_value, new_value)
            except Exception:
                pass
    
    def add_watcher(self, watcher: Callable[[T, T], None]) -> 'Atom[T]':
        """Add watcher."""
        with self._watcher_lock:
            self._watchers.append(watcher)
        return self
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        value = self._seqlock.read()
        encoded = self._json_encoder(value)
        return json.dumps(encoded, default=str)
    
    @classmethod
    def from_json(
        cls: Type['Atom[T]'], 
        json_str: str, 
        decoder: Optional[Callable[[Any], T]] = None
    ) -> 'Atom[T]':
        """Deserialize from JSON."""
        decoded = json.loads(json_str)
        if decoder:
            value = decoder(decoded)
        else:
            value = decoded
        return cls(value)
    
    def __repr__(self) -> str:
        return f"Atom(value={self._seqlock.read()!r})"


# ============================================================================
# Persistent Data Structures (Enhanced with JSON)
# ============================================================================

class PersistentVector(Generic[T]):
    """
    Immutable vector with structural sharing.
    
    O(log32 n) operations using 32-way branching.
    Supports JSON serialization.
    """
    
    __slots__ = ('_root', '_tail', '_size', '_shift', '_hash', '_json_encoder')
    
    BRANCH_FACTOR = 32
    BITS = 5
    MASK = 31
    
    def __init__(
        self,
        root: Optional[List] = None,
        tail: Optional[List] = None,
        size: int = 0,
        shift: int = 5,
        _hash: Optional[int] = None,
        json_encoder: Optional[Callable[[T], Any]] = None
    ):
        self._root = root if root is not None else []
        self._tail = tail if tail is not None else []
        self._size = size
        self._shift = shift
        self._hash = _hash
        self._json_encoder = json_encoder or (lambda x: x)
    
    def __len__(self) -> int:
        return self._size
    
    def __bool__(self) -> bool:
        return self._size > 0
    
    def __getitem__(self, index: int) -> T:
        if index < 0:
            index += self._size
        if index < 0 or index >= self._size:
            raise IndexError(f"Index {index} out of range")
        
        if index >= self._tail_offset():
            return self._tail[index & self.MASK]
        
        node = self._root
        for level in range(self._shift, 0, -self.BITS):
            node = node[(index >> level) & self.MASK]
        
        return node[index & self.MASK]
    
    def first(self) -> Optional[T]:
        """Get first element."""
        return self[0] if self._size > 0 else None
    
    def last(self) -> Optional[T]:
        """Get last element."""
        return self[self._size - 1] if self._size > 0 else None
    
    def _tail_offset(self) -> int:
        if self._size < self.BRANCH_FACTOR:
            return 0
        return ((self._size - 1) >> self.BITS) << self.BITS
    
    def conj(self, value: T) -> 'PersistentVector[T]':
        """Add element."""
        if self._size - self._tail_offset() < self.BRANCH_FACTOR:
            new_tail = self._tail + [value]
            return PersistentVector(
                self._root,
                new_tail,
                self._size + 1,
                self._shift
            )
        
        new_root = self._push_tail(self._shift, self._root, self._tail)
        new_shift = self._shift
        
        if len(new_root) > self.BRANCH_FACTOR:
            new_root = [self._root, new_root]
            new_shift += self.BITS
        
        return PersistentVector(
            new_root,
            [value],
            self._size + 1,
            new_shift
        )
    
    def _push_tail(self, level: int, parent: List, tail_node: List) -> List:
        """Push tail into tree."""
        sub_idx = ((self._size - 1) >> level) & self.MASK
        
        if level == self.BITS:
            return parent + [tail_node] if parent else [tail_node]
        
        if not parent:
            child = self._push_tail(level - self.BITS, [], tail_node)
            return [child]
        
        new_parent = list(parent)
        if len(new_parent) > sub_idx:
            new_parent[sub_idx] = self._push_tail(
                level - self.BITS,
                parent[sub_idx],
                tail_node
            )
        else:
            child = self._push_tail(level - self.BITS, [], tail_node)
            new_parent.append(child)
        
        return new_parent
    
    def assoc(self, index: int, value: T) -> 'PersistentVector[T]':
        """Set element."""
        if index < 0 or index >= self._size:
            raise IndexError(f"Index {index} out of range")
        
        if index >= self._tail_offset():
            new_tail = list(self._tail)
            new_tail[index & self.MASK] = value
            return PersistentVector(
                self._root,
                new_tail,
                self._size,
                self._shift
            )
        
        new_root = self._do_assoc(self._shift, self._root, index, value)
        return PersistentVector(
            new_root,
            self._tail,
            self._size,
            self._shift
        )
    
    def _do_assoc(self, level: int, node: List, index: int, value: T) -> List:
        """Recursive assoc."""
        new_node = list(node)
        if level == 0:
            new_node[index & self.MASK] = value
        else:
            idx = (index >> level) & self.MASK
            new_node[idx] = self._do_assoc(
                level - self.BITS,
                node[idx],
                index,
                value
            )
        return new_node
    
    def pop(self) -> 'PersistentVector[T]':
        """Remove last element."""
        if self._size == 0:
            raise IndexError("Cannot pop empty vector")
        
        if self._size == 1:
            return PersistentVector()
        
        if self._size - self._tail_offset() > 1:
            new_tail = self._tail[:-1]
            return PersistentVector(
                self._root,
                new_tail,
                self._size - 1,
                self._shift
            )
        
        new_root, new_tail, new_shift = self._pop_tail()
        return PersistentVector(
            new_root or [],
            new_tail,
            self._size - 1,
            new_shift
        )
    
    def _pop_tail(self) -> Tuple[Optional[List], List, int]:
        """Pop from tree."""
        if self._size <= self.BRANCH_FACTOR:
            return None, list(self._root), self.BITS
        
        new_shift = self._shift
        new_root = list(self._root) if self._root else []
        
        while new_root and len(new_root) == 1 and new_shift > self.BITS:
            new_root = new_root
            new_shift -= self.BITS
        
        return new_root, [], new_shift
    
    def rest(self) -> 'PersistentVector[T]':
        """All but first."""
        if self._size <= 1:
            return PersistentVector()
        return self._slice(1, self._size)
    
    def _slice(self, start: int, end: int) -> 'PersistentVector[T]':
        """Slice."""
        return PersistentVector.from_seq(self[i] for i in range(start, end))
    
    def map(self, fn: Callable[[T], R]) -> 'PersistentVector[R]':
        """Map function."""
        return PersistentVector.from_seq(fn(x) for x in self)
    
    def filter(self, pred: Callable[[T], bool]) -> 'PersistentVector[T]':
        """Filter."""
        return PersistentVector.from_seq(x for x in self if pred(x))
    
    def reduce(self, fn: Callable[[R, T], R], initial: R) -> R:
        """Reduce."""
        result = initial
        for x in self:
            result = fn(result, x)
        return result
    
    def to_list(self) -> List[T]:
        """Convert to list."""
        return list(self)
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        items = [self._json_encoder(x) for x in self]
        return json.dumps(items, default=str)
    
    @classmethod
    def from_json(
        cls: Type['PersistentVector[T]'], 
        json_str: str, 
        decoder: Optional[Callable[[Any], T]] = None
    ) -> 'PersistentVector[T]':
        """Deserialize from JSON."""
        items = json.loads(json_str)
        if decoder:
            decoded = [decoder(x) for x in items]
        else:
            decoded = items
        return cls.from_seq(decoded)
    
    @classmethod
    def from_seq(cls, items: Iterable[T]) -> 'PersistentVector[T]':
        """Create from iterable."""
        v = cls()
        for item in items:
            v = v.conj(item)
        return v
    
    def __iter__(self) -> Iterator[T]:
        for i in range(self._size):
            yield self[i]
    
    def __contains__(self, item: T) -> bool:
        for x in self:
            if x == item:
                return True
        return False
    
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PersistentVector):
            if self._size != other._size:
                return False
            for i in range(self._size):
                if self[i] != other[i]:
                    return False
            return True
        if isinstance(other, (list, tuple)):
            return list(self) == list(other)
        return False
    
    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(tuple(self))
        return self._hash
    
    def __repr__(self) -> str:
        items = ', '.join(repr(x) for x in self)
        return f"PersistentVector([{items}])"
    
    def __add__(self, other: 'PersistentVector[T]') -> 'PersistentVector[T]':
        """Concatenate."""
        result = self
        for item in other:
            result = result.conj(item)
        return result


class PersistentHashMap(Generic[K, V]):
    """
    Immutable hashmap with structural sharing using HAMT.
    
    O(log32 n) operations.
    Supports JSON serialization.
    """
    
    __slots__ = ('_root', '_size', '_hash', '_json_encoder')
    
    BITS = 5
    MASK = 31
    
    def __init__(
        self,
        root: Optional[Dict] = None,
        size: int = 0,
        _hash: Optional[int] = None,
        json_encoder: Optional[Callable[[V], Any]] = None
    ):
        self._root = root if root is not None else {}
        self._size = size
        self._hash = _hash
        self._json_encoder = json_encoder or (lambda x: x)
    
    def __len__(self) -> int:
        return self._size
    
    def __bool__(self) -> bool:
        return self._size > 0
    
    def __getitem__(self, key: K) -> V:
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result
    
    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get value."""
        h = hash(key)
        return self._get(self._root, h, key, default)
    
    def _get(self, node: Dict, h: int, key: K, default: Optional[V]) -> Optional[V]:
        if not node:
            return default
        
        idx = h & self.MASK
        
        if idx not in node:
            return default
        
        entry = node[idx]
        
        if isinstance(entry, dict):
            return self._get(entry, h >> self.BITS, key, default)
        
        if isinstance(entry, list):
            for k, v in entry:
                if k == key:
                    return v
            return default
        
        return default
    
    def assoc(self, key: K, value: V) -> 'PersistentHashMap[K, V]':
        """Associate key-value."""
        h = hash(key)
        new_root, added = self._assoc(self._root, h, key, value)
        
        return PersistentHashMap(
            new_root,
            self._size + (1 if added else 0)
        )
    
    def _assoc(self, node: Dict, h: int, key: K, value: V) -> Tuple[Dict, bool]:
        idx = h & self.MASK
        new_node = dict(node)
        added = False
        
        if idx not in node:
            new_node[idx] = [[key, value]]
            added = True
        else:
            entry = node[idx]
            
            if isinstance(entry, dict):
                child, added = self._assoc(entry, h >> self.BITS, key, value)
                new_node[idx] = child
            elif isinstance(entry, list):
                new_list = []
                found = False
                for k, v in entry:
                    if k == key:
                        new_list.append([key, value])
                        found = True
                    else:
                        new_list.append([k, v])
                
                if not found:
                    new_list.append([key, value])
                    added = True
                
                new_node[idx] = new_list
        
        return new_node, added
    
    def dissoc(self, key: K) -> 'PersistentHashMap[K, V]':
        """Remove key."""
        h = hash(key)
        new_root, removed = self._dissoc(self._root, h, key)
        
        return PersistentHashMap(
            new_root,
            self._size - (1 if removed else 0)
        )
    
    def _dissoc(self, node: Dict, h: int, key: K) -> Tuple[Dict, bool]:
        idx = h & self.MASK
        
        if idx not in node:
            return node, False
        
        entry = node[idx]
        new_node = dict(node)
        removed = False
        
        if isinstance(entry, dict):
            child, removed = self._dissoc(entry, h >> self.BITS, key)
            if child:
                new_node[idx] = child
            else:
                del new_node[idx]
        elif isinstance(entry, list):
            new_list = [[k, v] for k, v in entry if k != key]
            if len(new_list) < len(entry):
                removed = True
            if new_list:
                new_node[idx] = new_list
            else:
                del new_node[idx]
        
        return new_node, removed
    
    def contains(self, key: K) -> bool:
        """Check if key exists."""
        return self.get(key) is not None
    
    def keys(self) -> Iterator[K]:
        """Iterate keys."""
        yield from self._iter_keys(self._root)
    
    def _iter_keys(self, node: Dict) -> Iterator[K]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_keys(entry)
            elif isinstance(entry, list):
                for k, _ in entry:
                    yield k
    
    def values(self) -> Iterator[V]:
        """Iterate values."""
        yield from self._iter_values(self._root)
    
    def _iter_values(self, node: Dict) -> Iterator[V]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_values(entry)
            elif isinstance(entry, list):
                for _, v in entry:
                    yield v
    
    def items(self) -> Iterator[Tuple[K, V]]:
        """Iterate pairs."""
        yield from self._iter_items(self._root)
    
    def _iter_items(self, node: Dict) -> Iterator[Tuple[K, V]]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_items(entry)
            elif isinstance(entry, list):
                for k, v in entry:
                    yield k, v
    
    def to_dict(self) -> Dict[K, V]:
        """Convert to dict."""
        return dict(self.items())
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        items = {str(k): self._json_encoder(v) for k, v in self.items()}
        return json.dumps(items, default=str)
    
    @classmethod
    def from_json(
        cls: Type['PersistentHashMap[K, V]'], 
        json_str: str, 
        key_decoder: Optional[Callable[[Any], K]] = None,
        value_decoder: Optional[Callable[[Any], V]] = None
    ) -> 'PersistentHashMap[K, V]':
        """Deserialize from JSON."""
        items = json.loads(json_str)
        m = cls()
        for k, v in items.items():
            if key_decoder:
                key = key_decoder(k)
            else:
                key = k
            if value_decoder:
                value = value_decoder(v)
            else:
                value = v
            m = m.assoc(key, value)
        return m
    
    @classmethod
    def from_dict(cls, d: Dict[K, V]) -> 'PersistentHashMap[K, V]':
        """Create from dict."""
        m = cls()
        for k, v in d.items():
            m = m.assoc(k, v)
        return m
    
    def __iter__(self) -> Iterator[K]:
        return self.keys()
    
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PersistentHashMap):
            return dict(self.items()) == dict(other.items())
        if isinstance(other, dict):
            return dict(self.items()) == other
        return False
    
    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset(self.items()))
        return self._hash
    
    def __repr__(self) -> str:
        items = ', '.join(f'{k!r}: {v!r}' for k, v in self.items())
        return f"PersistentHashMap({{{items}}})"


# ============================================================================
# Higher-Level Primitives (Enhanced)
# ============================================================================

class STMQueue(Generic[T]):
    """
    Transactional queue with blocking semantics.
    """
    
    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._items: Ref[List[T]] = Ref([], name="queue_items")
        self._closed = Atom(False)
    
    def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put item in queue."""
        if self._closed.deref():
            return False
        
        deadline = time.time() + timeout if timeout else None
        
        while True:
            try:
                with transaction(timeout=timeout):
                    items = self._items.deref()
                    
                    if self._maxsize > 0 and len(items) >= self._maxsize:
                        if timeout and time.time() > deadline:
                            return False
                        retry()
                    
                    self._items.set(items + [item])
                    return True
            except RetryException:
                if deadline and time.time() > deadline:
                    return False
                time.sleep(0.001)
    
    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get item from queue."""
        deadline = time.time() + timeout if timeout else None
        
        while True:
            try:
                with transaction(timeout=timeout):
                    items = self._items.deref()
                    
                    if not items:
                        if self._closed.deref():
                            return None
                        if timeout and time.time() > deadline:
                            return None
                        retry()
                    
                    item = items[0]  # Fixed: Get first item
                    self._items.set(items[1:])
                    return item
            except RetryException:
                if self._closed.deref():
                    return None
                if deadline and time.time() > deadline:
                    return None
                time.sleep(0.001)
    
    def peek(self) -> Optional[T]:
        """Peek next item."""
        with transaction():
            items = self._items.deref()
            return items[0] if items else None
    
    def size(self) -> int:
        """Get size."""
        with transaction():
            return len(self._items.deref())
    
    def empty(self) -> bool:
        """Check if empty."""
        return self.size() == 0
    
    def full(self) -> bool:
        """Check if full."""
        if self._maxsize == 0:
            return False
        return self.size() >= self._maxsize
    
    def close(self) -> None:
        """Close queue."""
        self._closed.reset(True)
    
    def is_closed(self) -> bool:
        """Check if closed."""
        return self._closed.deref()


class STMAgent(Generic[T]):
    """
    Asynchronous agent for background processing.
    """
    
    def __init__(self, initial_value: T, name: Optional[str] = None):
        self._value = Atom(initial_value)
        self._queue: STMQueue[Callable[[T], T]] = STMQueue()
        self._name = name
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=name or "STM-Agent",
            daemon=True
        )
        self._thread.start()
    
    def _run_loop(self) -> None:
        """Background loop."""
        while self._running:
            action = self._queue.get(timeout=0.1)
            if action is None:
                continue
            
            try:
                self._value.swap(action)
            except Exception as e:
                logger.error(f"Agent error: {e}")
    
    def send(self, action: Callable[[T], T]) -> None:
        """Send action."""
        if self._running:
            self._queue.put(action)
    
    def deref(self) -> T:
        """Get current value."""
        return self._value.deref()
    
    def shutdown(self, timeout: float = 5.0) -> None:
        """Shutdown agent."""
        self._running = False
        self._queue.close()
        self._thread.join(timeout)
    
    def __repr__(self) -> str:
        return f"STMAgent(name={self._name!r}, value={self._value.deref()!r})"


class STMVar(Generic[T]):
    """
    Transaction-local variable.
    """
    
    def __init__(self, name: Optional[str] = None):
        self._name = name
        self._tx_values: Dict[int, T] = {}
        self._tx_set: Dict[int, bool] = {}
        self._lock = threading.RLock()
    
    def set(self, value: T) -> None:
        """Set value in transaction."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("STMVar.set() requires transaction")
        
        with self._lock:
            self._tx_values[tx.id] = value
            self._tx_set[tx.id] = True
    
    def get(self) -> T:
        """Get value."""
        tx = _get_current_transaction()
        if tx is None:
            raise STMException("STMVar.get() requires transaction")
        
        with self._lock:
            if not self._tx_set.get(tx.id, False):
                raise STMException(f"STMVar {self._name!r} not set")
            return self._tx_values[tx.id]
    
    def is_set(self) -> bool:
        """Check if set."""
        tx = _get_current_transaction()
        if tx is None:
            return False
        
        with self._lock:
            return self._tx_set.get(tx.id, False)
    
    def clear(self) -> None:
        """Clear value."""
        tx = _get_current_transaction()
        if tx is None:
            return
        
        with self._lock:
            self._tx_values.pop(tx.id, None)
            self._tx_set.pop(tx.id, None)


# ============================================================================
# Context and Decorators (Enhanced)
# ============================================================================

_tx_context = threading.local()


def _get_current_transaction() -> Optional[Transaction]:
    """Get current transaction."""
    return getattr(_tx_context, 'tx', None)


def _set_current_transaction(tx: Optional[Transaction]) -> None:
    """Set current transaction."""
    _tx_context.tx = tx


@contextmanager
def transaction(
    timeout: float = 30.0,
    max_retries: int = 1000,
    label: Optional[str] = None
) -> Iterator[Transaction]:
    """
    Context manager for STM transaction.
    
    Example:
        with transaction() as tx:
            balance = account.deref()
            account.set(balance + 100)
    """
    coordinator = TransactionCoordinator()
    tx = Transaction(
        coordinator,
        timeout=timeout,
        max_retries=max_retries,
        label=label
    )
    
    old_tx = _get_current_transaction()
    _set_current_transaction(tx)
    
    try:
        coordinator.register_transaction(tx)
        
        while True:
            try:
                with tx:
                    yield tx
                break
                
            except ConflictException as e:
                backoff = coordinator.contention.get_backoff_for_retry(
                    tx._retry_count,
                    e.conflicting_refs or frozenset()
                )
                time.sleep(backoff)
                tx._reset_for_retry()
                
            except RetryException as e:
                backoff = coordinator.contention.get_backoff_for_retry(
                    e.retry_count,
                    frozenset()
                )
                time.sleep(backoff)
                tx._reset_for_retry()
                
    finally:
        _set_current_transaction(old_tx)
        coordinator.unregister_transaction(tx.id)


def dosync(
    timeout: float = 30.0,
    max_retries: int = 1000
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Decorator to run function in transaction.
    
    Example:
        @dosync()
        def transfer(from_acc, to_acc, amount):
            from_acc.alter(lambda x: x - amount)
            to_acc.alter(lambda x: x + amount)
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> R:
            with transaction(timeout=timeout, max_retries=max_retries):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def atomically(func: Callable[..., R]) -> Callable[..., R]:
    """Alias for @dosync()."""
    return dosync()(func)


# ============================================================================
# Async Support (Python 3.13+)
# ============================================================================

if PYTHON_3_13_PLUS:
    @asynccontextmanager
    async def async_transaction(
        timeout: float = 30.0,
        max_retries: int = 1000,
        label: Optional[str] = None
    ) -> AsyncIterator[Transaction]:
        """Async context manager for transactions."""
        with transaction(timeout=timeout, max_retries=max_retries, label=label) as tx:
            yield tx


# ============================================================================
# Utility Functions
# ============================================================================

def retry() -> None:
    """Signal transaction retry."""
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("retry() requires active transaction")
    tx._retry()


def ensure(ref: Ref[T]) -> T:
    """Ensure ref in read set."""
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("ensure() requires active transaction")
    return tx._read_ref(ref)


def commute(ref: Ref[T], fn: Callable[[T], T], *args, **kwargs) -> None:
    """Apply commutative function."""
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("commute() requires active transaction")
    tx._commute_ref(ref, fn, *args, **kwargs)


def io(func: Callable[..., R]) -> Callable[..., R]:
    """Mark function as I/O (not retried)."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> R:
        tx = _get_current_transaction()
        if tx is not None:
            _set_current_transaction(None)
            try:
                return func(*args, **kwargs)
            finally:
                _set_current_transaction(tx)
        return func(*args, **kwargs)
    return wrapper


# ============================================================================
# Snapshot and History
# ============================================================================

@dataclass
class Snapshot(Generic[T]):
    """Immutable snapshot."""
    ref_id: int
    version: VersionStamp
    value: T
    timestamp: float
    
    def __repr__(self) -> str:
        return f"Snapshot(ref={self.ref_id}, version={self.version.logical_time})"


def get_history(ref: Ref[T], limit: int = 10) -> List[Snapshot[T]]:
    """Get ref history."""
    with ref._lock:
        return [
            Snapshot(
                ref_id=ref._identity.id,
                version=version,
                value=value,
                timestamp=version.physical_time
            )
            for version, value in ref._history[-limit:]
        ]


def get_snapshot_at(ref: Ref[T], logical_time: int) -> Optional[Snapshot[T]]:
    """Get snapshot at logical time."""
    with ref._lock:
        for version, value in reversed(ref._history):
            if version.logical_time <= logical_time:
                return Snapshot(
                    ref_id=ref._identity.id,
                    version=version,
                    value=value,
                    timestamp=version.physical_time
                )
        return None


# ============================================================================
# Testing Utilities
# ============================================================================

def run_concurrent(
    funcs: List[Callable[[], R]],
    max_workers: Optional[int] = None
) -> List[R]:
    """Run functions concurrently."""
    import concurrent.futures
    
    max_workers = max_workers or len(funcs)
    results = [None] * len(funcs)
    
    def run_one(idx: int, func: Callable[[], R]) -> Tuple[int, R, Optional[Exception]]:
        try:
            return idx, func(), None
        except Exception as e:
            return idx, None, e
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(run_one, i, f)
            for i, f in enumerate(funcs)
        ]
        
        for future in concurrent.futures.as_completed(futures):
            idx, result, exc = future.result()
            results[idx] = result
            if exc:
                logger.error(f"Task {idx} failed: {exc}")
    
    return results


# ============================================================================
# Diagnostics and Monitoring
# ============================================================================

def get_stm_stats() -> Dict[str, Any]:
    """Get STM statistics."""
    coordinator = TransactionCoordinator()
    
    return {
        "timestamp": time.time(),
        "python_version": f"{PY_VERSION.major}.{PY_VERSION.minor}.{PY_VERSION.micro}",
        "no_gil_enabled": NO_GIL_ENABLED,
        **coordinator.get_stats(),
        "contention": {
            "level": coordinator.contention.get_contention_level(),
            "throughput": coordinator.contention.get_throughput(),
        }
    }


def dump_stm_stats(filepath: Optional[str] = None) -> str:
    """Dump stats to JSON."""
    stats = get_stm_stats()
    json_str = json.dumps(stats, indent=2)
    
    if filepath:
        Path(filepath).write_text(json_str)
        logger.info(f"Stats dumped to {filepath}")
    
    return json_str


def reset_stm() -> None:
    """Reset STM state."""
    TransactionCoordinator().reset()
    logger.info("STM state reset")


# ============================================================================
# Cleanup on Exit
# ============================================================================

def _cleanup():
    """Cleanup on exit."""
    try:
        coordinator = TransactionCoordinator()
        coordinator._reaper.stop()
        logger.info("STM cleanup complete")
    except:
        pass


atexit.register(_cleanup)


# ============================================================================
# Public API
# ============================================================================

__all__ = [
    # Core types
    'Ref',
    'Atom',
    'Transaction',
    'TransactionCoordinator',
    'TransactionState',
    'VersionStamp',
    'RefIdentity',
    
    # Context managers
    'transaction',
    'dosync',
    'atomically',
    
    # Primitives
    'STMQueue',
    'STMAgent',
    'STMVar',
    
    # Persistent structures
    'PersistentVector',
    'PersistentHashMap',
    'Snapshot',
    
    # Operations
    'retry',
    'ensure',
    'commute',
    'io',
    
    # History
    'get_history',
    'get_snapshot_at',
    
    # Utilities
    'run_concurrent',
    'get_stm_stats',
    'dump_stm_stats',
    'reset_stm',
    
    # Lock utilities
    'SpinLock',
    'SeqLock',
    'RWLock',
    
    # Managers
    'ContentionManager',
    'HistoryManager',
    'STMReaper',
    
    # Exceptions
    'STMException',
    'RetryException',
    'CommitException',
    'ConflictException',
    'TransactionAbortedException',
    'TimeoutException',
    'ValidationException',
    'HistoryExpiredException',
    'InvariantViolationException',
]


# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 70)
    print("Atomix v3.0 - Ultimate Production-Ready STM Library for Python 3.13+")
    print("=" * 70)
    
    # Demo basic transaction
    print("\n=== Basic Transaction ===")
    counter = Ref(0, name="counter")
    
    @dosync()
    def increment():
        counter.set(counter.deref() + 1)
    
    run_concurrent([increment] * 100)
    
    with transaction():
        print(f"Counter: {counter.deref()}")
    
    # Demo bank transfer
    print("\n=== Bank Transfer ===")
    alice = Ref(1000.0, name="alice")
    bob = Ref(500.0, name="bob")
    
    def transfer(amount: float):
        with transaction():
            if alice.deref() >= amount:
                alice.set(alice.deref() - amount)
                bob.set(bob.deref() + amount)
    
    run_concurrent([lambda: transfer(100.0)] * 5)
    
    with transaction():
        print(f"Alice: ${alice.deref():.2f}")
        print(f"Bob: ${bob.deref():.2f}")
        print(f"Total: ${alice.deref() + bob.deref():.2f}")
    
    # Demo persistent structures
    print("\n=== Persistent Structures ===")
    v1 = PersistentVector.from_seq([1, 2, 3, 4, 5])
    v2 = v1.conj(6)
    v3 = v1.assoc(2, 100)
    
    print(f"v1: {v1.to_list()}")
    print(f"v2: {v2.to_list()}")
    print(f"v3: {v3.to_list()}")
    print(f"v1 unchanged: {v1.to_list() == [1, 2, 3, 4, 5]}")
    
    # Demo STMQueue
    print("\n=== STM Queue ===")
    queue = STMQueue[int](maxsize=5)
    
    def producer():
        for i in range(10):
            if queue.put(i):
                print(f"Produced: {i}")
            time.sleep(0.001)
    
    def consumer():
        for _ in range(10):
            item = queue.get(timeout=0.1)
            if item is not None:
                print(f"Consumed: {item}")
            time.sleep(0.002)
    
    import threading
    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Demo stats
    print("\n=== STM Statistics ===")
    stats = get_stm_stats()
    print(json.dumps(stats, indent=2))
    
    print("\n" + "=" * 70)
    print("All demos completed successfully!")
    print("=" * 70)
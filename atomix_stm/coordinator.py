import atexit
import itertools
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, cast
from .versioning import VersionStamp

PY_VERSION = sys.version_info
PYTHON_3_13_PLUS = PY_VERSION >= (3, 13)

try:
    NO_GIL_ENABLED = not sys._is_gil_enabled()
except AttributeError:
    NO_GIL_ENABLED = os.getenv("PYTHON_GIL") == "0"


class _SafeStreamHandler(logging.StreamHandler[Any]):
    """Logging handler safeguarding against closed files during interpreter shutdown."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except (ValueError, OSError):
            pass

    def handleError(self, record: logging.LogRecord) -> None:
        return


logger = logging.getLogger("atomix_stm.core")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = _SafeStreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(handler)


def _safe_log(level: int, msg: str) -> None:
    try:
        logger.log(level, msg)
    except (ValueError, OSError):
        pass


def _safe_log_info(msg: str) -> None:
    _safe_log(logging.INFO, msg)


def _safe_log_debug(msg: str) -> None:
    _safe_log(logging.DEBUG, msg)


def _safe_log_warning(msg: str) -> None:
    _safe_log(logging.WARNING, msg)


def _safe_log_error(msg: str) -> None:
    _safe_log(logging.ERROR, msg)


class ContentionManager:
    """Adaptive contention management preventing livelock."""

    def __init__(
        self,
        base_backoff: float = 0.0001,
        max_backoff: float = 0.1,
        jitter_factor: float = 0.3,
        enable_priority: bool = True,
        throughput_window: float = 60.0,
    ) -> None:
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
        self._history_window: List[float] = []
        self._max_history = 1000
        self._last_throughput = 0.0

    def record_conflict(self, ref_ids: Set[int], retry_count: int = 0) -> float:
        import random

        with self._contention_lock:
            self._global_contention += 1
            self._total_retries += 1
            max_score = 0
            for ref_id in ref_ids:
                self._contention_scores[ref_id] += 1
                max_score = max(max_score, self._contention_scores[ref_id])
            self._history_window.append(time.time())
            if len(self._history_window) > self._max_history:
                self._history_window.pop(0)

        exponent = min(max_score + retry_count, 15)
        backoff = self._base_backoff * (2**exponent)
        backoff = min(backoff, self._max_backoff)
        jitter = backoff * self._jitter_factor * random.random()
        return cast(float, backoff + jitter)

    def record_success(self, ref_ids: Set[int]) -> None:
        with self._contention_lock:
            self._total_commits += 1
            self._global_contention = max(0, self._global_contention - 1)
            for ref_id in ref_ids:
                self._contention_scores[ref_id] = max(
                    0, self._contention_scores[ref_id] - 1
                )

    def get_contention_level(self) -> float:
        with self._contention_lock:
            total = self._total_commits + self._total_retries
            if total == 0:
                return 0.0
            return min(1.0, self._total_retries / total)

    def get_throughput(self, window_seconds: Optional[float] = None) -> float:
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
        if not self._enable_priority:
            return False
        with self._contention_lock:
            score = self._contention_scores.get(ref_id, 0)
            if score < 3:
                return False
            return (tx_id % (score + 1)) != 0

    def get_backoff_for_retry(self, retry_count: int, ref_ids: Set[int]) -> float:
        import random

        with self._contention_lock:
            max_score = max(
                (self._contention_scores.get(rid, 0) for rid in ref_ids), default=0
            )
        exponent = min(retry_count + max_score, 15)
        backoff = self._base_backoff * (2**exponent)
        backoff = min(backoff, self._max_backoff)
        jitter = backoff * self._jitter_factor * random.random()
        return cast(float, backoff + jitter)

    def get_metrics(self) -> Dict[str, Any]:
        with self._contention_lock:
            return {
                "contention_level": self.get_contention_level(),
                "throughput": self.get_throughput(),
                "total_retries": self._total_retries,
                "total_commits": self._total_commits,
                "global_contention": self._global_contention,
            }

    def reset(self) -> None:
        with self._contention_lock:
            self._contention_scores.clear()
            self._global_contention = 0
            self._total_retries = 0
            self._total_commits = 0
            self._history_window.clear()
            self._last_throughput = 0.0


class HistoryManager:
    """Adaptive history retention based on active snapshots, access rates, and memory limits."""

    def __init__(
        self,
        default_max_history: int = 100,
        min_history: int = 5,
        memory_threshold_percent: float = 80.0,
        access_threshold: int = 100,
    ) -> None:
        self._default_max = default_max_history
        self._min_history = min_history
        self._memory_threshold = memory_threshold_percent / 100.0
        self._access_threshold = access_threshold
        self._psutil_available = False
        try:
            import psutil  # type: ignore[import-untyped] # noqa: F401

            self._psutil_available = True
        except ImportError:
            pass
        self._active_snapshots: Dict[int, Tuple[VersionStamp, float]] = {}
        self._snapshots_lock = threading.RLock()
        self._access_patterns: Dict[int, List[float]] = defaultdict(list)
        self._patterns_lock = threading.RLock()
        self._cleanups_performed = 0
        self._snapshots_removed = 0
        self._memory_pressure_events = 0

    def register_snapshot(self, tx_id: int, version: VersionStamp) -> None:
        with self._snapshots_lock:
            self._active_snapshots[tx_id] = (version, time.time())

    def unregister_snapshot(self, tx_id: int) -> None:
        with self._snapshots_lock:
            self._active_snapshots.pop(tx_id, None)

    def get_retention_bound(self) -> int:
        with self._snapshots_lock:
            if not self._active_snapshots:
                return 0
            return min(
                (v.logical_time for v, _ in self._active_snapshots.values()), default=0
            )

    def compute_max_history(self, ref_id: int) -> int:
        base = self._default_max
        with self._patterns_lock:
            accesses = self._access_patterns.get(ref_id, [])
            if len(accesses) > 10:
                elapsed = time.time() - accesses[0]
                if elapsed > 0:
                    recent_rate = len(accesses) / elapsed
                    if recent_rate > self._access_threshold:
                        base = int(base * 1.5)
        if self._psutil_available:
            try:
                import psutil

                memory_percent = psutil.virtual_memory().percent / 100.0
                if memory_percent > self._memory_threshold:
                    base = max(self._min_history, int(base * 0.5))
                    self._memory_pressure_events += 1
            except (ImportError, AttributeError):
                self._psutil_available = False
        return base

    def record_access(self, ref_id: int) -> None:
        with self._patterns_lock:
            accesses = self._access_patterns[ref_id]
            accesses.append(time.time())
            cutoff = time.time() - 60.0
            self._access_patterns[ref_id] = [t for t in accesses if t > cutoff]

    def cleanup_stale_snapshots(self, timeout: float = 300.0) -> int:
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
        with self._snapshots_lock:
            active = len(self._active_snapshots)
        with self._patterns_lock:
            tracked_refs = len(self._access_patterns)
        return {
            "active_snapshots": active,
            "tracked_refs": tracked_refs,
            "cleanups_performed": self._cleanups_performed,
            "snapshots_removed": self._snapshots_removed,
            "memory_pressure_events": self._memory_pressure_events,
        }

    def reset(self) -> None:
        with self._snapshots_lock:
            self._active_snapshots.clear()
        with self._patterns_lock:
            self._access_patterns.clear()
        self._cleanups_performed = 0
        self._snapshots_removed = 0
        self._memory_pressure_events = 0


class STMReaper(threading.Thread):
    """Background daemon thread trimming history versions and cleaning stale snapshots."""

    def __init__(
        self,
        coordinator: "TransactionCoordinator",
        interval: float = 5.0,
        batch_size: int = 100,
    ) -> None:
        super().__init__(name="STM-Reaper", daemon=True)
        self.coordinator = coordinator
        self.interval = interval
        self.batch_size = batch_size
        self._cleanup_count = 0
        self._stop_event = threading.Event()

    def run(self) -> None:
        _safe_log_info("STM Reaper started")
        while not self._stop_event.is_set():
            try:
                self.coordinator.history.cleanup_stale_snapshots(timeout=60.0)
                refs = self.coordinator.get_refs_snapshot()
                retention_bound = self.coordinator.history.get_retention_bound()
                for i in range(0, len(refs), self.batch_size):
                    batch = refs[i : i + self.batch_size]
                    for ref in batch:
                        try:
                            ref._trim_history(retention_bound)
                        except Exception as e:
                            _safe_log_error(f"Error trimming ref {ref.id}: {e}")
                    if i > 0 and i % (self.batch_size * 2) == 0:
                        if self._stop_event.wait(0.001):
                            return
                self._cleanup_count += 1
                if self._stop_event.wait(self.interval):
                    return
            except Exception as e:
                _safe_log_error(f"Reaper error: {e}")
                if self._stop_event.wait(self.interval):
                    return

    def stop(self, timeout: float = 2.0) -> bool:
        self._stop_event.set()
        self.join(timeout=timeout)
        _safe_log_info("STM Reaper stopped")
        return not self.is_alive()


class TransactionCoordinator:
    """Coordinates transaction execution, registration, clock timing and diagnostics."""

    _instance: Optional["TransactionCoordinator"] = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "TransactionCoordinator":
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
        self._refs: Dict[int, Any] = {}
        self._refs_lock = threading.RLock()
        self._active_txs: Dict[int, Any] = {}
        self._txs_lock = threading.RLock()
        self._current_epoch = 0
        self._epoch_lock = threading.Lock()
        self._contention_manager = ContentionManager()
        self._history_manager = HistoryManager()
        self._commit_lock = threading.RLock()
        self._reaper = STMReaper(self, interval=5.0)
        self._reaper.start()
        atexit.register(self._reaper.stop)

        self._stats_lock = threading.RLock()
        self._stats = {
            "total_transactions": 0,
            "total_commits": 0,
            "total_aborts": 0,
            "total_conflicts": 0,
        }
        _safe_log_info("TransactionCoordinator initialized")

    def reset(self) -> None:
        with self._init_lock:
            self._logical_clock = 0
            self._current_epoch = 0
            self._refs.clear()
            self._active_txs.clear()
            self._stats = {
                "total_transactions": 0,
                "total_commits": 0,
                "total_aborts": 0,
                "total_conflicts": 0,
            }
            self._contention_manager.reset()
            self._history_manager.reset()
            if hasattr(self, "_reaper") and self._reaper.is_alive():
                self._reaper.stop()
            self._reaper = STMReaper(self, interval=5.0)
            self._reaper.start()
            atexit.register(self._reaper.stop)

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
            epoch=self._current_epoch,
            logical_time=self.advance_clock(),
            transaction_id=tx_id,
            physical_time=time.time(),
        )

    def register_ref(self, ref: Any) -> int:
        ref_id = self.new_ref_id()
        with self._refs_lock:
            self._refs[ref_id] = ref
        return ref_id

    def unregister_ref(self, ref_id: int) -> None:
        with self._refs_lock:
            self._refs.pop(ref_id, None)

    def get_ref(self, ref_id: int) -> Optional[Any]:
        with self._refs_lock:
            return self._refs.get(ref_id)

    def register_transaction(self, tx: Any) -> None:
        with self._txs_lock:
            self._active_txs[tx.id] = tx
        self._history_manager.register_snapshot(tx.id, tx.snapshot_version)

    def unregister_transaction(self, tx_id: int) -> None:
        with self._txs_lock:
            self._active_txs.pop(tx_id, None)
        self._history_manager.unregister_snapshot(tx_id)

    def get_active_transactions(self) -> List[Any]:
        with self._txs_lock:
            return list(self._active_txs.values())

    def detect_conflicts(
        self, tx: Any, read_set: Dict[int, VersionStamp], write_set: Set[int]
    ) -> Optional[FrozenSet[int]]:
        conflicting = set()
        for ref_id, read_version in read_set.items():
            ref = self.get_ref(ref_id)
            if ref is None:
                continue
            current_version = ref._get_version()
            if current_version != read_version:
                conflicting.add(ref_id)
        return frozenset(conflicting) if conflicting else None

    def record_conflict(self, conflicting_refs: FrozenSet[int]) -> None:
        with self._stats_lock:
            self._stats["total_conflicts"] += len(conflicting_refs)

    def record_commit(self) -> None:
        with self._stats_lock:
            self._stats["total_commits"] += 1

    def record_abort(self) -> None:
        with self._stats_lock:
            self._stats["total_aborts"] += 1

    @property
    def contention(self) -> ContentionManager:
        return self._contention_manager

    @property
    def history(self) -> HistoryManager:
        return self._history_manager

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

    def get_refs_snapshot(self) -> List[Any]:
        with self._refs_lock:
            return list(self._refs.values())


def _cleanup() -> None:
    try:
        coordinator = TransactionCoordinator()
        coordinator._reaper.stop()
        from .primitives import STMAgent

        if hasattr(STMAgent, "_agent_pool") and STMAgent._agent_pool is not None:
            STMAgent._agent_pool.shutdown(wait=False)
        _safe_log_info("STM cleanup complete")
    except Exception as e:
        _safe_log_debug(f"Cleanup error (safe to ignore): {e}")


atexit.register(_cleanup)

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, Generic, List, Optional, Set, Tuple, TypeVar
from .versioning import TransactionState, VersionStamp
from .exceptions import STMException, ConflictException, TimeoutException, TransactionAbortedException

T = TypeVar('T')

@dataclass
class ReadLogEntry:
    ref_id: int
    version_read: VersionStamp
    value_hash: int


@dataclass
class WriteLogEntry(Generic[T]):
    ref_id: int
    old_value: Optional[T]
    new_value: T
    old_version: Optional[VersionStamp]
    is_commutative: bool = False
    commutative_fn: Optional[Callable[[T], T]] = None


@dataclass
class CommuteEntry(Generic[T]):
    ref_id: int
    function: Callable[[T], T]
    args: Tuple
    kwargs: Dict[str, Any]


class Transaction:
    """STM transaction coordinating commits under a global commit lock for MVCC consistency."""
    def __init__(
        self,
        coordinator: Any,
        timeout: float = 30.0,
        max_retries: int = 1000,
        label: Optional[str] = None
    ) -> None:
        self._coordinator = coordinator
        self.id = coordinator.new_transaction_id()
        self._label = label
        self._state = TransactionState.ACTIVE
        self._timeout = timeout
        self._max_retries = max_retries
        self._start_time = time.time()
        self.snapshot_version = coordinator.create_version_stamp(self.id)
        
        self._read_log: Dict[int, ReadLogEntry] = {}
        self._write_log: Dict[int, WriteLogEntry] = {}
        self._commutes: Dict[int, List[CommuteEntry]] = {}
        self._retry_count = 0
        self._lock = threading.RLock()
        self._depth = 0

    @property
    def elapsed_time(self) -> float:
        return time.time() - self._start_time

    def _check_active(self) -> None:
        if self._state != TransactionState.ACTIVE:
            if self._state == TransactionState.ABORTED:
                raise TransactionAbortedException(f"Transaction {self.id} has been aborted")
            raise STMException(f"Transaction {self.id} is in state {self._state.name}")
        if self.elapsed_time > self._timeout:
            self._state = TransactionState.ABORTED
            raise TimeoutException(f"Transaction {self.id} timed out")

    def _read_ref(self, ref: Any) -> Any:
        with self._lock:
            self._check_active()
            ref_id = ref.id
            if ref_id in self._write_log:
                return self._write_log[ref_id].new_value
            value, version = ref._read_at_version(self.snapshot_version)
            try:
                val_hash = hash(value)
            except TypeError:
                val_hash = id(value)
            self._read_log[ref_id] = ReadLogEntry(
                ref_id=ref_id,
                version_read=version,
                value_hash=val_hash
            )
            return value

    def _write_ref(self, ref: Any, value: Any) -> None:
        with self._lock:
            self._check_active()
            ref_id = ref.id
            old_entry = self._write_log.get(ref_id)
            old_value = old_entry.old_value if old_entry else ref._read_raw()
            ref._validate(value)
            self._write_log[ref_id] = WriteLogEntry(
                ref_id=ref_id,
                old_value=old_value,
                new_value=value,
                old_version=ref._get_version()
            )

    def _prepare_inside_lock(self) -> Tuple[bool, Optional[FrozenSet[int]]]:
        read_set = {rid: entry.version_read for rid, entry in self._read_log.items()}
        write_set = set(self._write_log.keys())
        conflicts = self._coordinator.detect_conflicts(self, read_set, write_set)
        if conflicts:
            return False, conflicts
        return True, None

    def _commit(self) -> bool:
        with self._lock:
            if self._state == TransactionState.COMMITTED:
                return True
            if self._state != TransactionState.ACTIVE:
                raise STMException(f"Cannot commit in state {self._state.name}")
            self._state = TransactionState.PREPARING

        # Acquire the global commit_lock for the entire commit sequence
        with self._coordinator._commit_lock:
            success, conflicts = self._prepare_inside_lock()
            if not success and conflicts:
                self._state = TransactionState.ABORTED
                raise ConflictException(f"Transaction conflicted on refs {conflicts}", conflicting_refs=conflicts)
            
            self._state = TransactionState.COMMITTING

            # Applying commits inside the global commit_lock to ensure atomic stamp application
            commit_version = self._coordinator.create_version_stamp(self.id)
            notifications = []
            for ref_id, entry in self._write_log.items():
                ref = self._coordinator.get_ref(ref_id)
                if ref is not None:
                    notif_fn = ref._commit_value(entry.new_value, commit_version)
                    notifications.append(notif_fn)

        self._state = TransactionState.COMMITTED
        for notif in notifications:
            notif()
        return True

    def _abort(self) -> None:
        with self._lock:
            self._state = TransactionState.ABORTED

    def _reset_for_retry(self) -> None:
        with self._lock:
            self._state = TransactionState.ACTIVE
            self.snapshot_version = self._coordinator.create_version_stamp(self.id)
            self._read_log.clear()
            self._write_log.clear()
            self._start_time = time.time()

    def __enter__(self) -> 'Transaction':
        self._depth += 1
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self._depth -= 1
        if exc_type is not None:
            self._abort()
            return False
        if self._depth == 0:
            self._commit()
        return False

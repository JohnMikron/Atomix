import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Literal,
)
from .versioning import TransactionState, VersionStamp
from .exceptions import (
    STMException,
    ConflictException,
    TimeoutException,
    TransactionAbortedException,
    RetryException,
)

T = TypeVar("T")


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
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class Transaction:
    """STM transaction implementing Snapshot Isolation with validation and commit retry."""

    def __init__(
        self,
        coordinator: Any,
        timeout: float = 30.0,
        max_retries: int = 1000,
        label: Optional[str] = None,
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
        self._write_log: Dict[int, WriteLogEntry[Any]] = {}
        self._commutes: Dict[int, List[CommuteEntry[Any]]] = defaultdict(list)
        self._retry_count = 0
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
        if self._state != TransactionState.ACTIVE:
            if self._state == TransactionState.ABORTED:
                raise TransactionAbortedException(
                    f"Transaction {self.id} has been aborted"
                )
            raise STMException(f"Transaction {self.id} is in state {self._state.name}")
        if self.elapsed_time > self._timeout:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise TimeoutException(f"Transaction {self.id} timed out")

    def _read_ref(self, ref: Any) -> Any:
        with self._lock:
            self._check_active()
            ref_id = ref.id
            if ref_id in self._write_log:
                return self._write_log[ref_id].new_value
            if ref_id in self._commutes:
                value = ref._read_raw()
                for commute in self._commutes[ref_id]:
                    value = commute.function(value, *commute.args, **commute.kwargs)
                return value

            value, version = ref._read_at_version(self.snapshot_version)
            try:
                val_hash = hash(value)
            except TypeError:
                val_hash = id(value)
            self._read_log[ref_id] = ReadLogEntry(
                ref_id=ref_id, version_read=version, value_hash=val_hash
            )
            self._coordinator.history.record_access(ref_id)
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
                old_value=old_value
                if ref_id not in self._write_log
                else (old_entry.old_value if old_entry else None),
                new_value=value,
                old_version=ref._get_version(),
            )

    def _commute_ref(
        self, ref: Any, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        with self._lock:
            self._check_active()
            ref_id = ref.id
            if ref_id in self._write_log:
                entry = self._write_log[ref_id]
                new_value = fn(entry.new_value, *args, **kwargs)
                self._write_log[ref_id] = WriteLogEntry(
                    ref_id=ref_id,
                    old_value=entry.old_value,
                    new_value=new_value,
                    old_version=entry.old_version,
                    is_commutative=True,
                    commutative_fn=fn,
                )
                return new_value
            else:
                self._commutes[ref_id].append(
                    CommuteEntry(ref_id=ref_id, function=fn, args=args, kwargs=kwargs)
                )
                return ref._read_raw()

    def _prepare_inside_lock(self) -> Tuple[bool, Optional[FrozenSet[int]]]:
        """
        2PC prepare phase, to be called while the coordinator's commit lock is held.

        Detects write-write conflicts between this transaction's read set and the
        live versions of the refs it touched. Conflicting refs are reported back
        to the caller and recorded on the coordinator so the ``total_conflicts``
        diagnostic reflects the real contention rate rather than zero.
        """
        read_set = {rid: entry.version_read for rid, entry in self._read_log.items()}
        write_set = set(self._write_log.keys()) | set(self._commutes.keys())
        conflicts = self._coordinator.detect_conflicts(self, read_set, write_set)
        if conflicts:
            self._coordinator.record_conflict(conflicts)
            return False, conflicts
        return True, None

    def _commit(self) -> bool:
        with self._lock:
            if self._state == TransactionState.COMMITTED:
                return True
            if self._state != TransactionState.ACTIVE:
                raise STMException(f"Cannot commit in state {self._state.name}")
            if self.elapsed_time > self._timeout:
                self._state = TransactionState.ABORTED
                self._coordinator.record_abort()
                raise TimeoutException("Transaction timeout during commit")
            self._state = TransactionState.PREPARING

        try:
            notifications = []
            with self._coordinator._commit_lock:
                success, conflicts = self._prepare_inside_lock()
                if not success and conflicts:
                    self._coordinator.contention.record_conflict(
                        conflicts, self._retry_count
                    )
                    raise ConflictException(
                        f"Transaction conflicted on refs {conflicts}",
                        conflicting_refs=conflicts,
                    )

                self._state = TransactionState.COMMITTING
                self._apply_commutes()
                commit_version = self._coordinator.create_version_stamp(self.id)
                for ref_id, entry in self._write_log.items():
                    ref = self._coordinator.get_ref(ref_id)
                    if ref is not None:
                        notif_fn = ref._commit_value(entry.new_value, commit_version)
                        notifications.append(notif_fn)

            self._state = TransactionState.COMMITTED
            for notif in notifications:
                notif()
            all_refs = set(self._write_log.keys()) | set(self._read_log.keys())
            self._coordinator.contention.record_success(all_refs)
            self._coordinator.record_commit()
            return True
        except Exception:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise

    def _apply_commutes(self) -> None:
        for ref_id, commutes in self._commutes.items():
            if ref_id in self._write_log:
                continue
            ref = self._coordinator.get_ref(ref_id)
            if ref is None:
                continue
            old_value = ref._read_raw()
            value = old_value
            for commute in commutes:
                value = commute.function(value, *commute.args, **commute.kwargs)
            ref._validate(value)
            self._write_log[ref_id] = WriteLogEntry(
                ref_id=ref_id,
                old_value=old_value,
                new_value=value,
                old_version=ref._get_version(),
                is_commutative=True,
            )

    def _abort(self, reason: Optional[str] = None) -> None:
        with self._lock:
            if self._state in (TransactionState.COMMITTED, TransactionState.ABORTED):
                return
            self._state = TransactionState.ABORTED
            self._read_log.clear()
            self._write_log.clear()
            self._commutes.clear()
            self._coordinator.record_abort()
            if reason:
                raise TransactionAbortedException(
                    f"Transaction {self.id} aborted: {reason}"
                )

    def _retry(self) -> None:
        self._retry_count += 1
        if self._retry_count > self._max_retries:
            self._state = TransactionState.ABORTED
            self._coordinator.record_abort()
            raise TimeoutException(f"Transaction {self.id} exceeded max retries")
        raise RetryException(
            f"Transaction {self.id} retrying", retry_count=self._retry_count
        )

    def _reset_for_retry(self) -> None:
        with self._lock:
            self._state = TransactionState.ACTIVE
            self.snapshot_version = self._coordinator.create_version_stamp(self.id)
            self._read_log.clear()
            self._write_log.clear()
            self._commutes.clear()
            self._start_time = time.time()

    def __enter__(self) -> "Transaction":
        self._depth += 1
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self._depth -= 1
        if exc_type is not None:
            if not issubclass(exc_type, (RetryException, ConflictException)):
                self._abort()
            return False
        if self._depth == 0:
            try:
                self._commit()
            except Exception:
                raise
        return False

    def __repr__(self) -> str:
        return f"Transaction(id={self.id}, state={self._state.name}, retries={self._retry_count})"

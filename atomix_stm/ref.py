import threading
import time
from typing import Callable, Generic, Optional, TypeVar, Tuple, List, Any
from .versioning import VersionStamp, _get_current_transaction
from .exceptions import ValidationException, InvariantViolationException, HistoryExpiredException
from .coordinator import TransactionCoordinator

T = TypeVar('T')

class RefIdentity:
    """Unique identity for a transactional Ref."""
    __slots__ = ('id', 'created_at', 'name')
    def __init__(self, id: int, created_at: float, name: Optional[str] = None) -> None:
        self.id = id
        self.created_at = created_at
        self.name = name

    def __hash__(self) -> int:
        return hash(self.id)


class Ref(Generic[T]):
    """Transactional reference containing shared state."""
    __slots__ = (
        '_identity', '_value', '_version', '_history',
        '_lock', '_coordinator', '_validators', '_watchers',
        '_max_history', '_min_history', '_watcher_lock',
        '_invariant', '_access_time'
    )
    
    def __init__(
        self,
        value: T,
        min_history: int = 0,
        max_history: int = 100,
        validator: Optional[Callable[[T], bool]] = None,
        name: Optional[str] = None
    ) -> None:
        self._coordinator = TransactionCoordinator()
        ref_id = self._coordinator.register_ref(self)
        self._identity = RefIdentity(id=ref_id, created_at=time.time(), name=name)
        self._value = value
        self._version = self._coordinator.create_version_stamp(0)
        self._history: List[Tuple[VersionStamp, T]] = []
        self._min_history = min_history
        self._max_history = max_history
        self._lock = threading.RLock()
        self._validators: List[Callable[[T], bool]] = []
        if validator:
            self._validators.append(validator)
        self._watchers: dict[str, Callable[[T, T], None]] = {}
        self._watcher_lock = threading.Lock()
        self._invariant: Optional[Callable[[T], bool]] = None
        self._access_time = time.time()

    @property
    def id(self) -> int:
        return self._identity.id

    @property
    def name(self) -> Optional[str]:
        return self._identity.name

    def _get_version(self) -> VersionStamp:
        with self._lock:
            return self._version

    def _read_raw(self) -> T:
        with self._lock:
            return self._value

    def _read_at_version(self, version: VersionStamp) -> Tuple[T, VersionStamp]:
        with self._lock:
            if self._version.logical_time <= version.logical_time:
                return self._value, self._version
            for hist_version, hist_value in reversed(self._history):
                if hist_version.logical_time <= version.logical_time:
                    return hist_value, hist_version
            raise HistoryExpiredException(
                f"Version {version.logical_time} expired for ref {self._identity.id}",
                self._identity.id
            )

    def _commit_value(self, value: T, version: VersionStamp) -> Callable[[], None]:
        with self._lock:
            old_value = self._value
            if self._min_history > 0 or len(self._history) > 0:
                self._history.append((self._version, self._value))
            self._value = value
            self._version = version
            self._access_time = time.time()
            self._trim_history_unlocked(0)
        return lambda: self._notify_watchers(old_value, value)

    def _trim_history_unlocked(self, retention_logical_time: int) -> None:
        if len(self._history) <= 1:
            return
        base_idx = 0
        for i in range(len(self._history) - 1, -1, -1):
            if self._history[i][0].logical_time <= retention_logical_time:
                base_idx = i
                break
        start_idx = max(base_idx, len(self._history) - self._max_history)
        self._history = self._history[start_idx:]

    def _validate(self, value: T) -> None:
        for validator in self._validators:
            if not validator(value):
                raise ValidationException(f"Validation failed for Ref {self._identity.id}")
        if self._invariant and not self._invariant(value):
            raise InvariantViolationException(f"Invariant violated for Ref {self._identity.id}")

    def _notify_watchers(self, old_value: T, new_value: T) -> None:
        with self._watcher_lock:
            watchers = list(self._watchers.values())
        for watcher in watchers:
            try:
                watcher(old_value, new_value)
            except Exception:
                pass

    def deref(self) -> T:
        tx = _get_current_transaction()
        if tx is None:
            return self._read_raw()
        return tx._read_ref(self)

    @property
    def value(self) -> T:
        return self.deref()

    @value.setter
    def value(self, new_value: T) -> None:
        self.set(new_value)

    def set(self, value: T) -> None:
        tx = _get_current_transaction()
        if tx is None:
            self.reset(value)
            return
        tx._write_ref(self, value)

    def reset(self, value: T) -> T:
        self._validate(value)
        version = self._coordinator.create_version_stamp(0)
        notif_fn = self._commit_value(value, version)
        if notif_fn:
            notif_fn()
        return value

    def alter(self, fn: Callable[[T], T], *args: Any, **kwargs: Any) -> T:
        tx = _get_current_transaction()
        if tx is None:
            result = [None]
            from .api import atomically
            @atomically
            def _do_alter():
                inner_tx = _get_current_transaction()
                current = inner_tx._read_ref(self)
                new_val = fn(current, *args, **kwargs)
                inner_tx._write_ref(self, new_val)
                result[0] = new_val
            _do_alter()
            return result[0]
        
        current = tx._read_ref(self)
        new_value = fn(current, *args, **kwargs)
        tx._write_ref(self, new_value)
        return new_value

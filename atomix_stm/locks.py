import threading
import time
from typing import Any, Callable, Generic, Optional, TypeVar
from .exceptions import TimeoutException

T = TypeVar('T')


class SpinLock:
    """Lightweight reentrant spin lock wrapper around threading.RLock."""
    __slots__ = ('_lock', '_owner', '_spin_count', '_recursion_count')
    
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._owner: Optional[int] = None
        self._spin_count = 0
        self._recursion_count = 0
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            timeout = -1.0
        acquired = self._lock.acquire(timeout=timeout)
        if acquired:
            ident = threading.current_thread().ident
            if self._owner != ident:
                self._owner = ident
                self._recursion_count = 1
                self._spin_count = 1
            else:
                self._recursion_count += 1
            return True
        return False
    
    def release(self) -> None:
        if self._owner != threading.current_thread().ident:
            raise RuntimeError("Lock not owned by current thread")
        self._recursion_count -= 1
        if self._recursion_count == 0:
            self._owner = None
        self._lock.release()
    
    def __enter__(self) -> 'SpinLock':
        self.acquire()
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.release()
    
    @property
    def spin_count(self) -> int:
        return self._spin_count


class RWLock:
    """Reader-Writer lock allowing multiple readers or a single writer."""
    def __init__(self) -> None:
        self._readers = 0
        self._writers = 0
        self._pending_writers = 0
        self._lock = threading.RLock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        with self._read_ready:
            while self._writers > 0 or self._pending_writers > 0:
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
            self._pending_writers += 1
            try:
                while self._writers > 0 or self._readers > 0:
                    if not self._write_ready.wait(timeout):
                        return False
                self._writers += 1
                return True
            finally:
                self._pending_writers -= 1
    
    def release_write(self) -> None:
        with self._write_ready:
            self._writers -= 1
            self._write_ready.notify_all()
            self._read_ready.notify_all()


class SeqLock(Generic[T]):
    """Sequence lock for lock-free optimistic reads."""
    __slots__ = ('_sequence', '_value', '_write_lock')
    
    def __init__(self, initial_value: T) -> None:
        self._sequence = 0
        self._value = initial_value
        self._write_lock = threading.Lock()
    
    def read(self) -> T:
        spins: int = 0
        max_spins: int = 100000
        while True:
            seq1 = self._sequence
            if seq1 & 1:
                spins += 1
                if spins > max_spins:
                    raise TimeoutException(
                        f"SeqLock.read() exceeded {max_spins} spins — "
                        f"possible writer starvation or deadlock"
                    )
                time.sleep(min(0.001, 1e-6 * (2 ** min(spins, 10))))
                continue

            value = self._value
            seq2 = self._sequence
            if seq1 == seq2:
                return value

            spins += 1
            if spins > max_spins:
                raise TimeoutException(
                    f"SeqLock.read() exceeded {max_spins} spins — "
                    f"extreme write contention"
                )

    def write(self, new_value: T) -> None:
        with self._write_lock:
            self._sequence += 1
            self._value = new_value
            self._sequence += 1

    def get_version(self) -> int:
        return self._sequence

    def read_seq(self) -> int:
        return self._sequence

    def read_value(self) -> T:
        return self._value

    def cas(self, expected_seq: int, expected_value: T, new_value: T) -> bool:
        with self._write_lock:
            if self._sequence == expected_seq and self._value == expected_value:
                self._sequence += 1
                self._value = new_value
                self._sequence += 1
                return True
            return False

    def cas_value(self, expected_value: T, new_value: T) -> bool:
        with self._write_lock:
            if self._value is expected_value or self._value == expected_value:
                self._sequence += 1
                self._value = new_value
                self._sequence += 1
                return True
            return False

import threading
from typing import Optional


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
    
    def __exit__(self, *args: any) -> None:
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

import logging
import threading
import time
from typing import Any, Callable, ClassVar, Generic, List, Optional, TypeVar, cast

from .api import atomically
from .exceptions import QueueClosedException, TimeoutException
from .ref import Atom, Ref

T = TypeVar("T")
logger = logging.getLogger(__name__)

_QUEUE_CLOSED = object()
_QUEUE_RETRY = object()


class STMQueue(Generic[T]):
    """Transactional queue with blocking semantics."""

    def __init__(self, maxsize: int = 0, name: Optional[str] = None) -> None:
        self._maxsize = maxsize
        self._items: Ref[List[T]] = Ref([], name=name or "queue_items")
        self._closed = Atom(False)
        self._cond = threading.Condition()

    def put(self, item: T, timeout: Optional[float] = None) -> bool:
        if self._closed.deref():
            return False

        deadline = time.time() + timeout if timeout is not None else None

        while True:
            if self._maxsize > 0:
                with self._cond:
                    while len(self._items.deref()) >= self._maxsize:
                        if self._closed.deref():
                            return False
                        if deadline is not None:
                            rem = deadline - time.time()
                            if rem <= 0:
                                return False
                            self._cond.wait(timeout=rem)
                        else:
                            self._cond.wait()

            @atomically
            def _do_put() -> Any:
                if self._closed.deref():
                    return False
                items = self._items.deref()
                if self._maxsize > 0 and len(items) >= self._maxsize:
                    return _QUEUE_RETRY
                self._items.set(items + [item])
                return True

            result = _do_put()
            if result is True:
                with self._cond:
                    self._cond.notify_all()
                return True
            elif result is False:
                return False

    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        deadline = time.time() + timeout if timeout is not None else None

        while True:

            @atomically
            def _do_get() -> Any:
                items = self._items.deref()
                if not items:
                    if self._closed.deref():
                        return _QUEUE_CLOSED
                    if deadline is not None and time.time() >= deadline:
                        return _QUEUE_RETRY
                    return _QUEUE_RETRY

                item = items[0]
                self._items.set(items[1:])
                return item

            result = _do_get()
            if result is not _QUEUE_RETRY:
                with self._cond:
                    self._cond.notify_all()
                if result is _QUEUE_CLOSED:
                    raise QueueClosedException("Queue is closed and empty")
                return cast(Optional[T], result)

            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutException(f"STMQueue.get() timed out after {timeout}s")
                wait_time: Optional[float] = remaining
            else:
                wait_time = None

            with self._cond:
                self._cond.wait(timeout=wait_time)

    def peek(self) -> Optional[T]:
        result: List[Optional[T]] = [None]

        @atomically
        def _do() -> None:
            items = self._items.deref()
            result[0] = items[0] if items else None

        _do()
        return result[0]

    def size(self) -> int:
        result = [0]

        @atomically
        def _do() -> None:
            result[0] = len(self._items.deref())

        _do()
        return result[0]

    def empty(self) -> bool:
        return self.size() == 0

    def full(self) -> bool:
        if self._maxsize == 0:
            return False
        return self.size() >= self._maxsize

    def close(self) -> None:
        self._closed.reset(True)
        with self._cond:
            self._cond.notify_all()

    def is_closed(self) -> bool:
        return self._closed.deref()


class STMAgent(Generic[T]):
    """Asynchronous agent for managing shared state via thread pool."""

    __slots__ = (
        "_ref",
        "_errors",
        "_lock",
        "_pending_count_lock",
        "_pending",
        "_pending_count",
    )
    _agent_pool: ClassVar[Optional[Any]] = None
    _agent_pool_lock = threading.Lock()

    def __init__(self, initial_value: T, name: Optional[str] = None) -> None:
        self._ref = Ref(initial_value, name=name)
        self._errors: List[Exception] = []
        self._lock = threading.Lock()
        self._pending_count_lock = threading.Lock()
        self._pending = threading.Condition(self._pending_count_lock)
        self._pending_count = 0

    def send(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        with self._pending_count_lock:
            self._pending_count += 1

        def task() -> None:
            @atomically
            def do_update() -> None:
                self._ref.alter(fn, *args, **kwargs)

            try:
                do_update()
            except Exception as e:
                with self._lock:
                    self._errors.append(e)
                logger.error(f"Agent {self._ref.name} error: {e}")
            finally:
                with self._pending_count_lock:
                    self._pending_count -= 1
                    if self._pending_count == 0:
                        self._pending.notify_all()

        if STMAgent._agent_pool is None:
            with STMAgent._agent_pool_lock:
                if STMAgent._agent_pool is None:
                    import concurrent.futures
                    import os

                    STMAgent._agent_pool = concurrent.futures.ThreadPoolExecutor(
                        max_workers=os.cpu_count() or 4, thread_name_prefix="STMAgent"
                    )
        cast(Any, STMAgent._agent_pool).submit(task)

    def deref(self) -> T:
        return self._ref.deref()

    @property
    def value(self) -> T:
        return self.deref()

    @property
    def errors(self) -> List[Exception]:
        with self._lock:
            return list(self._errors)

    def clear_errors(self) -> List[Exception]:
        with self._lock:
            errs = list(self._errors)
            self._errors.clear()
            return errs

    def await_value(self, timeout: float = 10.0) -> T:
        deadline = time.time() + timeout
        with self._pending_count_lock:
            while self._pending_count > 0:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self._pending.wait(timeout=remaining)
        return self.deref()


class STMVar(Generic[T]):
    """Dynamic variable with thread-local bindings."""

    def __init__(self, initial_value: T, name: Optional[str] = None) -> None:
        self._root_value = initial_value
        self._local = threading.local()
        self.name = name

    def deref(self) -> T:
        return getattr(self._local, "value", self._root_value)

    @property
    def value(self) -> T:
        return self.deref()

    def binding(self, value: T) -> Any:
        class BindingContext:
            def __init__(self, local: Any, val: Any, root: Any) -> None:
                self.local = local
                self.val = val
                self.root = root
                self.has_old = hasattr(local, "value")
                self.old = getattr(local, "value", None)

            def __enter__(self) -> None:
                self.local.value = self.val

            def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
                if self.has_old:
                    self.local.value = self.old
                else:
                    delattr(self.local, "value")

        return BindingContext(self._local, value, self._root_value)


class STMPromise(Generic[T]):
    """
    Transactional promise providing write-once atomic resolution.

    Can be safely delivered from inside or outside an STM transaction.
    Consumers can wait with timeout or poll realization state.
    """

    __slots__ = ("_value", "_realized", "_lock", "_cond", "_name")

    def __init__(self, name: Optional[str] = None) -> None:
        self._value: Optional[T] = None
        self._realized = False
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._name = name

    def deliver(self, value: T) -> bool:
        """
        Deliver a value to the promise. Returns True if this call realized
        the promise, or False if the promise was already delivered.
        """
        with self._lock:
            if self._realized:
                return False
            self._value = value
            self._realized = True
            self._cond.notify_all()
            return True

    def deref(self, timeout: Optional[float] = None) -> T:
        """
        Retrieve the delivered value. Blocks until delivery or timeout.
        Raises TimeoutException if timeout expires.
        """
        with self._lock:
            if not self._realized:
                if not self._cond.wait(timeout=timeout):
                    raise TimeoutException(
                        f"STMPromise {self._name or ''} deref timed out after {timeout}s"
                    )
            return cast(T, self._value)

    def is_realized(self) -> bool:
        """Return whether this promise has been fulfilled."""
        with self._lock:
            return self._realized

    @property
    def value(self) -> Optional[T]:
        """Non-blocking access to the realized value (or None if unfulfilled)."""
        with self._lock:
            return self._value


class STMChannel(Generic[T]):
    """
    Multi-producer, multi-consumer transactional communication channel.

    Enables message passing with atomic transactions, buffering, and backpressure.
    """

    __slots__ = ("_queue", "_name")

    def __init__(self, maxsize: int = 0, name: Optional[str] = None) -> None:
        self._queue: STMQueue[T] = STMQueue(maxsize=maxsize, name=name)
        self._name = name

    def send(self, item: T, timeout: Optional[float] = None) -> bool:
        """Send an item to the channel. Returns False if channel is closed."""
        return self._queue.put(item, timeout=timeout)

    def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive an item from the channel. Raises QueueClosedException if closed."""
        return self._queue.get(timeout=timeout)

    def close(self) -> None:
        """Close the channel."""
        self._queue.close()

    def is_closed(self) -> bool:
        """Check if the channel has been closed."""
        return self._queue.is_closed()

    def size(self) -> int:
        """Get current number of queued items."""
        return self._queue.size()

    def empty(self) -> bool:
        """Check if channel has no items."""
        return self._queue.empty()

    def full(self) -> bool:
        """Check if channel buffer is full."""
        return self._queue.full()

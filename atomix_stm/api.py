import functools
import threading
import time
from typing import Any, Callable, Iterator, Optional, TypeVar, Union
from contextlib import contextmanager
from .versioning import VersionStamp
from .exceptions import STMException, RetryException, ConflictException, TransactionAbortedException
from .coordinator import TransactionCoordinator
from .transaction import Transaction
from .ref import Ref

R = TypeVar('R')
T = TypeVar('T')

_tx_context = threading.local()


def _get_current_transaction() -> Optional[Transaction]:
    return getattr(_tx_context, 'tx', None)


def _set_current_transaction(tx: Optional[Transaction]) -> None:
    _tx_context.tx = tx


@contextmanager
def transaction(
    timeout: float = 10.0,
    max_retries: int = 100,
    label: Optional[str] = None
) -> Iterator[Transaction]:
    coordinator = TransactionCoordinator()
    old_tx = _get_current_transaction()
    if old_tx:
        yield old_tx
        return

    tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries, label=label)
    _set_current_transaction(tx)
    try:
        with tx:
            yield tx
    finally:
        _set_current_transaction(old_tx)


def dosync(
    fn: Optional[Callable[..., R]] = None,
    timeout: float = 10.0,
    max_retries: int = 500
) -> Union[R, Callable]:
    def wrapper(func):
        @functools.wraps(func)
        def inner(*args: Any, **kwargs: Any) -> R:
            coordinator = TransactionCoordinator()
            retry_count = 0
            old_tx = _get_current_transaction()
            if old_tx:
                old_tx._check_active()
                return func(*args, **kwargs)

            tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries)
            _set_current_transaction(tx)
            try:
                while True:
                    try:
                        with tx:
                            result = func(*args, **kwargs)
                        return result
                    except (RetryException, ConflictException):
                        retry_count += 1
                        if retry_count > max_retries:
                            raise CommitException("Max retries exceeded")
                        time.sleep(0.001 * retry_count) # Minimal backoff
                        tx._reset_for_retry()
            finally:
                _set_current_transaction(old_tx)
        return inner

    if fn is not None:
        return wrapper(fn)()
    return wrapper


def atomically(fn: Callable[..., R]) -> Callable[..., R]:
    return dosync(fn=None)(fn)


def ref(value: T) -> Ref[T]:
    return Ref(value)


def alter(reference: Ref[T], fn: Callable[[T], T], *args: Any, **kwargs: Any) -> T:
    return reference.alter(fn, *args, **kwargs)


def write(reference: Ref[T], value: T) -> None:
    reference.set(value)


def read(reference: Ref[T]) -> T:
    return reference.deref()

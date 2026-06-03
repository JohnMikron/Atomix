import functools
import json
import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Optional,
    TypeVar,
    Generic,
    List,
    Tuple,
    cast,
)
from contextlib import contextmanager
from pathlib import Path

from .versioning import VersionStamp, _get_current_transaction, _set_current_transaction
from .exceptions import STMException, RetryException, ConflictException, CommitException
from .coordinator import TransactionCoordinator, logger, PY_VERSION, NO_GIL_ENABLED
from .transaction import Transaction
from .ref import Ref, Atom

R = TypeVar("R")
T = TypeVar("T")


@contextmanager
def transaction(
    timeout: float = 10.0, max_retries: int = 100, label: Optional[str] = None
) -> Iterator[Transaction]:
    coordinator = TransactionCoordinator()
    old_tx = _get_current_transaction()
    if old_tx:
        yield old_tx
        return

    tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries, label=label)
    _set_current_transaction(tx)
    coordinator.register_transaction(tx)
    try:
        with tx:
            yield tx
    finally:
        coordinator.unregister_transaction(tx.id)
        _set_current_transaction(old_tx)


def dosync(
    fn: Optional[Callable[..., R]] = None, timeout: float = 10.0, max_retries: int = 500
) -> Any:
    def wrapper(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        def inner(*args: Any, **kwargs: Any) -> R:
            coordinator = TransactionCoordinator()
            retry_count = 0
            old_tx = _get_current_transaction()
            if old_tx:
                old_tx._check_active()
                return func(*args, **kwargs)

            tx = Transaction(coordinator, timeout=timeout, max_retries=max_retries)
            coordinator.register_transaction(tx)
            _set_current_transaction(tx)
            try:
                while True:
                    try:
                        with tx:
                            result = func(*args, **kwargs)
                        return result
                    except (RetryException, ConflictException) as e:
                        retry_count += 1
                        if retry_count > max_retries:
                            raise CommitException("Max retries exceeded")
                        refs = set(tx._read_log.keys()) | set(tx._write_log.keys())
                        if isinstance(e, ConflictException):
                            refs |= e.conflicting_refs
                        backoff = coordinator.contention.get_backoff_for_retry(
                            retry_count, refs
                        )
                        coordinator.unregister_transaction(tx.id)
                        time.sleep(backoff)
                        tx._reset_for_retry()
                        coordinator.register_transaction(tx)
            finally:
                coordinator.unregister_transaction(tx.id)
                _set_current_transaction(old_tx)

        return inner

    if fn is not None:
        return wrapper(fn)()
    return wrapper


def atomically(fn: Callable[..., R]) -> Callable[..., R]:
    return cast(Callable[..., R], dosync(fn=None)(fn))


def transactional(timeout: float = 10.0, max_retries: int = 500) -> Callable[..., Any]:
    return cast(
        Callable[..., Any], dosync(fn=None, timeout=timeout, max_retries=max_retries)
    )


def ref(value: T) -> Ref[T]:
    return Ref(value)


def atom(value: T) -> Atom[T]:
    return Atom(value)


def alter(reference: Ref[T], fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    return reference.alter(fn, *args, **kwargs)


def write(reference: Ref[T], value: T) -> None:
    reference.set(value)


def read(reference: Ref[T]) -> T:
    return reference.deref()


def commute(reference: Ref[T], fn: Callable[[T], T], *args: Any, **kwargs: Any) -> T:
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("commute() requires active transaction")
    return cast(T, tx._commute_ref(reference, fn, *args, **kwargs))


def ensure(reference: Ref[T]) -> T:
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("ensure() requires active transaction")
    return cast(T, tx._read_ref(reference))


def retry() -> None:
    tx = _get_current_transaction()
    if tx is None:
        raise STMException("retry() requires active transaction")
    tx._retry()


def io(func: Callable[..., R]) -> Callable[..., R]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        tx = _get_current_transaction()
        if tx is not None:
            _set_current_transaction(None)
            try:
                return func(*args, **kwargs)
            finally:
                _set_current_transaction(tx)
        return func(*args, **kwargs)

    return wrapper


def get_stm_stats() -> Dict[str, Any]:
    coordinator = TransactionCoordinator()
    return {
        "timestamp": time.time(),
        "python_version": f"{PY_VERSION.major}.{PY_VERSION.minor}.{PY_VERSION.micro}",
        "no_gil_enabled": NO_GIL_ENABLED,
        **coordinator.get_stats(),
        "contention": {
            "level": coordinator.contention.get_contention_level(),
            "throughput": coordinator.contention.get_throughput(),
        },
    }


def dump_stm_stats(filepath: Optional[str] = None) -> str:
    stats = get_stm_stats()
    json_str = json.dumps(stats, indent=2)
    if filepath:
        Path(filepath).write_text(json_str)
        logger.info(f"Stats dumped to {filepath}")
    return json_str


def reset_stm() -> None:
    TransactionCoordinator().reset()
    logger.info("STM state reset")


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
                timestamp=version.physical_time,
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
                    timestamp=version.physical_time,
                )
        return None


def run_concurrent(
    funcs: List[Callable[[], R]], max_workers: Optional[int] = None
) -> List[R]:
    """Run functions concurrently."""
    import concurrent.futures

    max_workers = max_workers or len(funcs)
    results = [None] * len(funcs)

    def run_one(idx: int, func: Callable[[], R]) -> Tuple[int, R, Optional[Exception]]:
        try:
            return idx, func(), None
        except Exception as e:
            return idx, None, e  # type: ignore

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_one, i, f) for i, f in enumerate(funcs)]

        for future in concurrent.futures.as_completed(futures):
            idx, result, exc = future.result()
            results[idx] = result  # type: ignore
            if exc:
                logger.error(f"Task {idx} failed: {exc}")

    return results  # type: ignore

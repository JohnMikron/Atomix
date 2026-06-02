class STMException(Exception):
    """Base exception for all STM operations."""
    pass


class RetryException(STMException):
    """Raised when a transaction conflicts and needs to retry."""
    def __init__(self, message: str, retry_count: int = 0, backoff: float = 0.0):
        super().__init__(message)
        self.retry_count = retry_count
        self.backoff = backoff


class CommitException(STMException):
    """Raised when a transaction commit fails."""
    pass


class ConflictException(CommitException):
    """Raised when a write conflict is detected during the prepare phase."""
    def __init__(self, message: str, conflicting_refs: frozenset[int] = frozenset()):
        super().__init__(message)
        self.conflicting_refs = conflicting_refs


class TransactionAbortedException(STMException):
    """Raised when an operation is attempted on an aborted transaction."""
    pass


class TimeoutException(STMException):
    """Raised when a transaction or lock acquisition times out."""
    pass


class ValidationException(STMException):
    """Raised when a Ref validator function returns False."""
    pass


class HistoryExpiredException(ConflictException):
    """Raised when the MVCC history of a Ref has been trimmed past the transaction's read version."""
    def __init__(self, message: str, ref_id: int):
        super().__init__(message, frozenset([ref_id]))


class InvariantViolationException(STMException):
    """Raised when a transaction invariant check fails."""
    pass


class QueueClosedException(STMException):
    """Raised when an operation is performed on a closed STMQueue."""
    pass

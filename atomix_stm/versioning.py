import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


class TransactionState(Enum):
    """Lifecycle states of an STM transaction."""
    ACTIVE = auto()
    PREPARING = auto()
    COMMITTING = auto()
    COMMITTED = auto()
    ABORTED = auto()
    RETRYING = auto()


@dataclass(slots=True)
class VersionStamp:
    """Immutable version stamp representing a specific logical point in time."""
    epoch: int = 0
    logical_time: int = 0
    transaction_id: int = 0
    physical_time: float = field(default_factory=time.time)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VersionStamp):
            return False
        return (self.epoch == other.epoch and 
                self.logical_time == other.logical_time and
                self.transaction_id == other.transaction_id)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, VersionStamp):
            return NotImplemented
        if self.epoch != other.epoch:
            return self.epoch < other.epoch
        if self.logical_time != other.logical_time:
            return self.logical_time < other.logical_time
        return self.transaction_id < other.transaction_id

    def __hash__(self) -> int:
        return hash((self.epoch, self.logical_time, self.transaction_id))

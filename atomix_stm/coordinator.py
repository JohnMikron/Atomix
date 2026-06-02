import itertools
import threading
from typing import Any, Dict, Optional
from .versioning import VersionStamp


class TransactionCoordinator:
    """Coordinates transaction IDs, logical clocks, and Ref registries."""
    _instance: Optional['TransactionCoordinator'] = None
    _init_lock = threading.Lock()
    
    def __new__(cls) -> 'TransactionCoordinator':
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
            return cls._instance
    
    def _initialize(self) -> None:
        self._tx_counter = itertools.count(1)
        self._ref_counter = itertools.count(1)
        self._logical_clock = 0
        self._clock_lock = threading.RLock()
        self._refs: Dict[int, Any] = {}
        self._refs_lock = threading.RLock()
        self._commit_lock = threading.RLock()
        
    def reset(self) -> None:
        with self._init_lock:
            self._logical_clock = 0
            self._refs.clear()
            
    def new_transaction_id(self) -> int:
        return next(self._tx_counter)
        
    def new_ref_id(self) -> int:
        return next(self._ref_counter)
        
    def get_logical_time(self) -> int:
        with self._clock_lock:
            return self._logical_clock
            
    def advance_clock(self) -> int:
        with self._clock_lock:
            self._logical_clock += 1
            return self._logical_clock
            
    def create_version_stamp(self, tx_id: int) -> VersionStamp:
        return VersionStamp(
            epoch=0,
            logical_time=self.advance_clock(),
            transaction_id=tx_id
        )
        
    def register_ref(self, ref: Any) -> int:
        ref_id = self.new_ref_id()
        with self._refs_lock:
            self._refs[ref_id] = ref
        return ref_id
        
    def unregister_ref(self, ref_id: int) -> None:
        with self._refs_lock:
            self._refs.pop(ref_id, None)
            
    def get_ref(self, ref_id: int) -> Optional[Any]:
        with self._refs_lock:
            return self._refs.get(ref_id)

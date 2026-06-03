import json
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K")
V = TypeVar("V")


class PersistentVector(Generic[T]):
    """Immutable vector wrapper around tuple."""

    __slots__ = ("_items", "_shift", "_json_encoder", "_hash")

    @classmethod
    def empty(cls) -> "PersistentVector[T]":
        return cls(())

    def __init__(
        self,
        items: Tuple[T, ...] = (),
        shift: int = 5,
        json_encoder: Optional[Callable[[T], Any]] = None,
        _hash: Optional[int] = None,
    ) -> None:
        self._items = items
        self._shift = shift
        self._json_encoder = json_encoder or (lambda x: x)
        self._hash = _hash

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __getitem__(self, index: int) -> T:
        if index < 0:
            index += len(self._items)
        if index < 0 or index >= len(self._items):
            raise IndexError(f"Index {index} out of range")
        return self._items[index]

    def first(self) -> Optional[T]:
        return self._items[0] if self._items else None

    def last(self) -> Optional[T]:
        return self._items[-1] if self._items else None

    def conj(self, value: T) -> "PersistentVector[T]":
        return PersistentVector(self._items + (value,), self._shift, self._json_encoder)

    def assoc(self, index: int, value: T) -> "PersistentVector[T]":
        if index < 0:
            index += len(self._items)
        if index < 0 or index >= len(self._items):
            raise IndexError(f"Index {index} out of range")
        new_items = list(self._items)
        new_items[index] = value
        return PersistentVector(tuple(new_items), self._shift, self._json_encoder)

    def pop(self) -> "PersistentVector[T]":
        if not self._items:
            raise IndexError("Cannot pop empty vector")
        return PersistentVector(self._items[:-1], self._shift, self._json_encoder)

    def rest(self) -> "PersistentVector[T]":
        if len(self._items) <= 1:
            return PersistentVector.empty()
        return PersistentVector(self._items[1:], self._shift, self._json_encoder)

    def _slice(self, start: int, end: int) -> "PersistentVector[T]":
        return PersistentVector(self._items[start:end], self._shift, self._json_encoder)

    def map(self, fn: Callable[[T], R]) -> "PersistentVector[R]":
        return PersistentVector.from_seq(fn(x) for x in self._items)

    def filter(self, pred: Callable[[T], bool]) -> "PersistentVector[T]":
        return PersistentVector.from_seq(x for x in self._items if pred(x))

    def reduce(self, fn: Callable[[R, T], R], initial: R) -> R:
        result = initial
        for x in self._items:
            result = fn(result, x)
        return result

    def to_list(self) -> List[T]:
        return list(self._items)

    def to_json(self) -> str:
        return json.dumps([self._json_encoder(x) for x in self._items], default=str)

    @classmethod
    def from_json(
        cls: Type["PersistentVector[T]"],
        json_str: str,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> "PersistentVector[T]":
        items = json.loads(json_str)
        if decoder:
            items = [decoder(x) for x in items]
        return cls(tuple(items))

    @classmethod
    def from_seq(cls, seq: Iterable[T]) -> "PersistentVector[T]":
        return cls(tuple(seq))

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __contains__(self, item: T) -> bool:
        return item in self._items

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PersistentVector):
            return self._items == other._items
        if isinstance(other, (list, tuple)):
            return self._items == tuple(other)
        return False

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(self._items)
        return self._hash

    def __repr__(self) -> str:
        return f"PersistentVector({list(self._items)})"

    def __add__(self, other: "PersistentVector[T]") -> "PersistentVector[T]":
        if not isinstance(other, PersistentVector):
            raise TypeError(f"Can only concatenate PersistentVector, not {type(other)}")
        return PersistentVector(
            self._items + other._items, self._shift, self._json_encoder
        )


class PersistentHashMap(Generic[K, V]):
    """Immutable hashmap with structural sharing using HAMT (Hash Array Mapped Trie)."""

    __slots__ = ("_root", "_size", "_hash", "_json_encoder")

    BITS = 5
    MASK = (1 << BITS) - 1
    BUCKET_SIZE = 8

    def __init__(
        self,
        root: Optional[Dict[int, Any]] = None,
        size: int = 0,
        _hash: Optional[int] = None,
        json_encoder: Optional[Callable[[V], Any]] = None,
    ) -> None:
        self._root = root if root is not None else {}
        self._size = size
        self._hash = _hash
        self._json_encoder = json_encoder or (lambda x: x)

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def __getitem__(self, key: K) -> V:
        _sentinel = object()
        result = self.get(key, default=_sentinel)
        if result is _sentinel:
            raise KeyError(key)
        return cast(V, result)

    def get(self, key: K, default: Any = None) -> Any:
        h = hash(key)
        return self._get(self._root, h, key, default, 0)

    def _get(
        self, node: Dict[int, Any], h: int, key: K, default: Any, shift: int = 0
    ) -> Any:
        if not node:
            return default
        idx = (h >> shift) & self.MASK
        if idx not in node:
            return default
        entry = node[idx]
        if isinstance(entry, dict):
            return self._get(entry, h, key, default, shift + self.BITS)
        if isinstance(entry, list):
            for k, v in entry:
                if k == key:
                    return v
            return default
        return default

    def assoc(self, key: K, value: V) -> "PersistentHashMap[K, V]":
        h = hash(key)
        new_root, added = self._assoc(self._root, h, key, value, 0)
        return PersistentHashMap(new_root, self._size + (1 if added else 0))

    def _assoc(
        self, node: Dict[int, Any], h: int, key: K, value: V, shift: int = 0
    ) -> Tuple[Dict[int, Any], bool]:
        idx = (h >> shift) & self.MASK
        new_node: Dict[int, Any] = dict(node)
        added = False

        if idx not in node:
            new_node[idx] = [[key, value]]
            added = True
        else:
            entry = node[idx]
            if isinstance(entry, dict):
                child, added = self._assoc(entry, h, key, value, shift + self.BITS)
                new_node[idx] = child
            elif isinstance(entry, list):
                new_list = []
                found = False
                is_exact_hash = False
                for k, v in entry:
                    if k == key:
                        new_list.append([key, value])
                        found = True
                    else:
                        new_list.append([k, v])
                    if hash(k) == h:
                        is_exact_hash = True

                if not found:
                    if is_exact_hash or len(entry) < self.BUCKET_SIZE:
                        new_list.append([key, value])
                        new_node[idx] = new_list
                        added = True
                    else:
                        sub_node: Dict[int, Any] = {}
                        for k, v in entry:
                            sub_node, _ = self._assoc(
                                sub_node, hash(k), k, v, shift + self.BITS
                            )
                        sub_node, added = self._assoc(
                            sub_node, h, key, value, shift + self.BITS
                        )
                        new_node[idx] = sub_node
                else:
                    new_node[idx] = new_list
        return new_node, added

    def dissoc(self, key: K) -> "PersistentHashMap[K, V]":
        h = hash(key)
        new_root, removed = self._dissoc(self._root, h, key, 0)
        return PersistentHashMap(new_root, self._size - (1 if removed else 0))

    def _dissoc(
        self, node: Dict[int, Any], h: int, key: K, shift: int = 0
    ) -> Tuple[Dict[int, Any], bool]:
        idx = (h >> shift) & self.MASK
        if idx not in node:
            return node, False
        entry = node[idx]
        new_node: Dict[int, Any] = dict(node)
        removed = False

        if isinstance(entry, dict):
            child, removed = self._dissoc(entry, h, key, shift + self.BITS)
            if child:
                new_node[idx] = child
            else:
                del new_node[idx]
        elif isinstance(entry, list):
            new_list = [[k, v] for k, v in entry if k != key]
            if len(new_list) < len(entry):
                removed = True
            if new_list:
                new_node[idx] = new_list
            else:
                del new_node[idx]
        return new_node, removed

    def contains(self, key: K) -> bool:
        sentinel = object()
        return self.get(key, default=sentinel) is not sentinel

    def keys(self) -> Iterator[K]:
        yield from self._iter_keys(self._root)

    def _iter_keys(self, node: Dict[int, Any]) -> Iterator[K]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_keys(entry)
            elif isinstance(entry, list):
                for k, _ in entry:
                    yield k

    def values(self) -> Iterator[V]:
        yield from self._iter_values(self._root)

    def _iter_values(self, node: Dict[int, Any]) -> Iterator[V]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_values(entry)
            elif isinstance(entry, list):
                for _, v in entry:
                    yield v

    def items(self) -> Iterator[Tuple[K, V]]:
        yield from self._iter_items(self._root)

    def _iter_items(self, node: Dict[int, Any]) -> Iterator[Tuple[K, V]]:
        for entry in node.values():
            if isinstance(entry, dict):
                yield from self._iter_items(entry)
            elif isinstance(entry, list):
                for k, v in entry:
                    yield k, v

    def to_dict(self) -> Dict[K, V]:
        return dict(self.items())

    def to_json(self) -> str:
        items = {str(k): self._json_encoder(v) for k, v in self.items()}
        return json.dumps(items, default=str)

    @classmethod
    def from_json(
        cls: Type["PersistentHashMap[K, V]"],
        json_str: str,
        key_decoder: Optional[Callable[[Any], K]] = None,
        value_decoder: Optional[Callable[[Any], V]] = None,
    ) -> "PersistentHashMap[K, V]":
        items = json.loads(json_str)
        m = cls()
        for k, v in items.items():
            key = key_decoder(k) if key_decoder else k
            value = value_decoder(v) if value_decoder else v
            m = m.assoc(key, value)
        return m

    @classmethod
    def from_dict(cls, d: Dict[K, V]) -> "PersistentHashMap[K, V]":
        m = cls()
        for k, v in d.items():
            m = m.assoc(k, v)
        return m

    def __iter__(self) -> Iterator[K]:
        return self.keys()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PersistentHashMap):
            return dict(self.items()) == dict(other.items())
        if isinstance(other, dict):
            return dict(self.items()) == other
        return False

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset(self.items()))
        return self._hash

    def __repr__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self.items())
        return f"PersistentHashMap({{{items}}})"

"""Module for realtime dataset management."""

from typing import Any, Dict, List, TypeVar, Generic

K, V = TypeVar("K"), TypeVar("V")

class SortedQueue(Generic[K, V]):
    """Queue of key-value pairs, whose keys are always sorted.
    
    As it is a queue, only FIFO modification is allowed.
    Therefore, pushed keys must be greater than the current last key.
    """

    def __init__(self):
        self._keys: List[K] = []
        self._values: List[V] = []


class DatasetTracker:
    """Holds dataset modifications and provides searching API."""

    def __init__(self):
        self._modifications: Dict[str, SortedQueue[float, Dict[str, Any]]]

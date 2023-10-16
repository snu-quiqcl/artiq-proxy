"""Module for realtime dataset management."""

from typing import Any, Dict, List, Tuple, TypeVar, Generic

K, V = TypeVar("K"), TypeVar("V")

class SortedQueue(Generic[K, V]):
    """Queue of key-value pairs, whose keys are always sorted.
    
    As it is a queue, only FIFO modification is allowed.
    Therefore, pushed keys must be greater than the current last key.
    """

    def __init__(self):
        self._keys: List[K] = []
        self._values: List[V] = []

    def push(self, key: K, value: V):
        """Append a key-value pair to the queue.
        
        Args:
            key: Key for searching. It must be greater than the last key.
            value: Corresponding value.
        """
        self._keys.append(key)
        self._values.append(value)

    def pop(self) -> Tuple[K, V]:
        """Removes and returns the key-value pair at the front of the queue.
        
        The queue must not be empty.
        """
        return self._keys.pop(0), self._values.pop(0)


class DatasetTracker:
    """Holds dataset modifications and provides searching API."""

    def __init__(self):
        self._modifications: Dict[str, SortedQueue[float, Dict[str, Any]]]

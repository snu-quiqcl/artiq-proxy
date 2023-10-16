"""Module for realtime dataset management."""

import bisect
import functools
import itertools
from collections import deque, defaultdict
from typing import Any, Dict, Optional, Tuple, TypeVar, Generic

K, V = TypeVar("K"), TypeVar("V")

class SortedQueue(Generic[K, V]):
    """Queue of key-value pairs, whose keys are always sorted.
    
    As it is a queue, only FIFO modification is allowed.
    Therefore, pushed keys must be greater than the current last key.
    """

    def __init__(self, maxlen: Optional[int] = None):
        """
        Args:
            maxlen: The maximum length of deques.
        """
        self._keys = deque[K](maxlen=maxlen)
        self._values = deque[V](maxlen=maxlen)

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
        return self._keys.popleft(), self._values.popleft()
    
    def tail(self, key: K) -> Tuple[K, Tuple[V]]:
        """Returns the most recent key and the values after the given key.
        
        Args:
            key: The search point key.
        
        Returns:
            (latest_key, values): latest_key is the biggest key, including the
              given key. All the values whose keys are bigger than the given key
              are returned in a tuple. If the given key is the biggest, the
              return value will be (key, ()).
        """
        i = bisect.bisect(self._keys, key)
        latest_key = self._keys[-1] if i < len(self._keys) else key
        values = tuple(itertools.islice(self._values, i, None))
        return latest_key, values


Modification = Dict[str, Any]
ModificationQueue = SortedQueue[float, Modification]


class DatasetTracker:
    """Holds dataset modifications and provides searching API."""

    def __init__(self, maxlen: Optional[int] = None):
        """
        Args:
            maxlen: The maximum length of modification queues.
        """
        factory = functools.partial(ModificationQueue, maxlen=maxlen)
        self._modifications = defaultdict[str, ModificationQueue](factory)

    def datasets(self) -> Tuple[str]:
        """Returns the available dataset names."""
        return tuple(self._modifications)

    def add(self, dataset: str, timestamp: float, modification: Modification):
        """Adds a modification record.
        
        Args:
            dataset: The dataset name of the modification.
            timestamp: The timestamp of the modification.
            modification: Modification dict, e.g., {"action": "append", "x": value}.
        """
        self._modifications[dataset].push(timestamp, modification)

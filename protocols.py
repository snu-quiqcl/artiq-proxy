"""Protocol module for defining common forms."""

import bisect
import itertools
from collections import deque
from typing import Optional, TypeVar, Generic

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

    def pop(self) -> tuple[K, V]:
        """Removes and returns the key-value pair at the front of the queue.
        
        The queue must not be empty.
        """
        return self._keys.popleft(), self._values.popleft()

    def tail(self, key: K) -> tuple[K, tuple[V, ...]]:
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

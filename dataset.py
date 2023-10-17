"""Module for realtime dataset management."""

import bisect
import itertools
import logging
import time
from collections import deque
from typing import Any, Dict, Optional, Tuple, TypeVar, Generic

K, V = TypeVar("K"), TypeVar("V")

logger = logging.getLogger(__name__)

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

    def tail(self, key: K) -> Tuple[K, Tuple[V, ...]]:
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
        self._maxlen = maxlen
        self._modifications: Dict[str, ModificationQueue] = {}

    def datasets(self) -> Tuple[str]:
        """Returns the available dataset names."""
        return tuple(self._modifications)

    def addDataset(self, dataset: str):
        """Adds a dataset entry.
        
        Args:
            dataset: New dataset name.
        """
        if dataset in self._modifications:
            logger.warning("Dataset %s already exists hence is replaced.", dataset)
        self._modifications[dataset] = ModificationQueue(self._maxlen)

    def removeDataset(self, dataset: str):
        """Removes a dataset entry.
        
        Args:
            dataset: Dataset name to remove.
        """
        removed = self._modifications.pop(dataset, None)
        if removed is None:
            logger.error("Cannot remove dataset %s since it does not exist.", dataset)

    def add(self, dataset: str, timestamp: float, modification: Modification):
        """Adds a modification record.
        
        Args:
            dataset: The dataset name of the modification.
            timestamp: The timestamp of the modification.
            modification: Modification dict, e.g., {"action": "append", "x": value}.
        """
        queue = self._modifications.get(dataset, None)
        if queue is None:
            logger.error("Cannot add a modification to dataset %s since it does not exist.", dataset)
            return
        queue.push(timestamp, modification)

    def since(self, dataset: str, timestamp: float) -> Tuple[float, Tuple[Modification, ...]]:
        """Returns the latest timestamp and modifications since the given timestamp.

        See SortedQueue.tail() for details.
        
        Args:
            dataset: Target dataset name.
            timestamp: The last timestamp of the previous update.
              Any modifications added after this timestamp will be returned.
        """
        queue = self._modifications.get(dataset, None)
        if queue is None:
            logger.error("Cannot call since() for dataset %s since it does not exist.", dataset)
            return
        return queue.tail(timestamp)


def notify_callback(tracker: DatasetTracker, mod: Dict[str, Any]):
    """Adds modification to the tracker called as notify_cb() of sipyco system.
    
    Args:
        tracker: Target dataset tracket object.
        mod: The argument of notify_cb() called by sipyco.sync_struct.Subscriber.
    """
    action = mod["action"]
    if action == "init":
        return
    if action == "setitem" and not mod["path"]:
        tracker.addDataset(mod["key"])
        return
    if action == "delitem" and not mod["path"]:
        tracker.removeDataset(mod["key"])
        return
    dataset, *_ = mod.pop("path")
    tracker.add(dataset, time.time(), mod)

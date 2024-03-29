"""Module for realtime dataset management."""

import asyncio
import bisect
import itertools
import logging
import time
from collections import deque
from typing import Any, Optional, TypeVar, Generic

from tracker import Tracker

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


Dataset = dict[str, Any]
Modification = dict[str, Any]
ModificationQueue = SortedQueue[float, Modification]


class DatasetTracker(Tracker[Dataset]):
    """Holds dataset modifications and provides searching API.
    
    Attributes:
        list_modified: The event set when the dataset list is modified.
        modified: Dict(key=dataset_name, value=event) where the event is set
          when any new modification to the dataset is added.
    """

    def __init__(self, maxlen: Optional[int] = None):
        """
        Args:
            maxlen: The maximum length of modification queues.
        """
        super().__init__()
        self.list_modified = asyncio.Event()
        self.modified: dict[str, asyncio.Event] = {}
        self._maxlen = maxlen
        self._modifications: dict[str, ModificationQueue] = {}
        self._last_deleted: dict[str, float] = {}

    def datasets(self) -> tuple[str, ...]:
        """Returns the available dataset names."""
        return tuple(self._modifications)

    def add_dataset(self, dataset: str):
        """Adds a dataset entry.
        
        Args:
            dataset: New dataset name. If the same name already exists, it is
              overwritten by the new one. Since the old one is deleted, the time
              is saved in _last_deleted.
        """
        overwritten = dataset in self._modifications
        if overwritten:
            self._last_deleted[dataset] = time.time()
            logger.warning("Dataset %s already exists hence is replaced.", dataset)
            self._notify_modified(dataset)
        self._modifications[dataset] = ModificationQueue(self._maxlen)
        self.modified[dataset] = asyncio.Event()
        if not overwritten:
            self._notify_list_modified()

    def remove_dataset(self, dataset: str):
        """Removes a dataset entry.
        
        Args:
            dataset: Dataset name to remove.
        """
        removed = self._modifications.pop(dataset, None)
        if removed is None:
            logger.error("Cannot remove dataset %s since it does not exist.", dataset)
            return
        self._last_deleted[dataset] = time.time()
        self._notify_modified(dataset)
        self.modified.pop(dataset)
        self._notify_list_modified()


    def add(self, dataset: str, timestamp: float, modification: Modification):
        """Adds a modification record.
        
        Args:
            dataset: The dataset name of the modification.
            timestamp: The timestamp of the modification.
            modification: Modification dict, e.g., {"action": "append", "x": value}.
        """
        queue = self._modifications.get(dataset, None)
        if queue is None:
            logger.error("Cannot add modification to dataset %s since it does not exist.", dataset)
            return
        queue.push(timestamp, modification)
        self._notify_modified(dataset)

    def since(self, dataset: str, timestamp: float) -> tuple[float, tuple[Modification, ...]]:
        """Returns the latest timestamp and modifications since the given timestamp.
        
        Args:
            dataset: Target dataset name.
            timestamp: The last timestamp of the previous update.
              Any modifications added after this timestamp will be returned.
        
        Returns:
            (t, m) where t is the latest timestamp of the modifications and m is
              a tuple of modifications that are made strictly after the given
              timestamp.
            When the dataset was deleted after the given timestamp (even if it
              exists now) or it does not exist, it returns (-1, ()).
            Note that when there is no modification after the given timestamp,
              it returns (timestamp, ()).
        """
        if self._last_deleted.get(dataset, -1) > timestamp:
            return (-1, ())
        queue = self._modifications.get(dataset, None)
        if queue is None:
            logger.error("Cannot call since() for dataset %s since it does not exist.", dataset)
            return (-1, ())
        return queue.tail(timestamp)

    def get(self, key: str) -> tuple[float, Any]:
        """Returns the current timestamp and target dataset contents.

        See artiq.master.databases.DatasetDB for detailed target structure.
        
        Args:
            key: The key (name) of the dataset.
        
        Returns:
            If the dataset is not initialized yet or there is no dataset with
              the given key, it returns (-1, ()).
        """
        value = self._target.get(key, None)
        if value is None:
            return -1, ()
        return time.time(), value[1]  # value = persist, data, metadata

    def _notify_list_modified(self):
        """Sets and clears the modified event for the dataset list.
        
        All the coroutines that are waiting for the modified event will be awakened.
        """
        self.list_modified.set()
        self.list_modified.clear()

    def _notify_modified(self, dataset: str):
        """Sets and clears the modified event for the specific dataset.

        All the coroutines that are waiting for the modified event will be awakened.
        
        Args:
            dataset: Target dataset name.
        """
        modified = self.modified[dataset]
        modified.set()
        modified.clear()

    def notify_callback(self, mod: dict[str, Any]):
        """Overridden."""
        action = mod["action"]
        if action == "init":
            return
        if not mod["path"]:
            if action == "setitem":
                self.add_dataset(mod["key"])
            elif action == "delitem":
                self.remove_dataset(mod["key"])
            else:
                logger.error("Unexpected mod: %s.", mod)
            return
        dataset, *_ = mod.pop("path")
        self.add(dataset, time.time(), mod)

"""Module for realtime schedule management."""

import asyncio
import time
from typing import Any

from tracker import Tracker

Schedule = dict[int, Any]


class ScheduleTracker(Tracker[Schedule]):
    """Holds schedule in real time.
    
    Attributes:
        modified: The event set when any new modification to the schedule is added.
        latest: The timestamp of the last modification.
    """

    def __init__(self):
        """Extended."""
        super().__init__()
        self.modifed = asyncio.Event()
        self.latest = -1

    def get(self) -> tuple[float, Schedule]:
        """Returns the current timestamp and schedule."""
        return self.latest, self._target
    
    def notify_modified(self):
        """Sets and clears the modified event.
        
        All the coroutines that are waiting for the modified event will be awakened.
        """
        self.latest = time.time()
        self.modifed.set()
        self.modifed.clear()


def notify_callback(tracker: ScheduleTracker, _mod: dict[str, Any]):
    """Notifies modification to the tracker called as notify_cb() of sipyco system."""
    tracker.notify_modified()

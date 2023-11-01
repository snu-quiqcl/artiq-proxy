"""Module for realtime schedule management."""

import asyncio
import time
from typing import Any, Optional

from tracker import Tracker

class ScheduleTracker(Tracker[dict[int, Any]]):
    """Holds schedule in real time.
    
    Attributes:
        modified: The event set when any new modification to the schedule is added.
    """

    def __init__(self):
        """Extended."""
        super().__init__()
        self.modifed = asyncio.Event()

    def get(self) -> tuple[float, dict[int, Any]]:
        """Returns the current timestamp and schedule."""
        return time.time(), self._target
    
    def notify_modified(self):
        """Sets and clears the modified event.
        
        All the coroutines that are waiting for the modified event will be awakened.
        """
        self.modifed.set()
        self.modifed.clear()


def notify_callback(tracker: ScheduleTracker, _mod: dict[str, Any]):
    """Notifies modification to the tracker called as notify_cb() of sipyco system."""
    tracker.notify_modified()

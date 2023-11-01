"""Module for realtime schedule management."""

import asyncio
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

    def get(self) -> dict[int, Any]:
        """Returns the current schedule."""
        return self._target
    
    def notify_modified(self):
        """Sets and clears the modified event.
        
        All the coroutines that are waiting for the modified event will be awakened.
        """
        self.modifed.set()
        self.modifed.clear()

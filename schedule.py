"""Module for realtime schedule management."""

from typing import Any

from tracker import Tracker

class ScheduleTracker(Tracker[dict[int, Any]]):
    """Holds schedule in real time."""

    def get(self) -> dict[int, Any]:
        """Returns the current schedule."""
        return self._target

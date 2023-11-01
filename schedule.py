"""Module for realtime schedule management."""

from typing import Any, Optional

class ScheduleTracker:
    """Holds schedule in real time."""

    def __init__(self):
        self._target: Optional[dict[int, Any]] = None

    def get(self) -> dict[int, Any]:
        """Returns the current schedule."""
        return self._target

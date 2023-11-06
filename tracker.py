"""Module for base tracker."""

from typing import Any, Generic, Optional, TypeVar

T = TypeVar("T")

class Tracker(Generic[T]):
    """Base tracker class."""

    def __init__(self):
        self._target: Optional[T] = None

    def target_builder(self, struct: T) -> T:
        """Initializes the target with the given struct.
        
        See sipyco.sync_struct.Subscriber for details.

        This will make self._target the synchronized structure of the notifier.

        Args:
            struct: The initial structure for the target.
        """
        self._target = struct
        return self._target

    def notify_callback(self, mod: dict[str, Any]):
        """Notifies modification to the tracker called as notify_cb() of sipyco system."""

"""Module for base tracker."""

from typing import Generic, Optional, TypeVar

T = TypeVar("T")

class Tracker(Generic[T]):
    """Base tracker class."""

    def __init__(self):
        self._target: Optional[T] = None

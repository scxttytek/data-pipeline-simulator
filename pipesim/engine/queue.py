from __future__ import annotations
import queue
from typing import Generic, TypeVar, Optional

T = TypeVar("T")

class BoundedQueue(Generic[T]):
    """
    Thin wrapper around queue.Queue to make backpressure explicit.
    If maxsize > 0, put() blocks when full.
    """
    def __init__(self, name: str, maxsize: int = 0):
        self.name = name
        self._q: queue.Queue[T] = queue.Queue(maxsize=maxsize)

    def put(self, item: T, timeout: Optional[float] = None) -> None:
        self._q.put(item, timeout=timeout)

    def get(self, timeout: Optional[float] = None) -> T:
        return self._q.get(timeout=timeout)

    def task_done(self) -> None:
        self._q.task_done()

    def qsize(self) -> int:
        return self._q.qsize()

    @property
    def maxsize(self) -> int:
        return self._q.maxsize

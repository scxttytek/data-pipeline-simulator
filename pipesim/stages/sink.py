from __future__ import annotations
from typing import Optional
import json
import os

from pipesim.engine.event import Event
from pipesim.engine.queue import BoundedQueue
from pipesim.engine.metrics import Metrics

class StdoutSink:
    def __init__(self, name: str, *, limit: int = 5):
        self.name = name
        self.limit = limit

    def consume(self, in_q: BoundedQueue[Event], stop_id: str, metrics: Metrics) -> None:
        printed = 0
        while True:
            ev = in_q.get()
            try:
                if ev.id == stop_id:
                    return
                metrics.inc(f"{self.name}.in", 1)
                if printed < self.limit:
                    print(json.dumps(ev.payload, sort_keys=True))
                    printed += 1
            finally:
                in_q.task_done()

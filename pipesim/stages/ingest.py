from __future__ import annotations
from typing import Dict, Any
import random
import time

from pipesim.engine.event import new_event, Event
from pipesim.engine.queue import BoundedQueue
from pipesim.engine.metrics import Metrics

class OrdersIngest:
    def __init__(self, name: str, *, rng_seed: int = 123):
        self.name = name
        self.rng = random.Random(rng_seed)

    def emit(self, n: int, out_q: BoundedQueue[Event], metrics: Metrics) -> None:
        for _ in range(n):
            price = round(self.rng.uniform(5, 200), 2)
            qty = self.rng.randint(1, 5)
            payload = {
                "order_id": self.rng.randint(100000, 999999),
                "price": price,
                "qty": qty,
                "sku": f"SKU-{self.rng.randint(1, 200):03d}",
            }
            out_q.put(new_event(payload))
            metrics.inc(f"{self.name}.out", 1)

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
import time
import statistics

@dataclass
class Metrics:
    counters: Dict[str, int] = field(default_factory=dict)
    latencies_ms: List[float] = field(default_factory=list)
    queue_depth_samples: List[dict] = field(default_factory=list)
    started_ts: float = field(default_factory=time.time)
    finished_ts: float | None = None

    def inc(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n

    def observe_latency_ms(self, ms: float) -> None:
        self.latencies_ms.append(ms)

    def sample_queue_depth(self, depths: dict) -> None:
        depths = dict(depths)
        depths["_ts"] = time.time()
        self.queue_depth_samples.append(depths)

    def finalize(self) -> None:
        self.finished_ts = time.time()

    def summary(self) -> dict:
        dur = (self.finished_ts or time.time()) - self.started_ts
        lats = sorted(self.latencies_ms)
        def pct(p: float) -> float | None:
            if not lats:
                return None
            idx = int(round((p/100) * (len(lats)-1)))
            return lats[idx]

        return {
            "duration_s": dur,
            "counters": self.counters,
            "latency_ms": {
                "count": len(lats),
                "p50": pct(50),
                "p95": pct(95),
                "mean": (statistics.mean(lats) if lats else None),
            },
            "queue_depth_samples": self.queue_depth_samples,
        }

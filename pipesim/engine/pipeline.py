from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any
import json
import os
import time
import uuid

from .event import Event
from .metrics import Metrics
from .queue import BoundedQueue
from .replay import RunArtifact

@dataclass
class Pipeline:
    name: str
    stages: List[Any]  # Stage-like objects
    queues: List[BoundedQueue[Event]]

    def run(self, *, max_events: int = 1000, sample_every: int = 50) -> RunArtifact:
        metrics = Metrics()
        run_id = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        stop_id = f"__STOP__{run_id}"

        # First stage is a source: must implement emit(n, out_q)
        source = self.stages[0]
        # Last stage is a sink: must implement consume(in_q, stop_id, metrics)
        sink = self.stages[-1]

        # Start: emit events into first queue
        source.emit(max_events, self.queues[0], metrics)

        # Push a stop sentinel so downstream knows when to stop
        self.queues[0].put(Event(id=stop_id, ts=time.time(), payload={"_stop": True}))

        # Run middle stages in a simple single-threaded scheduler (deterministic).
        # This is intentionally simple; you can evolve to threaded workers later.
        middle = self.stages[1:-1]
        alive = [True for _ in middle]

        processed = 0
        while any(alive):
            for i, st in enumerate(middle):
                if not alive[i]:
                    continue
                try:
                    alive[i] = st.run_one(metrics, stop_event_id=stop_id)
                    processed += 1
                    if processed % sample_every == 0:
                        metrics.sample_queue_depth({q.name: q.qsize() for q in self.queues})
                except Exception:
                    # On hard failure, route the event to DLQ if sink supports it.
                    # For now, we just stop loudly.
                    metrics.inc("pipeline.hard_fail", 1)
                    raise

        # Finally, let sink drain until stop sentinel
        sink.consume(self.queues[-1], stop_id, metrics)

        metrics.finalize()
        artifact = RunArtifact(
            run_id=run_id,
            pipeline_name=self.name,
            created_ts=time.time(),
            metrics=metrics.summary(),
            config_snapshot={},  # filled by loader for replay
        )
        return artifact

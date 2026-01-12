from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Optional
import random
import time

from .event import Event
from .queue import BoundedQueue
from .metrics import Metrics

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay_s: float = 0.05
    backoff: float = 2.0

    def delay_for_attempt(self, attempt: int) -> float:
        # attempt is 1-based (1 = first retry delay)
        return self.base_delay_s * (self.backoff ** (attempt - 1))

class Stage:
    """
    Base stage: pulls Events from in_q and pushes to out_q.
    Implement process(event) -> Event | None
    Returning None means 'drop' (count it explicitly if you do).
    """
    def __init__(
        self,
        name: str,
        in_q: Optional[BoundedQueue[Event]],
        out_q: Optional[BoundedQueue[Event]],
        *,
        retry: RetryPolicy | None = None,
        fail_prob: float = 0.0,
        rng_seed: int | None = None,
        idempotency: bool = True,
    ):
        self.name = name
        self.in_q = in_q
        self.out_q = out_q
        self.retry = retry or RetryPolicy()
        self.fail_prob = fail_prob
        self.rng = random.Random(rng_seed)
        self.idempotency = idempotency
        self._seen_ids: set[str] = set()

    def maybe_fail(self) -> None:
        if self.fail_prob > 0 and self.rng.random() < self.fail_prob:
            raise RuntimeError(f"Injected failure in stage {self.name}")

    def process(self, event: Event) -> Event | None:
        raise NotImplementedError

    def run_one(self, metrics: Metrics, stop_event_id: str | None = None) -> bool:
        """
        Returns False when stage should stop, True otherwise.
        stop_event_id: if encountered, pass through and allow pipeline to terminate.
        """
        assert self.in_q is not None, "Source stages should override run loop."

        event = self.in_q.get()
        t0 = time.time()

        try:
            # Propagate stop sentinel
            if stop_event_id is not None and event.id == stop_event_id:
                if self.out_q is not None:
                    self.out_q.put(event)
                return False

            # Idempotency guard (per-stage)
            if self.idempotency and event.id in self._seen_ids:
                metrics.inc(f"{self.name}.deduped", 1)
                return True

            attempt = 0
            while True:
                try:
                    self.maybe_fail()
                    out = self.process(event)
                    if self.idempotency:
                        self._seen_ids.add(event.id)
                    if out is not None and self.out_q is not None:
                        self.out_q.put(out)
                        metrics.inc(f"{self.name}.out", 1)
                    else:
                        metrics.inc(f"{self.name}.dropped", 1)
                    break
                except Exception:
                    attempt += 1
                    metrics.inc(f"{self.name}.errors", 1)
                    if attempt >= self.retry.max_attempts:
                        metrics.inc(f"{self.name}.failed", 1)
                        raise
                    delay = self.retry.delay_for_attempt(attempt)
                    metrics.inc(f"{self.name}.retries", 1)
                    time.sleep(delay)
        finally:
            metrics.observe_latency_ms((time.time() - t0) * 1000)
            self.in_q.task_done()

        return True

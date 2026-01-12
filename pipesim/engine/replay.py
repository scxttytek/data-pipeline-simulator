from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict
import json
import os

@dataclass
class RunArtifact:
    run_id: str
    pipeline_name: str
    created_ts: float
    metrics: Dict[str, Any]
    config_snapshot: Dict[str, Any]

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "created_ts": self.created_ts,
            "metrics": self.metrics,
            "config_snapshot": self.config_snapshot,
        }

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

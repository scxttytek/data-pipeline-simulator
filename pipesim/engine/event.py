
---

## 3) Core engine code

### `pipesim/engine/event.py`
```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict
import time
import uuid

@dataclass(frozen=True)
class Event:
    id: str
    ts: float
    payload: Dict[str, Any]
    schema_version: int = 1
    meta: Dict[str, Any] = field(default_factory=dict)

def new_event(payload: Dict[str, Any], *, schema_version: int = 1, meta: Dict[str, Any] | None = None) -> Event:
    return Event(
        id=str(uuid.uuid4()),
        ts=time.time(),
        payload=payload,
        schema_version=schema_version,
        meta=meta or {},
    )

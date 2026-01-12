from __future__ import annotations
from pipesim.engine.stage import Stage

class AddTotal(Stage):
    def process(self, event):
        p = dict(event.payload)
        p["total"] = round(float(p["price"]) * int(p["qty"]), 2)
        return event.__class__(id=event.id, ts=event.ts, payload=p, schema_version=event.schema_version, meta=event.meta)

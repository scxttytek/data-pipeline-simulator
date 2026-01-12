from __future__ import annotations
from pipesim.engine.stage import Stage

class ValidateOrder(Stage):
    def __init__(self, *args, required=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.required = required or ["order_id", "price", "qty", "sku", "total"]

    def process(self, event):
        p = event.payload
        for k in self.required:
            if k not in p:
                raise ValueError(f"Missing field {k}")
        if p["price"] <= 0 or p["qty"] <= 0:
            raise ValueError("Invalid price/qty")
        return event

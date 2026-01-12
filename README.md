# pipesim

A local data pipeline simulator: ingest → transform → validate → sink.

## Quickstart
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .

pipesim init
pipesim run pipeline_examples/orders_basic.yaml
pipesim report runs/<runfile>.json

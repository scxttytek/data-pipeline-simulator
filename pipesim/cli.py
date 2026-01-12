from __future__ import annotations
import argparse
import os
import time
import yaml

from pipesim.engine.pipeline import Pipeline
from pipesim.engine.queue import BoundedQueue
from pipesim.engine.stage import RetryPolicy
from pipesim.stages.ingest import OrdersIngest
from pipesim.stages.transform import AddTotal
from pipesim.stages.validate import ValidateOrder
from pipesim.stages.sink import StdoutSink

def build_pipeline_from_yaml(path: str) -> Pipeline:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    name = cfg.get("name", "pipeline")

    q_cfg = cfg.get("queues", {})
    qsize = int(q_cfg.get("maxsize", 0))

    stage_cfgs = cfg["stages"]
    if len(stage_cfgs) < 2:
        raise ValueError("Need at least source + sink")

    # Create queues between stages (len(stages)-1)
    queues = []
    for i in range(len(stage_cfgs) - 1):
        queues.append(BoundedQueue(name=f"q{i}", maxsize=qsize))

    stages = []
    for i, s in enumerate(stage_cfgs):
        stype = s["type"]
        sname = s.get("name", stype)

        # common knobs for Stage-based ones
        fail_prob = float(s.get("fail_prob", 0.0))
        retry_cfg = s.get("retry", {}) or {}
        retry = RetryPolicy(
            max_attempts=int(retry_cfg.get("max_attempts", 3)),
            base_delay_s=float(retry_cfg.get("base_delay_s", 0.05)),
            backoff=float(retry_cfg.get("backoff", 2.0)),
        )
        rng_seed = s.get("rng_seed", None)

        if stype == "orders_ingest":
            stages.append(OrdersIngest(sname, rng_seed=int(s.get("rng_seed", 123))))
        elif stype == "add_total":
            stages.append(AddTotal(sname, queues[i-1], queues[i], retry=retry, fail_prob=fail_prob, rng_seed=rng_seed))
        elif stype == "validate_order":
            stages.append(ValidateOrder(sname, queues[i-1], queues[i], retry=retry, fail_prob=fail_prob, rng_seed=rng_seed))
        elif stype == "stdout_sink":
            stages.append(StdoutSink(sname, limit=int(s.get("limit", 5))))
        else:
            raise ValueError(f"Unknown stage type: {stype}")

    pipe = Pipeline(name=name, stages=stages, queues=queues)
    return pipe, cfg

def cmd_init() -> None:
    os.makedirs("pipeline_examples", exist_ok=True)
    sample = """name: orders_basic
queues:
  maxsize: 200
stages:
  - type: orders_ingest
    name: ingest_orders
    rng_seed: 123
  - type: add_total
    name: add_total
    fail_prob: 0.01
    retry:
      max_attempts: 3
      base_delay_s: 0.01
      backoff: 2
  - type: validate_order
    name: validate
    fail_prob: 0.005
    retry:
      max_attempts: 2
      base_delay_s: 0.01
      backoff: 2
  - type: stdout_sink
    name: sink
    limit: 5
"""
    out = os.path.join("pipeline_examples", "orders_basic.yaml")
    if not os.path.exists(out):
        with open(out, "w", encoding="utf-8") as f:
            f.write(sample)
    print(f"Wrote {out}")

def cmd_run(pipeline_path: str, max_events: int) -> None:
    pipe, cfg = build_pipeline_from_yaml(pipeline_path)
    artifact = pipe.run(max_events=max_events)

    # attach snapshot for replay/debug
    artifact.config_snapshot = cfg

    os.makedirs("runs", exist_ok=True)
    out_path = os.path.join("runs", f"{artifact.run_id}.json")
    artifact.save(out_path)
    print(f"Run saved: {out_path}")
    print("Summary:", artifact.metrics)

def cmd_report(run_path: str) -> None:
    import json
    with open(run_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    m = data["metrics"]
    print(f"Pipeline: {data['pipeline_name']}")
    print(f"Run ID:   {data['run_id']}")
    print(f"Duration: {m.get('duration_s'):.3f}s")
    print("Counters:")
    for k, v in sorted(m.get("counters", {}).items()):
        print(f"  {k}: {v}")
    lat = m.get("latency_ms", {})
    print("Latency(ms):", lat)

def main() -> None:
    ap = argparse.ArgumentParser(prog="pipesim")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")

    runp = sub.add_parser("run")
    runp.add_argument("pipeline", help="YAML pipeline config path")
    runp.add_argument("--max-events", type=int, default=1000)

    rep = sub.add_parser("report")
    rep.add_argument("runfile", help="Path to a run json artifact")

    args = ap.parse_args()

    if args.cmd == "init":
        cmd_init()
    elif args.cmd == "run":
        cmd_run(args.pipeline, args.max_events)
    elif args.cmd == "report":
        cmd_report(args.runfile)

if __name__ == "__main__":
    main()

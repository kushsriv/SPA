"""fedcast command line.

  fedcast sim         --dataset syndrift --method fedcast --param 20
  fedcast coordinator --dataset syndrift [--raw]
  fedcast edges       --dataset syndrift --method fedcast --param 20 --speed 10
  fedcast demo        --dataset syndrift --method fedcast --param 20     (coordinator + edges)
  fedcast reset-topics
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import time

from .data import load
from .sim import SimConfig, run


def _common(p):
    p.add_argument("--dataset", default="syndrift")
    p.add_argument("--method", default="fedcast",
                   choices=["fedcast", "naive", "periodic", "pdelta", "change", "norm", "kfed", "raw"])
    p.add_argument("--param", type=float, default=20.0,
                   help="fedcast: bytes/s per node; periodic/pdelta/kfed: period s; change: eps; norm: rel")
    p.add_argument("--nodes", type=int, default=10)
    p.add_argument("--duration", type=float, default=300.0, help="stream seconds")
    p.add_argument("--partition", default="dirichlet:0.3")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--max-points", type=int, default=60000)
    p.add_argument("--rank", default="objective", choices=["objective", "norm", "uniform", "hybrid", "lloyd", "lloydj"])
    p.add_argument("--quant", action="store_true", help="8-bit quantised deltas with error feedback")


def _cfg(a) -> SimConfig:
    return SimConfig(method=a.method, param=a.param, nodes=a.nodes, duration=a.duration,
                     partition=a.partition, seed=a.seed, rank=a.rank, quant=a.quant)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fedcast", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sim", help="run one simulated experiment")
    _common(s)
    for name in ("edges", "demo"):
        e = sub.add_parser(name, help="run edge nodes against Kafka" if name == "edges"
                           else "run coordinator and edges against Kafka")
        _common(e)
        e.add_argument("--bootstrap", default="localhost:9092")
        e.add_argument("--speed", type=float, default=10.0, help="stream seconds per wall second")
        e.add_argument("--out", default="results/kafka_edges.json")
        e.add_argument("--node-ids", default="", help="comma-separated subset of nodes to run here")
        e.add_argument("--start-at", type=float, default=0.0, help="shared start time (epoch seconds)")
    c = sub.add_parser("coordinator", help="run the coordinator against Kafka")
    c.add_argument("--dataset", default="syndrift")
    c.add_argument("--bootstrap", default="localhost:9092")
    c.add_argument("--raw", action="store_true")
    c.add_argument("--group", default="fedcast-coordinator")
    c.add_argument("--idle-exit", type=float, default=0.0)
    c.add_argument("--out", default="results/kafka_coordinator.json")
    c.add_argument("--history", default="", help="append every global model to this JSON-lines file")
    r = sub.add_parser("reset-topics", help="delete and recreate the fsc.* topics")
    r.add_argument("--bootstrap", default="localhost:9092")
    a = ap.parse_args(argv)

    if a.cmd == "sim":
        res = run(_cfg(a), load(a.dataset, seed=a.seed, max_points=a.max_points))
        res.pop("series"), res.pop("lam_trace"), res.pop("config")
        print(json.dumps(res, indent=1))
        return
    from . import kafka_runtime as kr
    if a.cmd == "reset-topics":
        kr.reset_topics(a.bootstrap)
        print("topics recreated")
        return
    if a.cmd == "coordinator":
        ds = load(a.dataset, max_points=1000)
        rc = kr.CoordRunConfig(bootstrap=a.bootstrap, group=a.group, k=ds.k, dim=ds.X.shape[1],
                               raw=a.raw, idle_exit=a.idle_exit, out=a.out, history=a.history)
        print(json.dumps({k: v for k, v in kr.run_coordinator(rc).items() if k not in ("state", "state_mass")}, indent=1))
        return
    erc = kr.EdgeRunConfig(bootstrap=a.bootstrap, dataset=a.dataset, max_points=a.max_points,
                           speed=a.speed, out=a.out, sim=_cfg(a),
                           node_ids=[int(x) for x in a.node_ids.split(",")] if a.node_ids else None,
                           start_at=a.start_at)
    if a.cmd == "edges":
        res = kr.run_edges(erc)
    else:  # demo
        kr.reset_topics(a.bootstrap)
        ds = load(a.dataset, max_points=1000)
        rc = kr.CoordRunConfig(bootstrap=a.bootstrap, k=ds.k, dim=ds.X.shape[1], raw=a.method == "raw",
                               idle_exit=5.0, out="results/kafka_coordinator.json")
        proc = mp.Process(target=kr.run_coordinator, args=(rc,))
        proc.start()
        time.sleep(2)
        res = kr.run_edges(erc)
        proc.join(60)
    res.pop("series", None)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()

"""Run the experiment suite.  Usage:  python experiments/run.py E1 [E2 ...] [--seeds 5] [--jobs 4]

Each experiment writes one JSON line per run to results/<name>.jsonl. Runs that
are already present are skipped, so an interrupted suite can be resumed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import asdict, replace
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fedcast.data import load  # noqa: E402
from fedcast.microcluster import MCParams  # noqa: E402
from fedcast.sim import SimConfig, run  # noqa: E402

RESULTS = ROOT / "results"
DATASETS = ["syndrift", "nslkdd", "shuttle", "pendigits", "letter"]
BASE = SimConfig(duration=300.0, nodes=10, partition="dirichlet:0.3", links=["wan", "cellular"])

SWEEP = ([("raw", 0), ("naive", 0)]
         + [("periodic", T) for T in (15, 30, 60, 120, 240)]
         + [("pdelta", T) for T in (15, 30, 60, 120, 240)]
         + [("change", e) for e in (0.3, 1.0, 3.0)]
         + [("norm", r) for r in (0.05, 0.2, 0.5)]
         + [("kfed", T) for T in (15, 30, 60, 120)]
         + [("fedcast", B) for B in (5, 10, 20, 50, 150, 500)])


def v(cfg: SimConfig, **kw) -> SimConfig:
    return replace(cfg, **kw)


def experiments(seeds: int):
    S = range(seeds)
    E = {}
    # E1 - cost/quality Pareto fronts on every dataset (RQ1)
    E["E1"] = [(ds, v(BASE, method=m, param=p, seed=s), None) for ds in DATASETS for m, p in SWEEP for s in S]
    # E2 - degree of non-IID (RQ2), incl. aggregator ablations
    parts = ["iid", "dirichlet:1.0", "dirichlet:0.3", "dirichlet:0.1", "exclusive:2", "exclusive:1"]
    variants = [("raw", 0, {}, "raw"), ("fedcast", 20, {}, "fedcast"),
                ("fedcast", 20, {"swaps": 0}, "fedcast-noswap"),
                ("fedcast", 20, {"beta": 0.5}, "fedcast-beta0.5"),
                ("periodic", 60, {}, "periodic"), ("pdelta", 60, {}, "pdelta"), ("kfed", 60, {}, "kfed")]
    E["E2"] = [(ds, v(BASE, method=m, param=p, partition=pt, seed=s, **kw), tag)
               for ds in ["syndrift", "pendigits", "letter"] for pt in parts
               for m, p, kw, tag in variants for s in S]
    # E3 - drift: evolving stream and class-rotation drift (RQ3); series kept
    drift_methods = [("raw", 0), ("fedcast", 10), ("fedcast", 20), ("periodic", 60), ("periodic", 120),
                     ("pdelta", 60), ("kfed", 60), ("change", 3.0), ("norm", 0.5)]
    E["E3"] = [(ds, v(BASE, method=m, param=p, partition=pt, seed=s, eval_period=5.0, eval_window=20.0), None)
               for ds, pt in [("syndrift", "dirichlet:0.3"), ("pendigits", "drift:2"), ("letter", "drift:3")]
               for m, p in drift_methods for s in S]
    # E4 - network heterogeneity and outages (RQ4)
    scen = {"homog-wan": dict(links=["wan"]),
            "hetero": dict(links=["lan", "wan", "cellular", "poor"]),
            "hetero+outage": dict(links=["lan", "wan", "cellular", "poor"], outage_nodes=3)}
    net_methods = [("raw", 0, {}, "raw"), ("fedcast", 20, {}, "fedcast"),
                   ("fedcast", 20, {"use_price": False}, "fedcast-noprice"),
                   ("periodic", 60, {}, "periodic"), ("pdelta", 60, {}, "pdelta"),
                   ("kfed", 60, {}, "kfed"), ("change", 3.0, {}, "change")]
    E["E4"] = [(ds, v(BASE, method=m, param=p, seed=s, **scen[sc], **kw), f"{sc}|{tag}")
               for ds in ["syndrift", "nslkdd"] for sc in scen for m, p, kw, tag in net_methods for s in S]
    # E5 - ablations of FedCAST (all datasets, B = 20)
    abl = [("fedcast", {}), ("no-dual", {"use_dual": False}), ("no-price", {"use_price": False}),
           ("no-novelty", {"use_novelty": False}), ("no-priority", {"gamma": 1.0}),
           ("full-summary", {"full_summary": True}), ("no-swap", {"swaps": 0}),
           ("no-decay", {"staleness_decay": False})]
    E["E5"] = [(ds, v(BASE, method="fedcast", param=20, seed=s, **kw), tag)
               for ds in DATASETS for tag, kw in abl for s in S]
    # E6 - sensitivity (syndrift, pendigits)
    sens = ([("eta", {"eta": x}) for x in (0.1, 0.25, 1.0, 2.0)]
            + [("gamma", {"gamma": x}) for x in (0.5, 0.7, 0.95)]
            + [("rho", {"rho": x}) for x in (0.1, 10.0)]
            + [("half_life", {"mc": MCParams(half_life=x)}) for x in (60.0, 300.0)]
            + [("max_mc", {"mc": MCParams(max_mc=x)}) for x in (25, 100)]
            + [("default", {})])
    E["E6"] = [(ds, v(BASE, method="fedcast", param=20, seed=s, **kw), f"{tag}={list(kw.values())[0] if kw else ''}")
               for ds in ["syndrift", "pendigits"] for tag, kw in sens for s in S]
    # E7 - scale in number of nodes (simulation)
    E["E7"] = [("syndrift", v(BASE, method=m, param=p, nodes=n, seed=s), None)
               for n in (5, 10, 20, 50) for m, p in [("fedcast", 20), ("periodic", 60), ("pdelta", 60), ("kfed", 60)]
               for s in range(min(seeds, 3))]
    # E8 - targeted ablations: what novelty and dual ascent are *for*
    #   novelty -> detection delay of emerging clusters (series kept)
    #   dual ascent -> spending under a loose budget (bytes actually used)
    E["E8"] = ([("syndrift", v(BASE, method="fedcast", param=B, seed=s, eval_period=5.0, eval_window=20.0,
                              use_novelty=nov), f"novelty={nov}")
                for B in (10, 20) for nov in (True, False) for s in S]
               + [(ds, v(BASE, method="fedcast", param=500, seed=s, use_dual=dual), f"dual={dual}")
                  for ds in ["syndrift", "pendigits"] for dual in (True, False) for s in S]
               + [(ds, v(BASE, method="fedcast", param=B, seed=s, links=["lan", "wan", "cellular", "poor"],
                         use_price=pr), f"price={pr}|B={B}")
                  for ds in ["syndrift", "nslkdd"] for B in (150, 500) for pr in (True, False) for s in S])
    # E9 - novelty check: which *ranking* matters (objective vs top-k by magnitude vs
    #      model-free bound) and budget-adaptive resolution; same base config as E1
    variants = [("norm", {"rank": "norm"}), ("uniform", {"rank": "uniform"}),
                ("aq", {"adaptive_q": True}), ("aq2", {"adaptive_q": True, "q_floor_k": 2.0})]
    E["E9"] = ([(ds, v(BASE, method="fedcast", param=2, seed=s), "objective") for ds in DATASETS for s in S]
               + [(ds, v(BASE, method="fedcast", param=B, seed=s, **kw), tag)
                  for ds in DATASETS for tag, kw in variants for B in (2, 5, 10, 20, 50, 150) for s in S])
    # E10 - does objective-aware ranking win on *real* evolving streams? (concept evolution
    #       built from the four real datasets; same configuration as E1 otherwise)
    EV = v(BASE, partition="evolve:0.3")
    real = ["nslkdd", "shuttle", "pendigits", "letter"]
    ranks = [("objective", {}), ("norm", {"rank": "norm"}), ("uniform", {"rank": "uniform"}),
             ("aq2", {"adaptive_q": True, "q_floor_k": 2.0})]
    E["E10"] = ([(ds, v(EV, method="raw", seed=s), "raw") for ds in real for s in S]
                + [(ds, v(EV, method="fedcast", param=B, seed=s, **kw), tag)
                   for ds in real for tag, kw in ranks for B in (2, 5, 10, 20, 50) for s in S]
                + [(ds, v(EV, method=m, param=T, seed=s), m)
                   for ds in real for m in ("kfed", "pdelta") for T in (30, 60, 120) for s in S])
    # E11 - hybrid ranking (objective + magnitude, each normalised) on every setting where
    #       objective and top-k magnitude were compared: static (E1/E9), SynDrift (E9), evolving (E10)
    H = {"rank": "hybrid"}
    E["E11"] = ([(ds, v(BASE, method="fedcast", param=B, seed=s, **H), "hybrid")
                 for ds in DATASETS for B in (2, 5, 10, 20, 50, 150) for s in S]
                + [(ds, v(EV, method="fedcast", param=B, seed=s, **H), "hybrid")
                   for ds in real for B in (2, 5, 10, 20, 50) for s in S])
    # E12 - exact-value ranking (Lloyd excess) on every setting of E9/E10/E11
    L = {"rank": "lloyd"}
    E["E12"] = ([(ds, v(BASE, method="fedcast", param=B, seed=s, **L), "lloyd")
                 for ds in DATASETS for B in (2, 5, 10, 20, 50, 150) for s in S]
                + [(ds, v(EV, method="fedcast", param=B, seed=s, **L), "lloyd")
                   for ds in real for B in (2, 5, 10, 20, 50) for s in S])
    # E13 - combined evaluation of FedCAST-v2 (exact-value ranking + 8-bit deltas with error
    #       feedback) against every compressible baseline given the same quantisation, on static,
    #       evolving and *naturally* drifting / partitioned real streams
    Q = {"quant": True}
    settings = ([(ds, BASE) for ds in DATASETS] + [(ds, EV) for ds in real]
                + [("gas", BASE), ("covtype", BASE), ("intel", v(BASE, partition="natural"))])
    E["E13"] = []
    for ds, base in settings:
        E["E13"] += [(ds, v(base, method="raw", seed=s), "raw") for s in S]
        E["E13"] += [(ds, v(base, method="fedcast", param=B, seed=s, rank="lloyd", **Q), "fedcast-v2")
                     for B in (2, 5, 10, 20, 50) for s in S]
        E["E13"] += [(ds, v(base, method="fedcast", param=B, seed=s, rank="norm", **Q), "topk-q")
                     for B in (2, 5, 10, 20, 50) for s in S]
        E["E13"] += [(ds, v(base, method=m, param=T, seed=s, **Q), f"{m}-q")
                     for m in ("pdelta", "kfed") for T in (30, 60, 120, 240) for s in S]
    # E14 - FedCAST-v3: exact value first, magnitude tie-break, 8-bit deltas, use-it-or-lose-it
    V3 = {"rank": "lloyd+", "quant": True, "overflow": True}
    E["E14"] = [(ds, v(base, method="fedcast", param=B, seed=s, **V3), "fedcast-v3")
                for ds, base in settings for B in (2, 5, 10, 20, 50) for s in S]
    return E


_CACHE = {}


def _key(ds, cfg, tag):
    blob = json.dumps([ds, asdict(cfg), tag], sort_keys=True, default=str)
    return hashlib.sha1(blob.encode()).hexdigest()[:16]


def _work(job):
    ds_name, cfg, tag, keep_series = job
    if ds_name not in _CACHE:
        _CACHE[ds_name] = load(ds_name, max_points=60000)
    r = run(cfg, _CACHE[ds_name])
    if not keep_series:
        r.pop("series")
        r.pop("lam_trace")
    else:
        for e in r["series"]:
            e["recall"] = {str(k): v for k, v in e["recall"].items()}
    r["tag"] = tag
    r["key"] = _key(ds_name, cfg, tag)
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="+")
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--jobs", type=int, default=4)
    a = ap.parse_args()
    RESULTS.mkdir(exist_ok=True)
    E = experiments(a.seeds)
    for name in a.names:
        out = RESULTS / f"{name}.jsonl"
        done = set()
        if out.exists():
            done = {json.loads(line)["key"] for line in out.open()}
        jobs = [(ds, cfg, tag, name in ("E3", "E8") or (name == "E9" and ds == "syndrift"))
                for ds, cfg, tag in E[name] if _key(ds, cfg, tag) not in done]
        # slow runs first for better load balancing
        jobs.sort(key=lambda j: (j[1].method != "raw", j[0] != "nslkdd"))
        print(f"{name}: {len(jobs)} runs to do ({len(done)} cached)", flush=True)
        t0 = time.time()
        with Pool(a.jobs) as pool, out.open("a") as f:
            for i, r in enumerate(pool.imap_unordered(_work, jobs), 1):
                f.write(json.dumps(r) + "\n")
                f.flush()
                if i % 25 == 0 or i == len(jobs):
                    print(f"  {name} {i}/{len(jobs)}  {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()

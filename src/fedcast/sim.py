"""Deterministic discrete-event simulation of the federated system.

Runs exactly the same EdgeNode / Policy / Coordinator / codec code as the
Kafka runtime, but replaces the broker with seeded per-node links (netem.py)
that reproduce Kafka's guarantees relevant here: per-key FIFO order,
retries on loss (retransmitted bytes are counted) and buffering during
outages. This makes large, repeatable parameter sweeps possible on a laptop;
the Kafka runtime (kafka_runtime.py) is used for the systems experiments.
"""
from __future__ import annotations

import heapq
import time
from dataclasses import asdict, dataclass, field

import numpy as np

from . import codec
from .coordinator import CentralCoordinator, Coordinator
from .data import Dataset
from .metrics import evaluate
from .microcluster import MCParams
from .netem import PROFILES, Link, LinkProfile
from .node import EdgeNode
from .partition import exclusive_classes, partition
from .policies import FedCAST, FedCASTMR, KFed, NormTrigger, ChangeThreshold, Naive, Periodic


@dataclass
class SimConfig:
    method: str = "fedcast"          # fedcast | naive | periodic | pdelta | change | norm | kfed | raw
    param: float = 0.0               # budget (B/s) | period | eps | rel | period
    partition: str = "dirichlet:0.5"
    nodes: int = 10
    duration: float = 600.0          # simulated seconds
    tick: float = 1.0
    agg_period: float = 5.0
    eval_period: float = 10.0
    eval_window: float = 60.0        # seconds of recent points used for evaluation
    eval_max: int = 3000
    warmup: float = 0.1              # fraction of the run excluded from averages
    links: list[str] = field(default_factory=lambda: ["wan"])
    outage_nodes: int = 0            # nodes that suffer an outage
    outage: tuple[float, float] = (0.4, 0.55)
    mc: MCParams = field(default_factory=MCParams)
    beta: float = 1.0
    swaps: int = 3
    staleness_decay: bool = True
    window: float = 10.0
    # FedCAST ablation switches
    use_price: bool = True
    use_dual: bool = True
    use_novelty: bool = True
    full_summary: bool = False
    gamma: float = 0.9
    rank: str = "objective"       # objective | norm | uniform | lloyd | lloyd+ (FedCAST scoring)
    overflow: bool = False           # use-it-or-lose-it sends when the token bucket is about to overflow
    adaptive_q: bool = False         # budget-adaptive summary resolution
    q_horizon: float = 60.0          # a full summary may cost at most this many seconds of budget
    q_min: int = 4
    q_floor_k: float = 1.0           # q >= q_floor_k * k
    quant: bool = False              # 8-bit quantised deltas with error feedback
    mr_eps: float = 0.5              # fedcast-mr: max relative scatter increase when coarsening
    aq_mode: str = "k"               # adaptive_q floor: "k" (q >= q_floor_k * k) | "fidelity" (data-driven)
    aq_eps: float = 0.25             # fidelity floor: max relative scatter increase allowed
    aq_warmup: float = 30.0          # seconds of data before the fidelity floor is measured
    eta: float = 0.5
    rho: float = 1.0
    k_local: int = 0                 # k-FED k' (0: oracle number of local classes)
    seed: int = 0


def make_policy(cfg: SimConfig, k_local: int):
    m = cfg.method
    if m == "fedcast":
        return FedCAST(budget=cfg.param * cfg.window, window=cfg.window, use_price=cfg.use_price,
                       use_dual=cfg.use_dual, use_novelty=cfg.use_novelty, full=cfg.full_summary,
                       gamma=cfg.gamma, eta=cfg.eta, rho=cfg.rho, rank=cfg.rank,
                       use_overflow=cfg.overflow)
    if m == "fedcast-mr":
        return FedCASTMR(budget=cfg.param * cfg.window, window=cfg.window, use_price=cfg.use_price,
                         use_dual=cfg.use_dual, use_novelty=cfg.use_novelty, gamma=cfg.gamma, eta=cfg.eta,
                         rho=cfg.rho, eps=cfg.mr_eps, seed=cfg.seed)
    if m == "naive":
        return Naive()
    if m == "periodic":
        return Periodic(cfg.param)
    if m == "pdelta":
        return Periodic(cfg.param, full=False)
    if m == "change":
        return ChangeThreshold(cfg.param)
    if m == "norm":
        return NormTrigger(cfg.param)
    if m == "kfed":
        return KFed(cfg.param, k_local)
    if m == "raw":
        return None
    raise ValueError(m)


def resolution_for_budget(cfg: SimConfig, dim: int, k: int) -> int:
    """Budget-adaptive resolution: the largest q whose full summary costs at most
    q_horizon seconds of the node's budget, but never below the global model's
    resolution k (a node must be able to represent every global cluster) and
    never above max_mc."""
    per_mc = 24 + 4 * dim
    fixed = codec.summary_size(0, 0, dim) + codec.KAFKA_RECORD_OVERHEAD
    q = int((cfg.param * cfg.q_horizon - fixed) // per_mc)
    return int(min(max(q, cfg.q_min, int(round(cfg.q_floor_k * k))), cfg.mc.max_mc))


def fidelity_resolution(cfg: SimConfig, node, now: float) -> int:
    """Data-driven resolution: max(what the budget affords, the smallest r whose
    coarsening raises the node's within-cluster scatter by at most aq_eps)."""
    from .policies import FedCASTMR, coarsen_loss
    per_mc = 24 + 4 * node.dim
    fixed = codec.summary_size(0, 0, node.dim) + codec.KAFKA_RECORD_OVERHEAD
    q_b = int((cfg.param * cfg.q_horizon - fixed) // per_mc)
    _, n, LS, SS = node.current_arrays(now)
    rng = np.random.default_rng(cfg.seed + node.id)
    q_f = len(n)
    for r in FedCASTMR.CANDIDATES:
        if r >= len(n):
            break
        loss, eff = coarsen_loss(n, LS, SS, r, rng)
        if loss <= cfg.aq_eps:
            q_f = eff
            break
    return int(min(max(q_b, q_f, 2), cfg.mc.max_mc))


def run(cfg: SimConfig, ds: Dataset, verbose: bool = False, probe=None) -> dict:
    t0 = time.time()
    rng = np.random.default_rng(cfg.seed)
    streams = partition(ds.y, cfg.nodes, cfg.partition, cfg.duration, cfg.seed, ds.u, ds.groups)
    focus = exclusive_classes(ds.y, streams)
    counts = np.bincount(ds.y)
    minority = [int(c) for c in np.flatnonzero((counts > 0) & (counts < 0.05 * len(ds.y)))]
    k, d = ds.k, ds.X.shape[1]

    # links: profiles are assigned round-robin; some nodes get an outage
    links = []
    for i in range(cfg.nodes):
        p = PROFILES[cfg.links[i % len(cfg.links)]]
        prof = LinkProfile(**{**asdict(p), "outages": []})
        if i < cfg.outage_nodes:
            prof.outages = [(cfg.outage[0] * cfg.duration, cfg.outage[1] * cfg.duration)]
        links.append(Link(prof, np.random.default_rng(cfg.seed * 1000 + i)))

    node_params = cfg.mc
    if cfg.method == "fedcast" and cfg.adaptive_q and cfg.aq_mode == "k":
        node_params = MCParams(**{**asdict(cfg.mc), "max_mc": resolution_for_budget(cfg, d, k)})
    nodes = []
    for i, s in enumerate(streams):
        k_loc = cfg.k_local or max(1, len(np.unique(ds.y[s.idx])))
        nodes.append(EdgeNode(i, d, node_params, make_policy(cfg, k_loc)))
        nodes[-1].quant = cfg.quant
    if cfg.method == "raw":
        q_central = cfg.mc.max_mc * cfg.nodes
        coord = CentralCoordinator(k, d, MCParams(**{**asdict(cfg.mc), "max_mc": q_central}),
                                   beta=1.0, swaps=cfg.swaps, seed=cfg.seed)
    else:
        coord = Coordinator(k, d, cfg.mc.half_life, beta=cfg.beta, swaps=cfg.swaps,
                            staleness_decay=cfg.staleness_decay, seed=cfg.seed)

    # global evaluation stream (all points ordered by time)
    all_idx = np.concatenate([s.idx for s in streams])
    all_t = np.concatenate([s.times for s in streams])
    o = np.argsort(all_t, kind="stable")
    all_idx, all_t = all_idx[o], all_t[o]

    pos = [0] * cfg.nodes
    events: list = []  # (time, seq, kind, payload)
    eseq = 0
    last_arrival = [0.0] * cfg.nodes
    down_bytes = 0
    series = []
    lam_trace = []
    next_agg, next_eval = cfg.agg_period, cfg.eval_period
    kfed_rng = np.random.default_rng(cfg.seed + 7)
    raw_seq = [0] * cfg.nodes

    t = 0.0
    while t < cfg.duration - 1e-9:
        t = round(t + cfg.tick, 9)
        # 1. ingest local points and decide
        for i, node in enumerate(nodes):
            s = streams[i]
            j0 = pos[i]
            j1 = int(np.searchsorted(s.times, t, side="right"))
            pos[i] = j1
            if cfg.method == "raw":
                if j1 > j0:
                    X = ds.X[s.idx[j0:j1]]
                    payload = codec.raw_size(len(X), d) + codec.KAFKA_RECORD_OVERHEAD * len(X)
                    arr, wire, _ = links[i].send(t, payload)
                    arr = max(arr, last_arrival[i])
                    last_arrival[i] = arr
                    node.bytes_up += payload
                    node.wire_up += wire
                    node.msgs_up += len(X)
                    heapq.heappush(events, (arr, eseq, "raw", (i, X, s.times[j0:j1].copy())))
                    eseq += 1
                continue
            for j in range(j0, j1):
                node.ingest(ds.X[s.idx[j]], float(s.times[j]))
            if int(t) % 10 == 0:
                node.mc.prune(t)
            if (cfg.method == "fedcast" and cfg.adaptive_q and cfg.aq_mode == "fidelity"
                    and abs(t - cfg.aq_warmup) < cfg.tick / 2):
                node.mc.shrink_to(fidelity_resolution(cfg, node, t), t)
            node.policy.tick(t)
            dec = node.policy.decide(node, t)
            if dec is None:
                continue
            summ = (node.build_kfed(t, node.policy.k_local, kfed_rng) if dec.kfed
                    else node.build(t, full=dec.full, only_slots=dec.only_slots))
            if cfg.quant:
                buf = codec.encode_summary_q(summ)
                summ = codec.decode_summary(buf)   # error feedback: remember what the server really holds
            else:
                buf = codec.encode_summary(summ)
            payload = len(buf) + codec.KAFKA_RECORD_OVERHEAD
            node.commit(summ, payload)
            arr, wire, _ = links[i].send(t, payload)
            arr = max(arr, last_arrival[i])  # Kafka keeps per-key order
            last_arrival[i] = arr
            heapq.heappush(events, (arr, eseq, "summary", (i, buf, payload, wire, arr - t)))
            eseq += 1

        # 2. deliver everything that has arrived by t
        while events and events[0][0] <= t:
            _, _, kind, pl = heapq.heappop(events)
            if kind == "summary":
                i, buf, payload, wire, lat = pl
                coord.apply(codec.decode_summary(buf))
                nodes[i].on_delivery(payload, wire, lat)
            elif kind == "raw":
                i, X, times = pl
                coord.apply_raw(X, times)
            elif kind == "global":
                i, g, nb = pl
                nodes[i].on_global(g, nb)

        # 3. aggregate and broadcast
        if t + 1e-9 >= next_agg:
            next_agg += cfg.agg_period
            g = coord.aggregate(t)
            if g is not None and cfg.method != "raw":
                nb = codec.global_size(k, d) + codec.KAFKA_RECORD_OVERHEAD
                for i in range(cfg.nodes):
                    arr, wire, _ = links[i].send(t, nb)
                    down_bytes += wire
                    heapq.heappush(events, (arr, eseq, "global", (i, g, nb)))
                    eseq += 1

        # 4. evaluate the server's current model on recent points
        if t + 1e-9 >= next_eval:
            next_eval += cfg.eval_period
            lo = int(np.searchsorted(all_t, t - cfg.eval_window, side="right"))
            hi = int(np.searchsorted(all_t, t, side="right"))
            if coord.model is not None and hi > lo:
                w = all_idx[lo:hi]
                if len(w) > cfg.eval_max:
                    w = w[np.linspace(0, len(w) - 1, cfg.eval_max).astype(int)]
                r = evaluate(ds.X[w], ds.y[w], coord.model.centers, focus=focus or minority)
                r["t"] = t
                r["bytes"] = sum(nd.bytes_up for nd in nodes)
                r["node_bytes"] = [nd.bytes_up for nd in nodes]
                series.append(r)
            if probe is not None and coord.model is not None:
                probe(t, nodes, coord)
            if cfg.method == "fedcast":
                lam_trace.append((t, [nd.policy.lam for nd in nodes]))

    start = cfg.warmup * cfg.duration
    ser = [r for r in series if r["t"] >= start]

    def avg(key):
        vals = [r[key] for r in ser if key in r and not np.isnan(r[key])]
        return float(np.mean(vals)) if vals else float("nan")

    up = np.array([nd.bytes_up for nd in nodes], float)
    budget_total = cfg.param * cfg.duration if cfg.method == "fedcast" else float("nan")
    return {
        "config": {**asdict(cfg), "mc": asdict(cfg.mc)},
        "dataset": ds.name,
        "n_points": int(len(ds.y)),
        "k": k,
        "dim": d,
        "focus_classes": focus or minority,
        "ari": avg("ari"), "nmi": avg("nmi"), "purity": avg("purity"), "ssq": avg("ssq"),
        "macro_recall": avg("macro_recall"), "focus_recall": avg("focus_recall"),
        "bytes_up": float(up.sum()),
        "bytes_up_per_node": up.tolist(),
        "wire_up": float(sum(nd.wire_up for nd in nodes)),
        "bytes_down": float(down_bytes),
        "msgs_up": int(sum(nd.msgs_up for nd in nodes)),
        "budget_per_node": budget_total,
        "max_node_over_budget": float(up.max() / budget_total) if cfg.method == "fedcast" else float("nan"),
        "series": series,
        "lam_trace": lam_trace,
        "wall_seconds": time.time() - t0,
    }

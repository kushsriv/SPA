"""Real Apache Kafka runtime: edge process and coordinator process.

Topics (see docker/docker-compose.yml, scripts/kafka_native.sh):
  fsc.summaries  key=node-<i>   summary deltas (per-node order via the key)
  fsc.snapshots  key=node-<i>   coordinator changelog, log-compacted: latest
                                server-side CF set per node + the summaries
                                offset it reflects (headers)
  fsc.global     key=model      latest global model, log-compacted
  fsc.raw        key=node-<i>   raw points (Centralised-Raw baseline only)
  fsc.metrics                   JSON telemetry

Exactly-once *state*: producers are idempotent (no duplicates from retries),
every summary carries a per-node sequence number and the coordinator ignores
seq <= last applied, so replaying the log after a crash cannot double-apply.
"""
from __future__ import annotations

import json
import os
import resource
import signal
import time
from dataclasses import dataclass, field

import numpy as np
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from . import codec
from .coordinator import CentralCoordinator, Coordinator
from .data import load
from .metrics import evaluate
from .microcluster import MCParams
from .node import EdgeNode
from .partition import partition
from .sim import SimConfig, make_policy

T_SUM, T_SNAP, T_GLOBAL, T_RAW, T_METRICS = "fsc.summaries", "fsc.snapshots", "fsc.global", "fsc.raw", "fsc.metrics"


def rss_mb() -> float:
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _producer(bootstrap: str, stats: dict | None = None, **extra) -> Producer:
    conf = {"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5,
            "queue.buffering.max.kbytes": 4096, "acks": "all", **extra}
    if stats is not None:
        conf["statistics.interval.ms"] = 1000

        def cb(js):
            d = json.loads(js)
            stats["txmsg_bytes"] = d.get("txmsg_bytes", 0)
            stats["tx_bytes"] = d.get("tx_bytes", 0)
            stats["txmsgs"] = d.get("txmsgs", 0)
        conf["stats_cb"] = cb
    return Producer(conf)


# =================================================================== edges
@dataclass
class EdgeRunConfig:
    bootstrap: str = "localhost:9092"
    dataset: str = "syndrift"
    max_points: int = 60000
    speed: float = 10.0            # stream seconds per wall second
    eval_period: float = 10.0
    eval_window: float = 60.0
    out: str = "results/kafka_edges.json"
    sim: SimConfig = field(default_factory=SimConfig)
    node_ids: list[int] | None = None   # run only these nodes (one container per node)
    start_at: float = 0.0               # shared wall-clock start (epoch s) across containers


def run_edges(rc: EdgeRunConfig) -> dict:
    cfg = rc.sim
    ds = load(rc.dataset, seed=cfg.seed, max_points=rc.max_points)
    streams = partition(ds.y, cfg.nodes, cfg.partition, cfg.duration, cfg.seed, ds.u, ds.groups)
    d = ds.X.shape[1]
    raw_mode = cfg.method == "raw"
    nodes = [EdgeNode(i, d, cfg.mc, make_policy(cfg, max(1, len(np.unique(ds.y[s.idx])))))
             for i, s in enumerate(streams)]
    for nd in nodes:
        nd.quant = cfg.quant
    active = list(rc.node_ids) if rc.node_ids is not None else list(range(len(nodes)))
    stats = {i: dict() for i in active}
    producers = {i: _producer(rc.bootstrap, stats[i], **({"linger.ms": 20} if raw_mode else {}))
                 for i in active}
    latencies: list[float] = []
    errors = [0]

    def on_delivery_factory(i, sent_wall, payload):
        def cb(err, msg):
            if err is not None:
                errors[0] += 1
                return
            lat = time.time() - sent_wall
            latencies.append(lat)
            nodes[i].on_delivery(payload, payload, lat * rc.speed)
        return cb

    gcons = Consumer({"bootstrap.servers": rc.bootstrap, "group.id": f"edges-{os.getpid()}",
                      "auto.offset.reset": "latest", "enable.auto.commit": False})
    gcons.assign([TopicPartition(T_GLOBAL, 0, -1)])  # -1 = OFFSET_END: only new models
    kfed_rng = np.random.default_rng(cfg.seed + 7)
    all_idx = np.concatenate([s.idx for s in streams])
    all_t = np.concatenate([s.times for s in streams])
    o = np.argsort(all_t, kind="stable")
    all_idx, all_t = all_idx[o], all_t[o]
    pos = [0] * len(nodes)
    series = []
    next_eval = rc.eval_period
    latest_global = None
    global_recv_lat = []
    if rc.start_at > 0:
        time.sleep(max(0.0, rc.start_at - time.time()))
        start = rc.start_at
    else:
        start = time.time()
    t = 0.0
    while t < cfg.duration - 1e-9:
        t = round(t + cfg.tick, 9)
        wait = start + t / rc.speed - time.time()
        if wait > 0:
            time.sleep(wait)
        for msg in gcons.consume(100, 0):
            if msg.error():
                continue
            g = codec.decode_global(msg.value())
            nb = len(msg.value()) + codec.KAFKA_RECORD_OVERHEAD
            latest_global = g
            for i in active:
                nodes[i].on_global(g, nb)
            ts = msg.timestamp()[1] / 1000.0
            global_recv_lat.append(time.time() - ts)
        for i in active:
            nd = nodes[i]
            s = streams[i]
            j0, j1 = pos[i], int(np.searchsorted(s.times, t, side="right"))
            pos[i] = j1
            if raw_mode:
                if j1 > j0:
                    X = ds.X[s.idx[j0:j1]]
                    for row in range(0, len(X), 64):  # up to 64 points per record
                        chunk = X[row:row + 64]
                        buf = codec.encode_raw(i, nd.seq, t, chunk)
                        nd.seq += 1
                        payload = len(buf) + codec.KAFKA_RECORD_OVERHEAD
                        nd.bytes_up += payload
                        nd.msgs_up += 1
                        producers[i].produce(T_RAW, value=buf, key=f"node-{i}".encode(),
                                             on_delivery=on_delivery_factory(i, time.time(), payload))
                producers[i].poll(0)
                continue
            for j in range(j0, j1):
                nd.ingest(ds.X[s.idx[j]], float(s.times[j]))
            if int(t) % 10 == 0:
                nd.mc.prune(t)
            nd.policy.tick(t)
            dec = nd.policy.decide(nd, t)
            if dec is not None:
                summ = (nd.build_kfed(t, nd.policy.k_local, kfed_rng) if dec.kfed
                        else nd.build(t, full=dec.full, only_slots=dec.only_slots))
                if cfg.quant:
                    buf = codec.encode_summary_q(summ)
                    summ = codec.decode_summary(buf)   # error feedback
                else:
                    buf = codec.encode_summary(summ)
                payload = len(buf) + codec.KAFKA_RECORD_OVERHEAD
                nd.commit(summ, payload)
                while True:
                    try:
                        producers[i].produce(T_SUM, value=buf, key=f"node-{i}".encode(),
                                             on_delivery=on_delivery_factory(i, time.time(), payload))
                        break
                    except BufferError:
                        producers[i].poll(0.01)
            producers[i].poll(0)
        if t + 1e-9 >= next_eval and rc.node_ids is None:
            next_eval += rc.eval_period
            lo = int(np.searchsorted(all_t, t - rc.eval_window, side="right"))
            hi = int(np.searchsorted(all_t, t, side="right"))
            if latest_global is not None and hi > lo:
                w = all_idx[lo:hi][:: max(1, (hi - lo) // 3000)]
                r = evaluate(ds.X[w], ds.y[w], latest_global.centers)
                r.pop("recall", None)
                r.update(t=t, wall=time.time() - start, bytes=sum(n.bytes_up for n in nodes),
                         model_version=latest_global.version)
                series.append(r)
                print(f"[edges] t={t:6.0f}s ARI={r['ari']:.3f} bytes={r['bytes']/1e3:8.1f}kB "
                      f"model v{latest_global.version}", flush=True)
    for p in producers.values():
        p.flush(30)
    time.sleep(1.2)  # let the last statistics callbacks fire
    for p in producers.values():
        p.poll(0)
    gcons.close()
    lat = np.array(latencies) if latencies else np.zeros(1)
    res = {
        "method": cfg.method, "param": cfg.param, "dataset": rc.dataset, "nodes": cfg.nodes,
        "speed": rc.speed, "wall_seconds": time.time() - start,
        "node_ids": active,
        "bytes_up_estimate": float(sum(nodes[i].bytes_up for i in active)),
        "kafka_txmsg_bytes": float(sum(s.get("txmsg_bytes", 0) for s in stats.values())),
        "kafka_tx_bytes": float(sum(s.get("tx_bytes", 0) for s in stats.values())),
        "msgs_up": int(sum(nodes[i].msgs_up for i in active)),
        "delivery_errors": errors[0],
        "produce_latency_ms_p50": float(np.percentile(lat, 50) * 1000),
        "produce_latency_ms_p95": float(np.percentile(lat, 95) * 1000),
        "global_model_latency_ms_p50": float(np.percentile(global_recv_lat, 50) * 1000) if global_recv_lat else None,
        "ari": float(np.mean([r["ari"] for r in series[len(series) // 10:]])) if series else None,
        "edge_rss_mb": rss_mb(),
        "series": series,
    }
    os.makedirs(os.path.dirname(rc.out) or ".", exist_ok=True)
    with open(rc.out, "w") as f:
        json.dump(res, f, indent=1)
    return res


# =================================================================== coordinator
@dataclass
class CoordRunConfig:
    bootstrap: str = "localhost:9092"
    group: str = "fedcast-coordinator"
    k: int = 15
    dim: int = 10
    raw: bool = False
    agg_every: float = 0.5            # wall seconds between aggregations
    checkpoint_every: float = 2.0     # wall seconds between changelog snapshots
    half_life: float = 120.0
    idle_exit: float = 0.0            # exit after this many idle wall seconds (0: never)
    out: str = "results/kafka_coordinator.json"
    stop_file: str = ""
    history: str = ""                 # append every global model (JSON lines) for offline evaluation


class KafkaCoordinator:
    def __init__(self, rc: CoordRunConfig):
        self.rc = rc
        if rc.raw:
            self.core = CentralCoordinator(rc.k, rc.dim, MCParams(max_mc=500, half_life=rc.half_life))
        else:
            self.core = Coordinator(rc.k, rc.dim, rc.half_life)
        self.topic = T_RAW if rc.raw else T_SUM
        self.cons = Consumer({"bootstrap.servers": rc.bootstrap, "group.id": rc.group,
                              "enable.auto.commit": False, "auto.offset.reset": "earliest",
                              "fetch.wait.max.ms": 50})
        self.prod = _producer(rc.bootstrap)
        md = self.cons.list_topics(self.topic, timeout=10)
        self.parts = sorted(md.topics[self.topic].partitions)
        self.offsets: dict[int, int] = {}
        self.now = 0.0
        self.e2e: list[float] = []
        self.restored_nodes = 0
        self.replayed = 0

    # ---------------------------------------------------------- recovery
    def restore(self) -> float:
        """Rebuild state from the compacted changelog, then resume the summaries log."""
        t0 = time.time()
        snap_offsets: dict[int, int] = {}
        if not self.rc.raw:
            c = Consumer({"bootstrap.servers": self.rc.bootstrap, "group.id": self.rc.group + "-restore",
                          "enable.auto.commit": False, "auto.offset.reset": "earliest"})
            md = c.list_topics(T_SNAP, timeout=10)
            tps = []
            for p in md.topics[T_SNAP].partitions:
                lo, hi = c.get_watermark_offsets(TopicPartition(T_SNAP, p), timeout=10)
                if hi > lo:
                    tps.append((p, hi))
            if tps:
                c.assign([TopicPartition(T_SNAP, p, 0) for p, _ in tps])
                remaining = {p: hi for p, hi in tps}
                while remaining:
                    for msg in c.consume(500, 1.0):
                        if msg.error():
                            continue
                        if msg.value() is not None:
                            s = codec.decode_summary(msg.value())
                            self.core.nodes[s.node] = {int(i): (float(s.n[j]), s.LS[j], float(s.SS[j]), float(s.t[j]))
                                                       for j, i in enumerate(s.ids)}
                            h = dict(msg.headers() or [])
                            self.core.last_seq[s.node] = int(h.get("seq", b"-1"))
                            for part, off in json.loads(h.get("offsets", b"{}")).items():
                                snap_offsets[int(part)] = max(snap_offsets.get(int(part), 0), int(off))
                            self.now = max(self.now, s.time)
                        if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
                            remaining.pop(msg.partition(), None)
            c.close()
            self.restored_nodes = len(self.core.nodes)
        # resume: committed group offsets, else the changelog's offsets, else the beginning
        committed = None
        for attempt in range(30):   # a fresh broker may not have its group coordinator ready yet
            try:
                committed = self.cons.committed([TopicPartition(self.topic, p) for p in self.parts], timeout=10)
                break
            except KafkaException:
                time.sleep(min(1.0 + attempt, 5.0))
        if committed is None:
            raise RuntimeError("group coordinator not available")
        assign = []
        for tp in committed:
            off = snap_offsets.get(tp.partition, tp.offset if tp.offset >= 0 else 0)
            if self.rc.raw:
                off = tp.offset if tp.offset >= 0 else 0
            assign.append(TopicPartition(self.topic, tp.partition, off))
            self.offsets[tp.partition] = off
        self.cons.assign(assign)
        return time.time() - t0

    def checkpoint(self) -> None:
        if self.rc.raw:
            return
        offs = json.dumps(self.offsets).encode()
        for node, st in self.core.nodes.items():
            ids = np.fromiter(st.keys(), np.int64, len(st))
            vals = list(st.values())
            s = codec.Summary(codec.KIND_FULL, node, self.core.last_seq.get(node, -1), self.now, ids,
                              np.array([v[0] for v in vals]),
                              np.array([v[1] for v in vals]).reshape(len(vals), self.rc.dim),
                              np.array([v[2] for v in vals]), np.array([v[3] for v in vals]), np.zeros(0, np.int64))
            self.prod.produce(T_SNAP, key=f"node-{node}".encode(), value=codec.encode_summary(s),
                              headers=[("seq", str(self.core.last_seq.get(node, -1)).encode()), ("offsets", offs)])
        self.prod.flush(10)
        self.cons.commit(offsets=[TopicPartition(self.topic, p, o) for p, o in self.offsets.items()],
                         asynchronous=False)

    def lag(self) -> int:
        total = 0
        for p in self.parts:
            _, hi = self.cons.get_watermark_offsets(TopicPartition(self.topic, p), timeout=5, cached=False)
            total += max(0, hi - self.offsets.get(p, 0))
        return total

    # ---------------------------------------------------------- main loop
    def serve(self) -> dict:
        rec = self.restore()
        print(f"[coord] restored {self.restored_nodes} nodes from changelog in {rec*1000:.0f} ms; "
              f"resuming at {self.offsets}", flush=True)
        stop = [False]
        signal.signal(signal.SIGTERM, lambda *a: stop.__setitem__(0, True))
        last_agg = last_ck = last_msg = time.time()
        caught_up_at = None
        started = time.time()
        applied = 0
        while not stop[0]:
            if self.rc.stop_file and os.path.exists(self.rc.stop_file):
                break
            msgs = self.cons.consume(1000, 0.05)
            noww = time.time()
            for msg in msgs:
                if msg.error():
                    continue
                self.offsets[msg.partition()] = msg.offset() + 1
                if self.rc.raw:
                    node, seq, t, X = codec.decode_raw(msg.value())
                    self.core.apply_raw(X, np.full(len(X), t))
                    self.now = max(self.now, t)
                else:
                    s = codec.decode_summary(msg.value())
                    if self.core.apply(s):
                        applied += 1
                    self.now = max(self.now, s.time)
                self.e2e.append(noww - msg.timestamp()[1] / 1000.0)
            if msgs:
                last_msg = noww
            if caught_up_at is None and not msgs and noww - started > 0.5:
                caught_up_at = noww - started
            if noww - last_agg >= self.rc.agg_every:
                last_agg = noww
                g = self.core.aggregate(self.now)
                if g is not None and self.rc.history:
                    with open(self.rc.history, "a") as hf:
                        hf.write(json.dumps({"version": g.version, "t": g.time, "wall": noww,
                                             "centers": g.centers.tolist()}) + "\n")
                if g is not None:
                    self.prod.produce(T_GLOBAL, key=b"model", value=codec.encode_global(g))
                    self.prod.poll(0)
            if noww - last_ck >= self.rc.checkpoint_every:
                last_ck = noww
                self.checkpoint()
            if self.rc.idle_exit and self.e2e and noww - last_msg > self.rc.idle_exit:
                break
        self.checkpoint()
        e2e = np.array(self.e2e) if self.e2e else np.zeros(1)
        res = {"applied": applied, "duplicates_ignored": self.core.duplicates,
               "restored_nodes": self.restored_nodes, "restore_ms": rec * 1000,
               "catch_up_s": caught_up_at, "e2e_ms_p50": float(np.percentile(e2e, 50) * 1000),
               "e2e_ms_p95": float(np.percentile(e2e, 95) * 1000), "coord_rss_mb": rss_mb(),
               "model_version": self.core.version,
               "state": {str(n): sorted(st.keys()) for n, st in self.core.nodes.items()},
               "state_mass": {str(n): float(sum(v[0] for v in st.values())) for n, st in self.core.nodes.items()}}
        os.makedirs(os.path.dirname(self.rc.out) or ".", exist_ok=True)
        with open(self.rc.out, "w") as f:
            json.dump(res, f, indent=1)
        self.cons.close()
        return res


def run_coordinator(rc: CoordRunConfig) -> dict:
    return KafkaCoordinator(rc).serve()


def reset_topics(bootstrap: str) -> None:
    """Delete and recreate all fsc.* topics (clean experiment state)."""
    from confluent_kafka.admin import AdminClient, NewTopic
    a = AdminClient({"bootstrap.servers": bootstrap})
    names = [T_SUM, T_SNAP, T_GLOBAL, T_RAW, T_METRICS]
    existing = set(a.list_topics(timeout=10).topics)
    fs = a.delete_topics([n for n in names if n in existing])
    for f in fs.values():
        try:
            f.result()
        except KafkaException:
            pass
    time.sleep(2)
    specs = [NewTopic(T_SUM, 8, 1), NewTopic(T_RAW, 8, 1), NewTopic(T_METRICS, 1, 1),
             NewTopic(T_SNAP, 8, 1, config={"cleanup.policy": "compact"}),
             NewTopic(T_GLOBAL, 1, 1, config={"cleanup.policy": "compact"})]
    for _ in range(10):
        fs = a.create_topics(specs)
        ok = True
        for f in fs.values():
            try:
                f.result()
            except KafkaException as e:
                if "already exists" not in str(e) and "TOPIC_ALREADY_EXISTS" not in str(e):
                    ok = False
        if ok:
            return
        time.sleep(1)

"""Multi-container deployment: every edge node in its own container (own network stack,
real TCP), a real Kafka broker and a coordinator, with per-node kernel bandwidth caps.

  python experiments/deploy_containers.py            # runs the D1 suite, results/deploy/*.json

Each edge container gets a Linux `tc tbf` rate limit on its interface (applied from the
host inside the container's network namespace). The sandbox kernel has no `netem`
module, so delay and loss are not injected here; they are covered by the in-app emulator.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fedcast.data import load  # noqa: E402
from fedcast.macro import sqdist  # noqa: E402
from fedcast.partition import partition  # noqa: E402
from fedcast.sim import SimConfig, run  # noqa: E402

NET = "fsc-net"
IMG = "fedcast:latest"
OUT = ROOT / "results" / "deploy"
RATES = {"lan": "100mbit", "wan": "5mbit", "cellular": "1mbit", "poor": "128kbit"}
LINKS = ["lan", "wan", "cellular", "poor"]


def sh(cmd: str, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stderr}")
    return (r.stdout or "").strip()


def cleanup():
    sh("docker ps -aq --filter label=fsc | xargs -r docker rm -f", check=False)


def start_kafka():
    sh(f"docker network inspect {NET} >/dev/null 2>&1 || docker network create {NET}")
    env = " ".join(f"-e {k}={v}" for k, v in {
        "KAFKA_NODE_ID": 1, "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1, "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 1,
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1, "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
        "KAFKA_HEAP_OPTS": '"-Xmx256M -Xms256M"'}.items())
    sh(f"docker run -d --label fsc --name kafka --network {NET} --memory 512m {env} apache/kafka:3.8.0")
    for _ in range(90):
        if subprocess.run("docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh "
                          "--bootstrap-server localhost:9092", shell=True, capture_output=True).returncode == 0:
            break
        time.sleep(1)
    K = "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists"
    for t, p, extra in [("fsc.summaries", 8, ""), ("fsc.raw", 8, ""), ("fsc.metrics", 1, ""),
                        ("fsc.snapshots", 8, "--config cleanup.policy=compact"),
                        ("fsc.global", 1, "--config cleanup.policy=compact")]:
        sh(f"{K} --topic {t} --partitions {p} --replication-factor 1 {extra}")


def shape(container: str, link: str):
    pid = sh(f"docker inspect -f '{{{{.State.Pid}}}}' {container}")
    sh(f"nsenter -t {pid} -n tc qdisc replace dev eth0 root tbf rate {RATES[link]} burst 32kbit latency 400ms")


def mem_mb(container: str) -> float:
    out = sh(f"docker stats --no-stream --format '{{{{.MemUsage}}}}' {container}", check=False)
    try:
        v, unit = out.split("/")[0].strip()[:-3], out.split("/")[0].strip()[-3:]
        return float(v) * (1024 if unit == "GiB" else 1)
    except Exception:
        return float("nan")


def deploy(run_id: str, dataset: str, partition_spec: str, method: str, param: float, extra: list[str],
           nodes: int = 10, duration: float = 300.0, speed: float = 5.0, seed: int = 0) -> dict:
    cleanup()
    start_kafka()
    rdir = OUT / run_id
    rdir.mkdir(parents=True, exist_ok=True)
    for f in rdir.glob("*"):
        f.unlink()
    vol = f"-v {rdir}:/res"
    raw = method == "raw"
    # the coordinator must outlast the longest silence between sends (periodic baselines)
    idle = max(15.0, 2.5 * (param if method in ("periodic", "pdelta", "kfed") else 10.0) / speed)
    sh(f"docker run -d --label fsc --name coord --network {NET} {vol} {IMG} coordinator --dataset {dataset} "
       f"--bootstrap kafka:9092 --idle-exit {idle:.0f} --out /res/coord.json --history /res/history.jsonl"
       + (" --raw" if raw else ""))
    time.sleep(4)
    t0 = time.time() + 25 + nodes * 1.5
    common = (f"--dataset {dataset} --method {method} --param {param} --nodes {nodes} --duration {duration} "
              f"--partition {partition_spec} --seed {seed} --speed {speed} --bootstrap kafka:9092 --start-at {t0} "
              + " ".join(extra))
    for i in range(nodes):
        sh(f"docker run -d --label fsc --name edge{i} --network {NET} --cap-add NET_ADMIN {vol} {IMG} "
           f"edges {common} --node-ids {i} --out /res/edge_{i}.json")
    for i in range(nodes):
        shape(f"edge{i}", LINKS[i % len(LINKS)])
    time.sleep(max(0.0, t0 - time.time()) + duration / speed * 0.5)
    mems = {c: mem_mb(c) for c in ["kafka", "coord"] + [f"edge{i}" for i in range(nodes)]}
    sh("docker wait " + " ".join(f"edge{i}" for i in range(nodes)))
    sh("docker wait coord")
    edges = [json.loads((rdir / f"edge_{i}.json").read_text()) for i in range(nodes)]
    coord = json.loads((rdir / "coord.json").read_text())
    res = {"run_id": run_id, "dataset": dataset, "partition": partition_spec, "method": method, "param": param,
           "extra": extra, "nodes": nodes, "speed": speed,
           "bytes_up_estimate": sum(e["bytes_up_estimate"] for e in edges),
           "kafka_txmsg_bytes": sum(e["kafka_txmsg_bytes"] for e in edges),
           "kafka_tx_bytes": sum(e["kafka_tx_bytes"] for e in edges),
           "msgs_up": sum(e["msgs_up"] for e in edges),
           "delivery_errors": sum(e["delivery_errors"] for e in edges),
           "bytes_by_link": {l: sum(e["bytes_up_estimate"] for i, e in enumerate(edges) if LINKS[i % 4] == l)
                             for l in LINKS},
           "coordinator": {k: coord[k] for k in ("applied", "e2e_ms_p50", "e2e_ms_p95", "model_version")},
           "memory_mb": mems}
    res["quality"] = offline_quality(rdir / "history.jsonl", dataset, partition_spec, nodes, duration, seed)
    cleanup()
    (OUT / f"{run_id}.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({k: res[k] for k in ("run_id", "bytes_up_estimate", "kafka_txmsg_bytes", "msgs_up",
                                          "delivery_errors", "coordinator", "quality")}), flush=True)
    return res


def offline_quality(history: Path, dataset: str, spec: str, nodes: int, duration: float, seed: int,
                    window: float = 60.0, period: float = 10.0) -> dict:
    """Evaluate the logged global models on the data windows they served, relative to the
    simulated Centralised-Raw learner on the same stream (same seed and partition)."""
    ds = load(dataset, max_points=60000)
    st = partition(ds.y, nodes, spec, duration, seed, ds.u, ds.groups)
    idx = np.concatenate([s.idx for s in st])
    tt = np.concatenate([s.times for s in st])
    o = np.argsort(tt, kind="stable")
    idx, tt = idx[o], tt[o]
    hist = [json.loads(l) for l in history.read_text().splitlines()] if history.exists() else []
    if not hist:
        return {"cost_ratio": None}
    ref = run(SimConfig(method="raw", nodes=nodes, duration=duration, partition=spec, seed=seed,
                        eval_period=period, eval_window=window), ds)
    ref_ssq = {round(e["t"]): e["ssq"] for e in ref["series"]}
    ratios = []
    for tev in np.arange(period, duration + 1e-9, period):
        cur = [h for h in hist if h["t"] <= tev]
        if not cur or tev < 0.1 * duration or round(tev) not in ref_ssq:
            continue
        C = np.array(cur[-1]["centers"])
        lo, hi = np.searchsorted(tt, tev - window, side="right"), np.searchsorted(tt, tev, side="right")
        w = idx[lo:hi]
        if len(w) > 3000:
            w = w[np.linspace(0, len(w) - 1, 3000).astype(int)]
        if len(w):
            ssq = float(sqdist(ds.X[w], C).min(1).mean())
            ratios.append(ssq / ref_ssq[round(tev)])
    return {"cost_ratio": float(np.mean(ratios)) if ratios else None, "n_evals": len(ratios)}


SUITE = [
    ("syndrift_fedcast3", "syndrift", "dirichlet:0.3", "fedcast", 10, ["--rank", "lloyd+", "--quant", "--overflow"]),
    ("syndrift_topkq", "syndrift", "dirichlet:0.3", "fedcast", 10, ["--rank", "norm", "--quant"]),
    ("syndrift_pdeltaq", "syndrift", "dirichlet:0.3", "pdelta", 120, ["--quant"]),
    ("intel_fedcast3", "intel", "natural", "fedcast", 10, ["--rank", "lloyd+", "--quant", "--overflow"]),
    ("intel_topkq", "intel", "natural", "fedcast", 10, ["--rank", "norm", "--quant"]),
    ("intel_pdeltaq", "intel", "natural", "pdelta", 120, ["--quant"]),
]

if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    names = sys.argv[1:]
    for run_id, ds, part, m, p, extra in SUITE:
        if names and run_id not in names:
            continue
        deploy(run_id, ds, part, m, p, extra)

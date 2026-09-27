"""Systems experiments on a real Kafka broker (RQ4, RQ5).

  K1  byte accounting: simulator estimate vs Kafka producer statistics
  K2  coordinator crash (SIGKILL) + restart vs a shadow coordinator that never crashes
  K3  broker restart in the middle of a run (no summary may be lost)
  K4  scalability: 5..50 nodes, latency / throughput / memory

Requires a running broker (scripts/kafka_native.sh start). Results: results/kafka_*.json
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fedcast import kafka_runtime as kr  # noqa: E402
from fedcast.data import load  # noqa: E402
from fedcast.sim import SimConfig  # noqa: E402

B = "localhost:9092"
OUT = ROOT / "results"
PY = sys.executable


def coord_proc(group: str, out: str, dataset="syndrift", raw=False, idle=6.0):
    cmd = [PY, "-m", "fedcast.cli", "coordinator", "--dataset", dataset, "--group", group,
           "--idle-exit", str(idle), "--out", out] + (["--raw"] if raw else [])
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def edges(method, param, nodes=10, duration=300.0, speed=10.0, dataset="syndrift", out="results/k_edges.json"):
    cfg = SimConfig(method=method, param=param, nodes=nodes, duration=duration, partition="dirichlet:0.3")
    return kr.run_edges(kr.EdgeRunConfig(bootstrap=B, dataset=dataset, speed=speed, out=out, sim=cfg))


def kafka_rss_mb() -> float:
    try:
        pid = subprocess.check_output(["pgrep", "-f", "kafka.Kafka"], text=True).split()[0]
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        return float("nan")
    return float("nan")


def k1():
    rows = []
    for method, param in [("fedcast", 20), ("fedcast", 150), ("periodic", 30), ("pdelta", 30), ("kfed", 30), ("raw", 0)]:
        kr.reset_topics(B)
        c = coord_proc("k1", str(OUT / "k1_coord.json"), raw=method == "raw")
        time.sleep(3)
        e = edges(method, param, duration=200, speed=20, out=str(OUT / "k1_edges.json"))
        c.wait(60)
        rows.append({k: e[k] for k in ("method", "param", "bytes_up_estimate", "kafka_txmsg_bytes",
                                       "kafka_tx_bytes", "msgs_up", "ari", "delivery_errors")})
        print(rows[-1], flush=True)
    json.dump(rows, open(OUT / "kafka_K1_bytes.json", "w"), indent=1)


def k2():
    kr.reset_topics(B)
    shadow = coord_proc("shadow", str(OUT / "k2_shadow.json"), idle=8.0)
    victim = coord_proc("victim", str(OUT / "k2_victim_run1.json"))
    time.sleep(3)
    import threading
    res = {}
    th = threading.Thread(target=lambda: res.update(edges("fedcast", 50, duration=400, speed=10,
                                                          out=str(OUT / "k2_edges.json"))))
    th.start()
    time.sleep(15)
    victim.kill()  # SIGKILL: no clean shutdown, no final checkpoint
    killed_at = time.time()
    time.sleep(8)
    victim2 = coord_proc("victim", str(OUT / "k2_victim_run2.json"), idle=8.0)
    restarted_at = time.time()
    th.join()
    victim2.wait(120)
    shadow.wait(120)
    log2 = victim2.stdout.read()
    a = json.load(open(OUT / "k2_victim_run2.json"))
    s = json.load(open(OUT / "k2_shadow.json"))
    same_ids = a["state"] == s["state"]
    mass_err = max(abs(a["state_mass"][n] - s["state_mass"].get(n, 0)) for n in a["state_mass"])
    out = {"killed_after_s": 15, "down_for_s": restarted_at - killed_at,
           "restore_ms": a["restore_ms"], "restored_nodes": a["restored_nodes"],
           "duplicates_ignored_after_restart": a["duplicates_ignored"],
           "state_identical_to_shadow": same_ids, "max_node_mass_difference": mass_err,
           "edges_msgs": res.get("msgs_up"), "shadow_applied": s["applied"], "log": log2[-600:]}
    print(out, flush=True)
    json.dump(out, open(OUT / "kafka_K2_crash.json", "w"), indent=1)


def k3():
    kr.reset_topics(B)
    c = coord_proc("k3", str(OUT / "k3_coord.json"), idle=15.0)
    time.sleep(3)
    import threading
    res = {}
    th = threading.Thread(target=lambda: res.update(edges("fedcast", 50, duration=400, speed=10,
                                                          out=str(OUT / "k3_edges.json"))))
    th.start()
    time.sleep(12)
    subprocess.run([str(ROOT / "scripts/kafka_native.sh"), "stop"], capture_output=True)
    t_stop = time.time()
    time.sleep(6)
    subprocess.run([str(ROOT / "scripts/kafka_native.sh"), "start"], capture_output=True)
    t_up = time.time()
    th.join()
    c.wait(180)
    co = json.load(open(OUT / "k3_coord.json"))
    out = {"broker_down_s": t_up - t_stop, "edge_msgs_sent": res.get("msgs_up"),
           "delivery_errors": res.get("delivery_errors"), "coordinator_applied": co["applied"],
           "lost": (res.get("msgs_up") or 0) - co["applied"], "ari": res.get("ari")}
    print(out, flush=True)
    json.dump(out, open(OUT / "kafka_K3_broker_restart.json", "w"), indent=1)


def k4():
    rows = []
    for nodes in (5, 10, 20, 50):
        for method, param in [("fedcast", 20), ("raw", 0)]:
            kr.reset_topics(B)
            c = coord_proc("k4", str(OUT / "k4_coord.json"), raw=method == "raw")
            time.sleep(3)
            e = edges(method, param, nodes=nodes, duration=200, speed=10, out=str(OUT / "k4_edges.json"))
            broker = kafka_rss_mb()
            c.wait(120)
            co = json.load(open(OUT / "k4_coord.json"))
            pts = 60000 * 200 / 300
            rows.append({"nodes": nodes, "method": method, "points_per_wall_s": pts / e["wall_seconds"],
                         "bytes_up": e["bytes_up_estimate"], "msgs": e["msgs_up"],
                         "produce_p50_ms": e["produce_latency_ms_p50"], "produce_p95_ms": e["produce_latency_ms_p95"],
                         "e2e_p50_ms": co["e2e_ms_p50"], "e2e_p95_ms": co["e2e_ms_p95"],
                         "edge_rss_mb": e["edge_rss_mb"], "coord_rss_mb": co["coord_rss_mb"],
                         "broker_rss_mb": broker, "ari": e["ari"]})
            print(rows[-1], flush=True)
    json.dump(rows, open(OUT / "kafka_K4_scale.json", "w"), indent=1)


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for name in sys.argv[1:] or ["k1", "k2", "k3", "k4"]:
        globals()[name]()

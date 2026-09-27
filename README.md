# FedCAST: Budget-Aware Federated Stream Clustering over Apache Kafka

*Stream Processing Analytics project.* Non-IID edge streams, network constraints, a hard byte budget, and Apache Kafka as the backbone.

Edge nodes summarise their local streams as time-decayed micro-clusters. Each node decides **when an update is worth its network cost** and **which parts to send**, using a staleness score that provably bounds the server's clustering error. A budget controller guarantees that no node exceeds its byte budget. A Kafka-backed coordinator rebuilds the global clustering, survives crashes with exactly-once state, and broadcasts the model back.

```
edge nodes ──(prioritised CF deltas, only when worth it)──► Kafka fsc.summaries ──► coordinator
    ▲                                                                                 │
    └────────────── fsc.global (compacted: latest global model) ◄─────────────────────┤
                    fsc.snapshots (compacted changelog: crash recovery) ◄──────────────┘
```

| | |
|---|---|
| 📄 Paper (PDF + LaTeX) | [`paper/`](paper/) |
| 🧠 Why this stack / these features / the alternatives | [`docs/03_design_rationale.md`](docs/03_design_rationale.md) |
| 📘 Project summary with flowcharts | [`docs/Project_Summary.pdf`](docs/Project_Summary.pdf) |
| 🔬 Research proposal, roadmap | [`docs/00_research_proposal.md`](docs/00_research_proposal.md), [`docs/01_roadmap.md`](docs/01_roadmap.md) |
| 💻 Low-memory laptop setup (under 1 GB RAM) | [`docs/02_low_memory_setup.md`](docs/02_low_memory_setup.md) |
| 📊 Results tables | [`results/tables/`](results/tables/) |

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .                       # installs the `fedcast` command

# 1) Kafka: no Docker (macOS / Windows-WSL2 / Linux, needs Java 17+) ...
scripts/kafka_native.sh start
#    ... or Docker (Linux)
docker compose -f docker/docker-compose.yml up -d

# 2) Live demo on real Kafka: coordinator + 10 edge nodes, FedCAST with 20 B/s per node
fedcast demo --dataset syndrift --method fedcast --param 20 --duration 300 --speed 10

# 3) One simulated run (no Kafka needed), e.g. the Periodic baseline
fedcast sim --dataset pendigits --method periodic --param 60
```

Run the processes separately (for example on different machines):
```bash
fedcast reset-topics
fedcast coordinator --dataset syndrift
fedcast edges --dataset syndrift --method fedcast --param 20 --speed 10
```

Methods: `fedcast` (param = bytes/s per node), `periodic` / `pdelta` / `kfed` (param = period in s), `change` (ε), `norm` (relative δ), `naive`, `raw`.
Datasets: `syndrift`, `nslkdd`, `shuttle`, `pendigits`, `letter` (downloaded automatically from GitHub mirrors into `data/raw/`).

## Reproduce the paper

```bash
python -m pytest                                   # unit tests (incl. numerical checks of the propositions)
python experiments/run.py E1 E2 E3 E4 E5 E6 E7     # about 2,000 seeded simulation runs, resumable
python experiments/kafka_experiments.py            # real-Kafka systems experiments K1-K4 (needs a broker)
python experiments/analyze.py                      # figures -> paper/figures, tables -> results/tables
cd paper && latexmk -pdf main.tex                  # the paper
```

| Experiment | Question |
|---|---|
| E1 | Cost-quality Pareto fronts, budget-matched comparison, Friedman/Nemenyi + Wilcoxon (RQ1) |
| E2 | Degree of non-IID: IID → Dirichlet → cluster-exclusive nodes (RQ2) |
| E3 | Drift: emerging clusters and class-rotation drift, detection delay (RQ3) |
| E4 | Heterogeneous links, outages, link-price awareness (RQ4) |
| E5 / E6 | Ablations / sensitivity |
| E7, K4 | Scalability (simulated / real Kafka) (RQ5) |
| K1–K3 | Byte accounting vs Kafka stats, coordinator SIGKILL recovery, broker restart |

## Code layout
```
src/fedcast/
  microcluster.py   time-decayed CF micro-clusters (CluStream/DenStream family), exact NN cache
  codec.py          fixed binary wire format (exact sizes known before encoding)
  node.py           edge node: local model + exact copy of the server view
  policies.py       FedCAST (staleness, prioritised deltas, dual ascent, token bucket) + baselines
  macro.py          CF cost, weighted k-means++ / Lloyd, swap local search
  coordinator.py    exactly-once delta application, staleness decay, aggregation
  netem.py          in-app network emulator (delay, jitter, loss + retransmit, bandwidth, outages)
  sim.py            deterministic discrete-event simulator (same classes as the Kafka runtime)
  kafka_runtime.py  real Kafka edge process and coordinator with changelog recovery
  data.py, partition.py, metrics.py, cli.py
experiments/        run.py (E1-E7), kafka_experiments.py (K1-K4), analyze.py
paper/              LaTeX source, figures, compiled PDF
docker/, scripts/   Kafka (Docker or native, under 1 GB RAM)
```

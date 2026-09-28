# FedCAST: Budget-Aware Federated Stream Clustering over Apache Kafka

*Stream Processing Analytics project.* Non-IID edge streams, network constraints, a hard byte budget, and Apache Kafka as the backbone.

Edge nodes summarise their local streams as time-decayed micro-clusters. Each node decides **when an update is worth its network cost** and **which parts to send**, using a staleness score that provably bounds the server's clustering error. A budget controller guarantees that no node exceeds its byte budget. A Kafka-backed coordinator rebuilds the global clustering, survives crashes with exactly-once state, and broadcasts the model back.

## Headline results (FedCAST-v3)

The main finding: **the value of an update has an exact closed form.** For a server centre ĉ, the staleness excess cost is ‖ΔLS − ΔN·ĉ‖²/N, so each micro-cluster's delta can be ranked by the exact error it removes rather than by its magnitude.

| | FedCAST-v3 vs top-k magnitude (both 8-bit, error feedback, same budget) |
|---|---|
| Non-stationary streams (drift / evolving / natural) | FedCAST-v3 better in **72%** of 200 paired runs, p = 3.9×10⁻¹¹ |
| Static streams | a tie (35% of pairs, p = 0.97): FedCAST-v3 is no worse |
| Friedman ranks, 31 budget-matched settings | FedCAST-v3 1.68, top-k 1.61, Periodic-Δ 3.23, k-FED 3.48 (p = 6.8×10⁻¹²) |
| Multi-container deployment, Intel Lab | cost ratio 1.091 at 35 kB, vs 1.188 for top-k and 2.547 for Periodic-Δ |
| Kafka | 1.08 s crash recovery, 0 of 231 summaries lost while the broker was down 34 s, 33–35 ms p50 latency at 5–50 nodes, 788 MB total RAM |

Where FedCAST-v3 does **not** win:
- On static streams, top-k magnitude is equally good.
- At the very-low-byte end on Gas, CoverType and static Pen-Digits, k-FED is cheaper.
- Negative results are reported in the paper (§VII-F) and in [`docs/04_novelty_and_critique.md`](docs/04_novelty_and_critique.md).

```
edge nodes ──(prioritised CF deltas, only when worth it)──► Kafka fsc.summaries ──► coordinator
    ▲                                                                                 │
    └────────────── fsc.global (compacted: latest global model) ◄─────────────────────┤
                    fsc.snapshots (compacted changelog: crash recovery) ◄──────────────┘
```

| | |
|---|---|
| 📄 **Paper (IEEE format, current)** | [`paper/ieee/main.pdf`](paper/ieee/main.pdf) · source [`paper/ieee/main.tex`](paper/ieee/main.tex) |
| 📄 Earlier v1 draft (Elsevier format, superseded) | [`paper/main.pdf`](paper/main.pdf) |
| 🧠 Why this stack / these features / the alternatives | [`docs/03_design_rationale.md`](docs/03_design_rationale.md) |
| 🔍 Brutally honest novelty assessment, rounds 1–2 (E9–E14) | [`docs/04_novelty_and_critique.md`](docs/04_novelty_and_critique.md) |
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

**Recommended configuration (FedCAST-v3):** `--method fedcast --rank lloyd+ --quant --overflow`, i.e. exact-value ranking with a magnitude tie-break, 8-bit deltas with error feedback, and use-it-or-lose-it sends.

Methods: `fedcast` (param = bytes/s per node), `periodic` / `pdelta` / `kfed` (param = period in s), `change` (ε), `norm` (relative δ), `naive`, `raw`.
Datasets: `syndrift`, `nslkdd`, `shuttle`, `pendigits`, `letter`, and naturally drifting real streams `gas` (Gas Sensor Drift, 36 months), `covtype` (CoverType in original order) and `intel` (Intel Berkeley Lab, naturally partitioned by sensor mote; use `--partition natural`). All are downloaded automatically from GitHub mirrors into `data/raw/`.

## Reproduce the paper

```bash
python -m pytest                                   # unit tests (incl. numerical checks of the propositions)
python experiments/run.py E1 E2 E3 E4 E5 E6 E7 E8 E9 E10 E11 E12 E13 E14   # seeded runs, resumable
python experiments/kafka_experiments.py            # real-Kafka systems experiments K1-K4 (needs a broker)
python experiments/deploy_containers.py            # D1: one container per edge node, tc-tbf bandwidth caps (needs Docker + root)
python experiments/analyze.py                      # figures -> paper/figures, tables -> results/tables
cd paper/ieee && latexmk -pdf main.tex             # the IEEE paper
```

| Experiment | Question |
|---|---|
| E1 | Cost-quality Pareto fronts, budget-matched comparison, Friedman/Nemenyi + Wilcoxon (RQ1) |
| E2 | Degree of non-IID: IID → Dirichlet → cluster-exclusive nodes (RQ2) |
| E3 | Drift: emerging clusters and class-rotation drift, detection delay (RQ3) |
| E4 | Heterogeneous links, outages, link-price awareness (RQ4) |
| E5 / E6 | Ablations / sensitivity |
| E7, K4 | Scalability (simulated / real Kafka) (RQ5) |
| E8 | Real, naturally drifting streams (Gas, CoverType, Intel Lab) |
| E9–E11 | Critique round 1: stronger baselines (top-k with error feedback, event-triggered), robustness checks |
| E12 | Exact-value (Lloyd) ranking vs magnitude ranking, float32 |
| E13 | 8-bit quantized deltas, budget-adaptive resolution, multi-resolution (negative results included) |
| E14 | **FedCAST-v3** (lloyd+ ranking, 8-bit, use-it-or-lose-it) vs top-k 8-bit, Periodic-Δ, k-FED: paired Wilcoxon + CD diagram |
| K1–K3 | Byte accounting vs Kafka stats, coordinator SIGKILL recovery, broker restart |
| D1 | Multi-container deployment: 10 edge containers on LAN / WAN / cellular / poor links, results in `results/deploy/` |

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
experiments/        run.py (E1-E14), kafka_experiments.py (K1-K4), deploy_containers.py (D1), analyze.py
paper/ieee/         IEEE-format paper (main.tex, references.bib, main.pdf)
paper/              v1 Elsevier draft, shared figures (paper/figures)
docker/, scripts/   Kafka (Docker or native, under 1 GB RAM), Dockerfile.fedcast for container deployment
```

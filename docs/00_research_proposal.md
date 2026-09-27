# Budget-Aware Federated Stream Clustering over Apache Kafka under Non-IID Data and Network Constraints

**Phase 0 deliverable: literature positioning, problem statement, objectives, methodology, expected outcomes**

Working name of the method: **FedCAST** (Federated Cost-Aware STream clustering). The name is provisional. Check it for collisions before submission; an unrelated "FedCast" already exists in forecasting.

---

## 1. Executive summary

Edge devices such as IoT gateways, network probes and smart meters produce high-velocity, unlabeled streams. Each device sees a different part of the data distribution (non-IID) over constrained, unreliable links. We want one continuously updated **global clustering** of the whole network. Raw data must never leave a device, and the system must respect a **communication budget** per node.

The two-phase micro-cluster paradigm is well established, but existing distributed stream clustering systems (Cormode et al., 2007; Gama et al., 2011; Tran, 2013) have three limitations:

1. They send updates according to **fixed, objective-agnostic thresholds or periods**. There is no budget, no adaptation to link conditions and no guarantee that links bytes to clustering quality.
2. They **ignore statistical heterogeneity**. A node that is the only one to see a new cluster is treated like everyone else.
3. They are evaluated in simulation, **without a real messaging substrate**, so fault tolerance, replay and ordering are assumed rather than demonstrated.

Meanwhile, federated clustering (k-FED, FFCM, AFCL) targets static, batch data, and event-triggered federated learning targets gradient-based supervised models.

FedCAST closes this gap with four parts:

- an **objective-linked staleness score** computed from micro-cluster cluster-feature (CF) vectors
- a **Lagrangian (dual-ascent) send policy** that meets a per-node byte budget while adapting to the node's current link price
- **heterogeneity-aware, staleness-weighted macro-aggregation** that preserves minority and node-exclusive clusters
- a **Kafka-native protocol**: keyed delta log, compacted snapshot topic, global-model broadcast topic, and offset replay for recovery

We also give a bound relating the server's clustering-cost error to the trigger thresholds, and a long-run budget-satisfaction guarantee.

---

## 2. Literature review and gap analysis

### 2.1 Stream clustering: the local building block
- **CluStream** (Aggarwal et al., VLDB 2003) introduced online micro-clusters, a temporal extension of BIRCH's cluster features, plus offline macro-clustering.
- **DenStream** (Cao et al., SDM 2006) adds time-decayed CFs and density-based macro-clustering, so it finds arbitrarily shaped clusters and handles noise.
- **DBSTREAM** (Hahsler & Bolaños, TKDE 2016) adds a shared-density graph between micro-clusters.
- Surveys: Zubaroğlu & Atalay, *Data Stream Clustering: A Review* (AI Review 2021; arXiv 2007.10781).
- **Takeaway:** CF vectors `(n, LS, SS, t)` are additive and subtractive, and cheap to send. They are the natural unit of federated exchange.

### 2.2 Distributed and communication-efficient stream clustering: the closest prior work
| Work | Setting | What is sent | Trigger / cost control | Limitation vs. us |
|---|---|---|---|---|
| Cormode, Muthukrishnan & Zhuang, *Conquering the Divide*, ICDE 2007 | Distributed k-center over streams | Local centers | Approximation-guarantee-driven | k-center only; no non-IID treatment, no budget, no real system |
| Gama, Rodrigues & Lopes, **DGClust**, IDA 2011 (ECML-PKDD 2008) | Sensor networks | Grid-cell state changes | Send on state change | Grid discretisation (per-dimension, scales poorly); no budget |
| Tran, *Communication-Efficient Exact Clustering of Distributed Streaming Data*, ICCSA 2013 (arXiv 1209.4257) | Coordinator + remote sites, micro-clusters | Micro-clusters | Periodic / on change | No budget, no link awareness, IID-like evaluation |
| Balcan, Ehrlich & Liang, NeurIPS 2013 | Distributed k-means / median, static | Coresets | One-shot, provable | Not streaming |
| Micro-cluster stream clustering on Apache Storm (2016) | Parallel, single site | — | — | Parallelism, not federation |

### 2.3 Federated clustering on static data
- **k-FED** (Dennis, Li & Smith, ICML 2021): one-shot federated k-means. It shows heterogeneity *helps* when each device holds k' ≤ √k clusters. The theoretical hook we reuse: non-IID data can be an asset if aggregation is designed for it.
- **FFCM** (Stallmann & Wilbik, 2022): federated fuzzy c-means.
- **AFCL** (Zhang et al., AAAI 2025): asynchronous federated clustering with unknown k.
- Recent one-shot and hierarchical federated clustering (2025–2026 arXiv).
- **Gap:** all assume *static* local datasets, with no drift, no continuous model and no stream budget.

### 2.4 Federated learning on streams and drift
- **FedCluLearn** (Angelova et al., ECML-PKDD 2025) applies stream micro-cluster indexing to federated continual *supervised* learning.
- **Fielding** (clustered federated learning under drift, arXiv 2411.01580) runs drift-triggered re-clustering of *clients*, not data.
- **Federated anomaly detection over distributed streams** (Silva, Vinagre & Gama, 2022) and **DFAS** (Li et al., WSDM 2025) are anomaly-focused.
- **Gap:** clustering of the *data*, under an explicit communication budget, is not the target of any of these.

### 2.5 Event-triggered communication
- EventGraD (2021) and decentralized event-triggered federated learning with heterogeneous thresholds (Zehtabi et al., 2022) send when the parameter norm changes beyond a threshold, sometimes scaled by resources.
- **Gap:** these thresholds are on gradient/parameter norms. Nothing links the trigger to the *clustering objective*, uses the additive CF structure, or enforces a hard byte budget with a guarantee.

### 2.6 Streaming middleware for federated ML
- **Kafka-ML** (Martín et al., FGCS 2022) and **Federated Learning in Kafka-ML** (Internet of Things, 2024) show Kafka as a federated backbone, but for neural-network training.
- **Gap:** no work exploits Kafka's semantics (keyed partitions, log compaction, offset replay, idempotent producers) as part of the *algorithm's* recovery and consistency story for federated clustering.

### 2.7 Privacy (secondary axis)
- Differentially private clustering in streams (Epasto et al., 2023, arXiv 2307.07449) and in continual release (arXiv 2307.03430).
- CF vectors leak aggregate statistics. Gaussian-mechanism noise on `(n, LS, SS)` is a natural optional extension (§5.7).

### 2.8 Gap statement
> No existing method keeps a **continuously updated global clustering** from **non-IID edge streams** while **provably respecting per-node communication budgets** under **time-varying network conditions**, using a trigger **tied to the clustering objective**, and validated on a **real fault-tolerant messaging substrate**.

---

## 3. Problem statement

**Setting.** There are `m` edge nodes. Node `i` observes a stream `X_i = (x_{i,1}, x_{i,2}, …)`, `x ∈ ℝ^d`, drawn from a time-varying local distribution `P_i(t)`. The distributions are non-IID: `P_i ≠ P_j`, and some clusters may be exclusive to one node. The global distribution is the mixture `P(t) = Σ_i π_i(t) P_i(t)`.

Node `i` maintains a micro-cluster summary `S_i(t) = {CF_{i,1}, …, CF_{i,q}}`, where `CF = (n, LS, SS, t_last)` is time-decayed with factor `2^{-λ_d Δt}`. Node `i` communicates over a link with time-varying price `p_i(t)` (reflecting latency, loss or metered bytes) and has a byte budget `B_i` per window `W`.

The coordinator holds the *last received* summaries `Ŝ_i(t)` and produces global centers `C(t) = {c_1..c_k}`, or a density-based partition.

**Objective.** Minimise the time-averaged gap between the server-side clustering and the clustering that a centralised learner with all raw data would produce:

```
min_{send policy}  (1/T) Σ_t  [ J(C(t); P(t)) − J(C*(t); P(t)) ]
s.t.               (1/T) Σ_t  bytes_i(t) ≤ B_i / W        ∀ i        (budget)
                   raw data never leaves node i                    (federation)
```

Here `J` is the (weighted) k-means cost. For density-based variants we use a CMM or ARI proxy.

**Research questions.**
- **RQ1 (cost–accuracy):** How much communication can an objective-linked, budget-constrained trigger save relative to periodic, change-based and norm-based triggers, at equal clustering quality?
- **RQ2 (non-IID):** Does heterogeneity-aware aggregation preserve node-exclusive and minority clusters that FedAvg-style or naive weighted aggregation lose? How does quality vary with the skew (Dirichlet α, clusters per node k')?
- **RQ3 (drift):** How quickly does the global model recover after abrupt, gradual or local-only drift, under a fixed budget?
- **RQ4 (network):** How robust is the system to latency, loss, bandwidth caps and node churn, and what do Kafka's retention and replay contribute?
- **RQ5 (scalability):** How do throughput, end-to-end latency and consumer lag scale with node count and stream rate on commodity hardware?

---

## 4. Objectives

1. **O1.** Design an objective-linked staleness score `Δ_i(t)` computable locally in `O(q·k·d)` time from CF vectors and the broadcast global centers.
2. **O2.** Design a budget-constrained, link-aware send policy with a provable long-run budget guarantee and a bound on server-side cost error.
3. **O3.** Design a heterogeneity- and staleness-aware macro-aggregation step that preserves exclusive and minority clusters.
4. **O4.** Implement an open-source, Kafka-native, reproducible testbed (Docker Compose, KRaft) with network-constraint injection, runnable on a laptop.
5. **O5.** Evaluate the method rigorously against at least 6 baselines on at least 4 datasets and at least 3 non-IID schemes, with at least 5 seeds, confidence intervals and significance tests.
6. **O6.** Produce a submission-ready manuscript and a reproducibility artifact.

---

## 5. Methodology

### 5.1 System architecture (Kafka-native)
```
                 ┌──────────────────── Kafka (KRaft) ─────────────────────┐
 Edge node i ──► │ fsc.summaries   key=node_id  (delta log, P partitions) │ ──► Coordinator(s)
 (river/CF,      │ fsc.snapshots   key=node_id  (log-compacted)           │     consumer group
  trigger,       │ fsc.global      key="model"  (log-compacted, 1 part.)  │ ◄── publishes C(t)
  producer)  ◄── │ fsc.metrics     (telemetry: bytes, lag, latency)       │
                 └────────────────────────────────────────────────────────┘
```
- **Ordering:** keying by `node_id` gives per-node total order, so deltas are applied in sequence without extra coordination.
- **Idempotent producers** (`enable.idempotence=true`) prevent duplicated deltas under retries. Where needed, delta-apply plus offset-commit is made transactional (exactly-once semantics).
- **Snapshots on a log-compacted topic:** the latest full summary per node survives indefinitely. A restarted coordinator rebuilds its state by reading the compacted snapshots and replaying deltas from the committed offsets. This is the *recovery protocol* evaluated in RQ4.
- **Downlink:** global centers go to a compacted `fsc.global` topic. Edges consume them to compute the objective-linked trigger (§5.3). This is the federated "broadcast" step, and it is also rate-limited.
- **Laptop footprint:** the whole testbed runs in under 1 GB of RAM (measured: Kafka 317–423 MB with a 256 MB heap; all simulated edge nodes share one Python process). See `docs/02_low_memory_setup.md`.
- **Scale-out:** coordinators form a consumer group. Partial aggregation per partition is followed by a final merge; this works because CFs are additive. PySpark Structured Streaming is an optional scale-out variant.

### 5.2 Local model (edge)
- A DenStream-style or CluStream-style online micro-clustering with time-decayed CFs. We reimplement CFs ourselves so we can control serialisation; `river` provides reference implementations and baselines.
- **Delta encoding:** each message carries only micro-clusters created, updated beyond ε, or deleted since the last acknowledged send. Payload: float16/float32 quantised `LS`; `SS` as a scalar (or diagonal), header and sequence number. Serialisation is compact binary (msgpack or Avro); bytes are measured exactly.

### 5.3 Objective-linked staleness score
For a CF with centroid μ = LS/n and scalar `SS = Σ‖x‖²`, the k-means cost of assigning the whole micro-cluster to center `c` has a closed form:

```
cost(CF, c) = SS − 2·LS·c + n‖c‖²
```

With the current global centers `C` from the downlink, node i computes, per global cluster `j`:

```
J_ij(S) = Σ_{CF∈S, a(CF)=j} cost(CF, c_j)        n_ij(S) = Σ_{CF∈S, a(CF)=j} n
```

The **staleness score** of the server's copy of node i is:

```
Δ_i(t) = Σ_j | J_ij(S_i(t)) − J_ij(Ŝ_i) |  +  ρ · Σ_j ‖c_j‖·| n_ij(S_i(t)) − n_ij(Ŝ_i) |  +  κ · N_i^far(t)
         └──── cost drift per global cluster ──┘   └────────── mass shift across clusters ──────────┘   └ novelty ┘
```

`N_i^far` is the decayed mass in micro-clusters farther than `r_far` from every global center. It captures emerging concepts: a new cluster seen only at node i, which is the critical non-IID case. `Ŝ_i` is the node's local copy of what it last sent, so the node knows exactly what the server holds.

**Proposition 1 (staleness bound, to be proven).** If every node keeps `Δ_i(t) ≤ τ_i(t)` between sends, then for the server-side cost estimate,

```
| Ĵ(C) − J(C) | ≤ Σ_i τ_i(t)
```

for the current `C`. This follows directly from the per-cluster triangle inequality on additive CF costs. It links the trigger threshold to global objective error, which is the key theoretical contribution. We will also bound the centroid displacement of the resulting weighted-Lloyd step.

### 5.4 Budget-constrained, link-aware send policy (dual ascent)
Node i sends at time t if

```
Δ_i(t)  ≥  λ_i(t) · p_i(t) · b̂_i(t)
```

- `b̂_i` is the size of the pending delta in bytes, known before sending.
- `p_i(t)` is the link price, estimated online from the producer's delivery-report latency and error rate, plus netem-imposed caps.
- `λ_i(t)` is a Lagrange multiplier updated per window by projected dual ascent:

```
λ_i ← max(λ_min, λ_i + η · (bytes_i(window) − B_i) / B_i)
```

A hard safety valve (a token bucket) enforces the budget exactly under bursts. A novelty override lets a large `N_i^far` jump the queue so it is not starved; it is still charged to the bucket.

**Proposition 2 (budget satisfaction, to be proven).** Under bounded message sizes, the long-run average bytes of node i are at most `B_i/W + O(1/(ηT))`. This is the standard dual-ascent / drift-plus-penalty argument (Neely, 2010), adapted to event triggering.

Together with Proposition 1, the budget determines λ, λ determines the threshold, and the threshold bounds cost error. That chain is the explicit cost–accuracy trade-off curve the paper will plot and characterise.

### 5.5 Heterogeneity- and staleness-aware aggregation (coordinator)
1. **Staleness weighting:** before merging, decay every received CF to the current time, i.e. multiply `(n, LS, SS)` by `2^{-λ_d (t − t_sent)}`. Stale nodes then fade instead of dominating.
2. **Node-balanced weighting:** a weight `w_i = (n_i)^β`, with β ∈ [0,1], so one high-rate node cannot swamp others. β = 1 is plain mass weighting. This is evaluated as an ablation.
3. **Macro-clustering:**
   - Variant K: weighted k-means++ plus weighted Lloyd over micro-cluster centroids with the CF cost. Following k-FED, per-node local centers seed the global centers, which exploits k' ≪ k heterogeneity.
   - Variant D: weighted DBSCAN or shared-density reachability (DBSTREAM-style) for unknown k and arbitrary shapes.
4. **Exclusive-cluster protection:** a micro-cluster group that is dense, supported by a single node and far from every existing center is kept as its own cluster rather than absorbed into the nearest one. This is the mechanism behind RQ2.
5. **Incremental aggregation:** the coordinator re-aggregates on a period or after M deltas, warm-starting from `C(t−1)`.

### 5.6 Network constraints and fault model
- A seeded **in-application network emulator** in each node's send path adds delay (50–500 ms), jitter, loss (0–10 %), bandwidth caps (64 kbit–10 Mbit) and scripted disconnections. It works on any OS and costs no extra memory. On Linux, `tc netem` is an optional cross-check.
- Node churn: scripted kill and restart of edge and coordinator containers.
- Broker faults: a single-broker restart. A 3-broker cluster is out of scope on laptops.

### 5.7 Optional extension: privacy
Gaussian-mechanism noise on the CF deltas, with sensitivity bounded by clipping ‖x‖ ≤ R, gives a (ε, δ) guarantee per send. The budget policy then also limits the privacy spend: fewer sends means less composition. We report this as a three-way trade-off if time permits.

---

## 6. Experimental design

### 6.1 Datasets
| Dataset | Why | Notes |
|---|---|---|
| Synthetic Gaussian/RBF with controlled drift (`river.datasets.synth`) | Ground truth; controlled drift and non-IID | Main tool for RQ2 and RQ3 |
| KDD Cup 99 (10 %) / NSL-KDD | Classic stream-clustering benchmark | Reviewers know it well; also flagged as dated, so we do not rely on it alone |
| CIC-IDS2017 or UNSW-NB15 | Modern network-intrusion traffic; natural edge story | Numeric flow features, standardised |
| Forest Covertype | Standard stream benchmark, 7 classes | Label skew is easy to induce |
| Intel Berkeley Lab sensor data or Electricity (ELEC2) | Real IoT drift | Natural per-sensor partitioning, which is *real* non-IID |

Labels are used **only for evaluation**, never for clustering.

### 6.2 Non-IID partitioning
- **Label skew:** Dirichlet(α), α ∈ {0.1, 0.5, 1, 100 (≈IID)}.
- **Cluster-exclusive:** each node sees k' ∈ {1, 2, √k, k} clusters, which tests the k-FED regime.
- **Quantity skew:** rates follow a power law.
- **Temporal / local drift:** drift injected at a subset of nodes only.
- **Natural partitioning:** per-sensor or per-subnet splits.

### 6.3 Baselines
1. **Centralised-Raw:** every point is sent to the coordinator via Kafka. This is the upper bound on quality and the maximum on bytes.
2. **Naive-Federated:** send the full summary on every update.
3. **Periodic-T:** full summary every T seconds, for several values of T.
4. **Change-Threshold (Tran 2013 / DGClust-style):** send when micro-clusters are created or deleted, or when centroids move more than ε.
5. **Norm-Event-Triggered (EventGraD-style):** send when ‖S_i − Ŝ_i‖ (flattened) exceeds an adaptive threshold.
6. **k-FED-periodic:** one-shot k-FED re-run on periodic local k-means centers.
7. **Ablations of FedCAST:** without the budget controller (fixed λ), without link price, without the novelty term, without staleness decay, without exclusive-cluster protection, and full summaries instead of deltas.

### 6.4 Metrics
- **Quality:** ARI, NMI, purity, SSQ ratio to centralised, CMM (evolving-stream measure), recall of minority and exclusive clusters.
- **Cost:** bytes on the wire (measured at Kafka and at the application), messages sent, bytes as a share of Centralised-Raw, and a per-node energy proxy.
- **Timeliness:** end-to-end latency from event to global-model update, consumer lag, and drift-recovery time (time until ARI returns within 5 % of its pre-drift value).
- **System:** throughput (events/s per node), coordinator CPU and memory, recovery time after a crash.
- **Statistics:** 5–10 seeds, 95 % confidence intervals, Friedman test with a Nemenyi post-hoc and critical-difference diagrams across datasets, and Wilcoxon signed-rank for pairwise comparisons.

### 6.5 Planned figures and tables
1. Pareto front of ARI against bytes for all methods (the headline figure)
2. Budget tracking: bytes per window against the target B over time, per node
3. ARI against Dirichlet α, and ARI against k'
4. ARI over time around drift events, with recovery times
5. Latency and lag under netem profiles, and a recovery timeline after a coordinator or node crash
6. Scalability: throughput and latency against the number of nodes (5–50) and the event rate
7. An ablation table and a critical-difference diagram

---

## 7. Expected outcomes and contributions
1. **C1 – Formulation:** budget-constrained federated stream clustering as an online constrained optimisation problem.
2. **C2 – Algorithm:** FedCAST, with an objective-linked CF staleness score, a dual-ascent link-aware send policy and heterogeneity-aware aggregation.
3. **C3 – Theory:** a staleness-to-cost-error bound (Proposition 1) and a long-run budget guarantee (Proposition 2).
4. **C4 – System:** an open-source Kafka-native implementation whose recovery protocol is built on log compaction and offset replay.
5. **C5 – Evidence:** extensive experiments. Target hypotheses, to be tested and reported honestly whether or not they hold:
   - **H1:** at least 50 % fewer bytes than Periodic and Change-Threshold baselines at the same ARI (within 2 %)
   - **H2:** higher exclusive-cluster recall than naive aggregation for α ≤ 0.5
   - **H3:** faster drift recovery than Periodic at equal budget
   - **H4:** zero summary loss and bounded recovery time under node and coordinator crashes

---

## 8. Threats to validity and mitigations
- **Laptop scale:** this is an emulated testbed (all nodes in one process, emulated links, under 1 GB of RAM). We report per-node rates and extrapolate carefully. Byte counts are exact, because they are measured on real Kafka messages.
- **Hyper-parameter sensitivity** (ρ, κ, η, β): we run sensitivity sweeps and use one fixed default configuration across all datasets.
- **Metric bias:** we use several external and internal metrics, plus CMM for evolving streams.
- **Dataset criticism** (KDD99): it is paired with modern datasets (CIC-IDS2017 / UNSW-NB15) and real sensor partitions.

---

## 9. Target venues (verify quartile and scope on SJR/JCR before submitting)
All journals below accept submissions year-round. Review usually takes 2–6 months.

| Publisher | Journal | Fit |
|---|---|---|
| IEEE | *IEEE Internet of Things Journal* | Edge, IoT and federated systems with a systems evaluation. Strong fit. |
| IEEE | *IEEE Transactions on Knowledge and Data Engineering* | Stream mining with theory. Highest bar. |
| IEEE | *IEEE Transactions on Parallel and Distributed Systems* | If the systems contribution dominates |
| Elsevier | *Future Generation Computer Systems* | Kafka-ML was published here. Very good fit. |
| Elsevier | *Information Sciences* / *Knowledge-Based Systems* | Algorithm focus |
| Springer | *Data Mining and Knowledge Discovery* / *Machine Learning* | Stream clustering community (Gama, Bifet, et al.) |
| Springer | *Journal of Big Data* / *Cluster Computing* | Faster review; a fallback option |
| ACM | *ACM Transactions on Knowledge Discovery from Data* | Stream mining |
| ACM | *ACM Transactions on Internet of Things* / *ACM Transactions on Internet Technology* | Edge systems |

**Conferences** (check the current call for papers for deadlines): **ACM DEBS** (Distributed and Event-Based Systems, the most natural home for Kafka work), IEEE BigData, ECML-PKDD, PAKDD, CIKM, IEEE ICDM, ICDE. ECML-PKDD and DMKD also run a *journal track*, which is worth considering.

**Recommendation:** aim the first submission at **Elsevier FGCS** or the **IEEE Internet of Things Journal**. The systems plus algorithm plus theory mix fits both. Hold TKDE for a strengthened version if the theory turns out to be clean.

---

## 10. Key references (to be expanded in BibTeX during writing)
1. C. C. Aggarwal, J. Han, J. Wang, P. S. Yu. A framework for clustering evolving data streams (CluStream). VLDB 2003.
2. F. Cao, M. Ester, W. Qian, A. Zhou. Density-based clustering over an evolving data stream with noise (DenStream). SDM 2006.
3. M. Hahsler, M. Bolaños. Clustering data streams based on shared density between micro-clusters (DBSTREAM). IEEE TKDE 2016.
4. G. Cormode, S. Muthukrishnan, W. Zhuang. Conquering the divide: Continuous clustering of distributed data streams. ICDE 2007.
5. J. Gama, P. P. Rodrigues, L. Lopes. Clustering distributed sensor data streams using local processing and reduced communication. Intelligent Data Analysis 15(1), 2011.
6. D.-H. Tran. Communication-efficient exact clustering of distributed streaming data. ICCSA 2013 (arXiv:1209.4257).
7. M.-F. Balcan, S. Ehrlich, Y. Liang. Distributed k-means and k-median clustering on general topologies. NeurIPS 2013.
8. D. K. Dennis, T. Li, V. Smith. Heterogeneity for the win: One-shot federated clustering. ICML 2021.
9. M. Stallmann, A. Wilbik. Towards federated clustering: A federated fuzzy c-means algorithm (FFCM). arXiv:2201.07316, 2022.
10. Y. Zhang et al. Asynchronous federated clustering with unknown number of clusters. AAAI 2025.
11. M. Angelova et al. FedCluLearn: Federated continual learning using stream micro-cluster indexing scheme. ECML-PKDD 2025.
12. P. R. Silva, J. Vinagre, J. Gama. Federated anomaly detection over distributed data streams. arXiv:2205.07829, 2022.
13. B. Li et al. Density-aware and cluster-based federated anomaly detection on data streams. WSDM 2025.
14. S. Ghosh et al. EventGraD: Event-triggered communication in parallel machine learning. Neurocomputing 2021 (arXiv:2103.07454).
15. S. Zehtabi et al. Decentralized event-triggered federated learning with heterogeneous communication thresholds. arXiv:2204.03726, 2022.
16. C. Martín et al. Kafka-ML: Connecting the data stream with ML/AI frameworks. FGCS 2022.
17. A. Carnerero-Cano et al. Towards flexible data stream collaboration: Federated learning in Kafka-ML. Internet of Things, 2024.
18. A. Epasto et al. Differentially private clustering in data streams. arXiv:2307.07449, 2023.
19. M. J. Neely. Stochastic Network Optimization with Application to Communication and Queueing Systems. Morgan & Claypool, 2010.
20. M. Zubaroğlu, V. Atalay. Data stream clustering: A review. Artificial Intelligence Review, 2021.
21. J. Montiel et al. River: machine learning for streaming data in Python. JMLR 2021.
22. Fielding: Clustered federated learning with data drift. arXiv:2411.01580.

> Some author lists and venues above (e.g. 14, 15, 17, 22) were gathered from search snippets and must be checked against the actual papers before they go into the bibliography.

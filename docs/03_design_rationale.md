# Design rationale: why this stack, why these features, why not the alternatives

Every choice below is judged against the project's four hard constraints:

1. **Kafka is mandatory** (course requirement), and it should be essential to the design, not decoration.
2. **Laptops with less than 4 GB of free RAM.**
3. **Research quality:** results must be reproducible, measurable to the byte, and statistically defensible.
4. **One month** to build, run and write.

---

## 1. Messaging backbone: Apache Kafka (KRaft mode)

| Requirement of the system | What Kafka gives | Used in FedCAST as |
|---|---|---|
| Updates from one node must be applied in order (a delta assumes the previous one) | Total order **per partition**; keying by `node_id` pins a node to one partition | `fsc.summaries`, key = `node-<i>` |
| Retries on bad links must not double-count data | **Idempotent producer** (`enable.idempotence=true`) | All producers; plus per-node sequence numbers in our wire format |
| A crashed coordinator must recover without asking nodes to resend | **Durable, replayable log** with committed consumer offsets | Replay of `fsc.summaries` from the checkpointed offsets |
| Recovery must not replay the whole history | **Log compaction** keeps only the latest record per key | `fsc.snapshots`: coordinator changelog, one record per node |
| Nodes need the current global model, including nodes that join late | Compacted topic plus "read from end" | `fsc.global`, key = `model` |
| Must scale beyond one coordinator | **Consumer groups** split partitions across coordinator instances | Supported: CF sums are additive, so partial aggregates merge exactly |
| Must be cheap to run | KRaft mode removes ZooKeeper; broker runs in a 256 MB heap | Measured 317–423 MB RSS (docs/02_low_memory_setup.md) |

**Measured, not assumed.** On a real broker we ran the following (results in `results/kafka_K*.json`):
- **Coordinator killed with SIGKILL and restarted:** state rebuilt from the changelog in about 1 s, and **identical** to a shadow coordinator that never crashed.
- **Broker offline for 34 s in the middle of a run:** 231 of 231 summaries delivered, **0 lost**, 0 delivery errors.
- **Byte accounting:** our estimate is within about 3 % of Kafka's own producer statistics.

### Alternatives considered

| Alternative | Why not |
|---|---|
| **MQTT** (Mosquitto, EMQX) | Great for tiny devices, but a *broker queue*, not a log: no replay after a coordinator crash, no compaction, and ordering and "exactly once" (QoS 2) are per session only. We would have to rebuild what Kafka gives us. MQTT can still feed Kafka at the edge (MQTT → Kafka bridge) in a deployment. |
| **RabbitMQ** | Messages are removed once consumed, so replay-based recovery is impossible without extra stores. Streams plugin exists but is less mature and heavier. |
| **Apache Pulsar** | Log semantics similar to Kafka, but it needs BookKeeper plus brokers (and ZooKeeper or oxia): too heavy for a laptop, and not the course's required technology. |
| **Redpanda** | Kafka-API compatible and lighter (C++), but it is not Apache Kafka (the course requirement) and its free licence is not open source. Our code would run on it unchanged. |
| **Cloud queues** (Kinesis, Pub/Sub) | Paid, need accounts and internet, and are not reproducible offline. |
| **gRPC / REST direct to the server** | No buffering during outages, no replay, and every guarantee would have to be hand-built. |

---

## 2. Stream processing: plain Python services on Kafka, not Flink or Spark

| Option | Laptop RAM | Fits the algorithm? | Verdict |
|---|---|---|---|
| **Python + confluent-kafka** (chosen) | about 150 MB per process (measured) | Complete control over *when* to send (the research contribution) | ✅ |
| Apache Flink (PyFlink) | JobManager + TaskManager ≥ 1.5–2 GB | The edge logic is per-device decision making, not a dataflow operator; the coordinator's state is small | ❌ Too heavy; adds nothing we need |
| Spark Structured Streaming | JVM driver + executors ≥ 1–2 GB, micro-batch latency | Micro-batching adds latency; state is tiny | ❌ Kept only as optional scale-out idea |
| Kafka Streams / ksqlDB | JVM; Java or SQL | The clustering code (numpy) is Python | ❌ Language mismatch |

The coordinator *does* follow stream-processing best practice: a **state store backed by a compacted changelog topic**, restored and then replayed from offsets. This is the Kafka Streams design, implemented in about 150 lines of Python.

**Why `confluent-kafka` and not `kafka-python`?** It wraps librdkafka (C), which is faster, gives idempotence and full producer statistics (we use `txmsg_bytes` to validate byte counts), and is actively maintained.

---

## 3. Local model: CF micro-clusters (CluStream/DenStream family)

| Option | What is sent | Additive / mergeable? | Supports an objective-linked trigger? |
|---|---|---|---|
| **CF micro-clusters** (chosen) | `(n, LS, SS, t)` per micro-cluster | ✅ exactly additive; decay is a scalar multiply | ✅ k-means cost of a CF has a closed form: `SS − 2·LS·c + n‖c‖²` |
| Grid cells (DGClust, D-Stream) | Cell counts | ✅ | ⚠️ Grid size grows exponentially with dimension (NSL-KDD has 51 features) |
| Coresets (Balcan et al.) | Weighted points | ✅ | ⚠️ Rebuilding coresets on a stream is costly; no cheap per-update delta |
| Sketches (Count-Min, etc.) | Counters | ✅ | ❌ Not a geometric summary |
| Local k-means centres (k-FED) | k' centres | ✅ as CFs | ⚠️ Loses shape and size; needs k' per device; strong when clusters are well separated (we report this honestly) |
| Sending raw points | Everything | — | Upper bound on quality; highest cost (Centralised-Raw baseline) |

**Key property we exploit:** decay multiplies `n`, `LS` and `SS` by the same factor, so the centroid never changes with decay. The coordinator reproduces a node's decayed state **exactly** from the last CF plus its timestamp. The node therefore knows precisely what the server holds, which makes the staleness score computable on the device.

---

## 4. The send policy: why FedCAST's design beats the alternatives

| Trigger family | Rule | Weakness | FedCAST's answer |
|---|---|---|---|
| Periodic (classic federated learning) | Every T seconds | Blind to change: wastes bytes on static data, stale during drift | Sends *when* the global objective is affected |
| Change-threshold (Tran 2013, DGClust) | When a micro-cluster changes by more than ε | ε is in data units: retune per dataset; no budget | Budget is the input; threshold λ adapts automatically |
| Norm-triggered (EventGraD) | When ‖S − Ŝ‖ exceeds δ | Norm of *parameters*, not of the clustering objective | Staleness measured in **clustering cost** (Propositions 1 and 2) |
| Fixed budget with FIFO or random sends | Token bucket only | Spends bytes on unimportant updates | Knapsack-style prioritisation: highest-impact micro-clusters first (γ-coverage) |

FedCAST's four ingredients, each with an ablation in experiment E5:

1. **Objective-linked staleness** `Δ = Σ e_id`, where `e_id = |J_id − Ĵ_id| + ρ(‖ΔLS‖ + ‖c‖·|Δn|)`. It is an *upper bound* on the server's k-means cost error and on centroid displacement, so a small Δ provably means a nearly up-to-date global model.
2. **Prioritised partial deltas:** send only the micro-clusters covering a fraction γ of Δ. Many micro-clusters change by one point; those bytes are wasted until the change accumulates.
3. **Exponentiated dual ascent on λ:** `λ ← λ·exp(η(spent − B)/B)` per window. It is *scale-free*: the same η works on every dataset because λ has no units to tune. A **token bucket** makes the budget a hard guarantee.
4. **Link price p and novelty override:** expensive links send only high-value updates, and a brand-new cluster (mass far from every global centre) jumps the queue.

**Why dual ascent rather than a PID controller or a fixed threshold?** It is the textbook method for "maximise utility subject to a long-run budget" (Lagrangian relaxation; Neely's drift-plus-penalty). It comes with a convergence guarantee (Proposition 3), has one parameter (η), and a multiplicative update removes scale dependence. PID would need three gains per dataset; a fixed λ breaks as soon as the data rate changes.

---

## 5. Aggregation: weighted k-means over CFs with swap local search

- **Weighted k-means++ plus Lloyd on CF centroids** is exact for the CF cost and cheap (hundreds of micro-clusters, not millions of points).
- **Warm start** from the previous global model keeps cluster identities stable over time. Warm start alone gets stuck, though: a new cluster at one node is absorbed by its nearest large neighbour.
- **Swap local search** (Kanungo et al., 2004) moves the centre with the smallest Ward merge cost to the worst-served micro-cluster, and keeps the move only if the total cost drops. It is a principled k-means local search with approximation guarantees, and it is what protects node-exclusive clusters in the non-IID setting (see the test `test_swap_search_recovers_small_far_cluster` and ablation E2/E5).
- **Staleness decay at the server:** a node that goes silent fades out instead of freezing old mass into the model.
- **Node balancing β:** optional; stops one high-rate node dominating (ablation in E2).

Alternatives: DBSCAN over micro-clusters (DenStream offline phase) needs ε and minPts tuning per dataset and has no fixed k to evaluate against labels. Federated averaging of centres (FedAvg style) mismatches cluster identities across nodes, the well-known label-permutation problem.

---

## 6. Wire format: fixed binary layout (struct + numpy)

| Format | Size known before encoding? | Dependencies | Speed |
|---|---|---|---|
| **Fixed binary** (chosen) | ✅ exact (the send policy needs `b` before deciding) | none | zero-copy decode |
| JSON | ❌ varies with number formatting; 3–5× larger | none | slow |
| msgpack | ≈ | small | fast |
| Avro / Protobuf + schema registry | ≈ | registry service (extra RAM) | fast |

A delta for one micro-cluster in d dimensions costs `24 + 4d` bytes, plus a 20-byte message header and about 24 bytes of Kafka record overhead.

---

## 7. Evaluation methodology

- **Two execution modes with the same code.** A deterministic discrete-event **simulator** runs thousands of seeded runs (sweeps, seeds, ablations) on a laptop. The **real Kafka runtime** measures latency, recovery and byte accounting. The simulator reproduces the Kafka properties that matter (per-key order, retries with counted retransmissions, buffering during outages), and its byte counts are validated against Kafka's producer statistics.
- **In-app network emulation** instead of `tc netem`: it works on Windows and macOS, is seeded and repeatable, and costs no RAM.
- **Two kinds of quality metric.** *Objective*: k-means cost relative to the centralised learner with all raw data, the quantity the method optimises. *Label-based*: ARI, NMI, purity, per-class recall.
- **Budget-matched comparison.** Each method's quality is interpolated at equal byte budgets (1 %, 3 %, 10 % of Centralised-Raw). This is fairer than comparing arbitrary parameter settings.
- **Statistics:** 5 seeds; Friedman test with Nemenyi critical difference over dataset × budget blocks; one-sided Wilcoxon signed-rank with Holm correction (Demšar, JMLR 2006).

---

## 8. Datasets

| Dataset | Why it is in the paper |
|---|---|
| **SynDrift** (ours, seeded) | Ground truth for drift: anisotropic, unequal clusters; 4 drifting, 2 emerging, 1 vanishing |
| **NSL-KDD** | Network intrusion traffic: the natural "edge monitoring" workload; 51 features after encoding |
| **Shuttle** | Extreme class imbalance (78 % one class): tests minority clusters |
| **Pen-Digits** | Classic clustering benchmark with natural cluster structure |
| **Letter** | 26 classes: many clusters, hard for any method |

Non-IID splits: IID, Dirichlet(α) label skew, cluster-exclusive nodes (k-FED's regime), class-rotation drift, quantity skew.

# Brutally honest assessment: how good and how novel is FedCAST?

This document is written as a hostile reviewer would write it, then answers each criticism with an experiment. Every number comes from `results/` (experiments E1–E11, K1–K4; about 3,800 runs in total).

## 1. Rating

| Dimension | Score | Why |
|---|---|---|
| **As a course project** (Stream Processing Analytics) | **9/10** | Kafka is essential to the design (ordering, idempotence, replay, compaction) and tested under crashes; real system plus simulator with one code base; 3,000+ seeded runs; statistics; under 1 GB of RAM. |
| **Engineering quality** | 8/10 | Clean package, 13 tests (including numerical checks of the theory), resumable experiment runner, a real bug found and fixed by the 50-node run. Missing: CI, type checking, multi-machine runs. |
| **Research novelty (before this review)** | **5/10** | Most building blocks are known (see §2). The one component carrying the gains was untested against its obvious competitor. |
| **Research novelty (after this review)** | **6/10** | The novelty is now *isolated and proven where it holds* (non-stationary streams, p = 0.003), the theory has a real end-to-end guarantee, and the limits are measured, not guessed. The effect is real but modest (mean cost-ratio gain 0.03), which caps the rating. |
| **Realistic venue** | Q2 journal / good workshop now; Q1 (FGCS, IEEE IoT-J) plausible **only** if the story is reframed around the drift finding (§4) and a multi-machine deployment is added. TKDE: unlikely. |

## 2. What is *not* novel (and a reviewer will say so)

| Component | Prior art |
|---|---|
| Cluster-feature micro-clusters | BIRCH (1996), CluStream (2003), DenStream (2006) |
| Sending summaries to a coordinator | Cormode et al. (ICDE 2007), DGClust (2011), Tran & Sattler (2013) |
| Event-triggered communication | EventGraD (2021), event-triggered FL (2022) |
| **Budgeted top-k sparse deltas with error feedback** | Standard in federated learning (Aji & Heafield 2017; Stich et al. 2018). Our "prioritised partial deltas" are structurally this. |
| Token bucket, dual ascent | Textbook traffic shaping / Lagrangian relaxation (Neely 2010) |
| Swap local search | Kanungo et al. (2004) |
| Changelog-based recovery | Kafka Streams / Flink state management |
| Propositions 1–2 | Essentially triangle inequalities |

**Our own ablations also killed three "contributions":** the novelty override is redundant (E8: identical detection delay), the link price shifts only 4–7 % of traffic and only under loose budgets (E4, E8), and dual ascent does not change quality (E8).

## 3. The killer question, and the answer

> *"Is ranking micro-clusters by their impact on the clustering objective better than the standard FL compressor, top-k by change magnitude, run under the same budget controller?"*

We implemented exactly that competitor (`rank="norm"`), plus a model-free variant (`rank="uniform"`), and ran paired comparisons on the same seeds and budgets (E9, E10).

**E9 (original streams, 150 pairs):**

| Data | objective better than top-k magnitude | p (one-sided Wilcoxon) |
|---|---|---|
| Drifting stream (SynDrift) | **80 %** of pairs | **1.5×10⁻⁵** |
| Static real datasets | 45 % | 0.71 (tie) |

**E10 (the four real datasets made into evolving streams, 100 pairs):**

| Data | objective better | p |
|---|---|---|
| NSL-KDD (evolving) | 60 % | 0.048 |
| Letter (evolving) | 80 % | 0.006 |
| Shuttle (evolving) | 56 % | 0.33 |
| Pen-Digits (evolving) | 32 % | 0.88 (magnitude better) |

**Pooled over everything (250 pairs, `results/tables/objective_vs_norm_pooled.json`):**

| Setting | pairs | objective better | mean cost-ratio gain | p |
|---|---|---|---|---|
| Static streams | 120 | 45 % | −0.001 | 0.71 (tie) |
| **Non-stationary (drift + evolving)** | **130** | **62 %** | **+0.030** | **0.003** |
| All | 250 | 54 % | +0.015 | 0.022 |

On NSL-KDD (evolving), objective ranking reaches within 5 % of centralised quality at 232 kB; top-k magnitude never does.

**Uncomfortable consequence:** on static data, most of FedCAST's advantage over the E1 baselines comes from the *budgeted sparse-delta structure*, which top-k magnitude also has, not from the objective score. The E1 baselines (Periodic, Change-Threshold, Norm-Trigger) lack sparsification, so they were weaker than the FL state of the art. **Any submission must include the top-k magnitude baseline.**

## 4. The novelty that survives: objective-aware communication for *evolving* streams

The defensible claim is sharper and more stream-specific than the original one:

1. **Finding.** Under concept evolution (drift, emerging clusters), what to send should be decided by the change in the *clustering objective under the current global model*, not by the magnitude of the parameter change. A magnitude ranking spends bytes on large but objective-neutral changes (e.g. mass growth of an already well-represented cluster), and misses small but objective-critical ones (the first points of a new cluster far from every centre). On static data the two coincide, so the choice does not matter there. This is a new, testable and now-tested statement about stream learning, not federated learning in general.
2. **Theory (upgraded).** A new end-to-end guarantee replaces the triangle-inequality propositions:

   > **Theorem (staleness-robust approximation).** Let every candidate centre satisfy ‖c‖ ≤ R, and let U = Σᵢ Σ_id (|ΔSS| + 2R‖ΔLS‖ + R²|Δn|) be the model-free staleness of all nodes. If the coordinator computes a γ-approximate k-means solution Ĉ on the stale summaries, then on the true current summaries
   > J(S; Ĉ) ≤ γ · OPT(S) + (1 + γ) · U.
   >
   > *Proof.* For any c with ‖c‖ ≤ R, |cost(CF,c) − cost(ĈF,c)| = |ΔSS − 2ΔLS·c + Δn‖c‖²| ≤ |ΔSS| + 2R‖ΔLS‖ + R²|Δn|, and |min_c a_c − min_c b_c| ≤ max_c |a_c − b_c|. Hence |J(S;C) − J(Ŝ;C)| ≤ U for every C in the R-ball. Optimal centres are convex combinations of data points, so they lie in the ball, and J(S;Ĉ) ≤ J(Ŝ;Ĉ) + U ≤ γ·OPT(Ŝ) + U ≤ γ(OPT(S) + U) + U. ∎

   It is checked numerically on 200 random centre sets in `tests/test_core.py::test_uniform_staleness_bound_holds_for_every_centre_set`. It gives a *model-free* staleness score whose sum bounds the extra cost of *any* aggregation algorithm. The objective score is the tighter, model-specific counterpart, and the experiments show it is the better ranking (objective beats model-free: 56 % of all pairs, p = 5×10⁻⁴).
3. **System.** Kafka-native exactly-once state for federated summaries, validated under SIGKILL and broker outage. This is engineering, not research novelty, but it is what makes the method deployable and is valued by systems venues (FGCS, IoT-J, DEBS).

## 5. Improvements attempted in this review

| Attempt | Result | Verdict |
|---|---|---|
| Top-k magnitude baseline (FL standard) | Ties on static data, loses under drift (§3) | **Kept:** it is the correct baseline and defines the novelty |
| Model-free "uniform" ranking (no downlink) | Worse than objective (p = 5×10⁻⁴); much worse under drift at tight budgets | Kept for the theorem; not recommended as the ranking |
| Budget-adaptive resolution, q ≥ 2k | SynDrift: reaches 1.05 at 35 kB instead of 43 (and 1.10 at 28 kB instead of 40). Pen-Digits: 25 kB instead of 45 (21 vs 43), **beats k-FED**. NSL-KDD and Letter: no gain or worse | **Partial win:** data-dependent |
| Budget-adaptive resolution, q ≥ k | Big win on SynDrift (1.10 at 22 kB, 2.7× less than k-FED); bad on Letter (26 classes) | Data-dependent |
| Fidelity-driven resolution (node measures its own complexity) | Slides along the same Pareto curve, no improvement | **Negative:** reported, not adopted |
| Multi-resolution full summaries (k-FED-like coarsening + FedCAST trigger) | Worse than both k-FED and FedCAST almost everywhere | **Negative:** reported, not adopted |
| Evolving versions of the four real datasets (E10) | Objective beats magnitude on NSL-KDD and Letter, ties on Shuttle, loses on Pen-Digits. **k-FED and Periodic-Δ never reach within 5 % of centralised on any of the four**, while every budgeted sparse-delta variant does | **Kept:** it is the evidence for §4 |
| Hybrid ranking (objective + magnitude, normalised) | A 3-seed pilot looked excellent; the full 250-block test ranked it **worst** (mean rank 2.18 vs 1.84 objective, 1.98 magnitude; Friedman p = 8×10⁻⁴) | **Negative:** the pilot was noise; objective alone is best |

## 6. Final verdict

* **What FedCAST really contributes.** (1) Budgeted, prioritised sparse CF deltas for federated *stream clustering*, a combination nobody had applied to this problem. On evolving streams it beats periodic and one-shot federated clustering by a wide margin: k-FED and Periodic-Δ never reach within 5 % of centralised on the four real evolving streams, and FedCAST does. (2) Objective-aware ranking, which is significantly better than the standard FL compressor on non-stationary streams (p = 0.003) and no worse on static ones. (3) An end-to-end staleness-robust approximation guarantee. (4) A Kafka-native, crash-proven implementation.
* **What it does not contribute.** New stream-clustering algorithms, new budget control, or large effect sizes. Mean gains over the strongest baseline are a few percent of cost, or 10–40 % of bytes at equal quality.
* **Negative results worth publishing.** The novelty override, link price and dual ascent add little quality. Fidelity-driven resolution, multi-resolution summaries and hybrid ranking do not help.

## 7. What would make it a solid Q1 paper

1. **Reframe the paper** around §4: "objective-aware communication matters under concept evolution", with top-k magnitude as the main competitor. Drop the novelty-override and link-price claims (or present them as negative results).
2. **Replace Propositions 1–2** with the theorem in §4.2.
3. **Real deployment:** run the Kafka experiments on 3–5 physical laptops or Raspberry Pis over Wi-Fi/4G, and measure energy.
4. **A naturally partitioned, naturally drifting dataset** (e.g. per-sensor IoT or per-subnet network traces). Our datasets are static benchmarks turned into streams.
5. **Stronger baselines:** top-k magnitude (done), quantised deltas (e.g. 8-bit CFs), and a recent federated clustering method with an unknown k (AFCL, AAAI 2025).
6. **Solve the low-budget regime** properly. Adaptive resolution helps on some data; a learned or feedback-driven resolution (increase q while the server's cost keeps improving) is the natural next step.

---

# Round 2: attacking every weakness (E12–E14, deployment D1)

## R2.1 A sharper core idea: the *exact value* of an update

**Weakness attacked:** the objective ranking beat the FL-standard top-k magnitude ranking only weakly (57 % on evolving real streams, p = 0.09).

**New idea.** With assignments fixed, the coordinator's centre for cluster j is ĉⱼ = L̂ⱼ/N̂ⱼ. When the true summaries add ΔLⱼ and ΔNⱼ, the k-means cost of cluster j at ĉⱼ exceeds its optimum by **exactly**

> excessⱼ = ‖ΔLⱼ − ΔNⱼ ĉⱼ‖² / Nⱼ

(proof: J(c) = Σ‖x−c‖² is minimised at the mean, and J(ĉ) − J(c*) = N‖ĉ − c*‖² = ‖L − Nĉ‖²/N; checked in `test_lloyd_excess_is_exact`). Every node knows ĉⱼ and Nⱼ from the broadcast model and its own contribution rⱼ to ΔLⱼ − ΔNⱼĉⱼ. The **marginal value** of sending micro-cluster *id* is the exact reduction of that excess, (2 u·rⱼ − ‖u‖²)/Nⱼ, and nodes pick greedily by value (`rank="lloyd"`). Two properties explain why it beats magnitude ranking:
* a cluster that grows *in place* (ΔLS ≈ Δn·ĉ) has value ≈ 0, however large the change: magnitude ranking wastes bytes on exactly these updates;
* new mass far from its centre, in a small global cluster, has high value (∝ distance² / Nⱼ): the first points of an emerging cluster.

**Result (E12, same seeds and budgets, float32, 250 pairs):** it beats top-k magnitude in **75 %** of the 130 non-stationary pairs (**p = 7×10⁻⁹**) and the old objective ranking in 66 % (p = 7×10⁻⁶). It cuts the excess cost over the centralised learner by 86 % on SynDrift and 71 % on evolving Pen-Digits. On static streams it ties (38 %, p = 0.75).

## R2.2 Byte efficiency: 8-bit deltas with error feedback

Centroids are quantised to 8 bits per dimension relative to the message's own range, the count is float32 and the spread float16. A record drops from 24 + 4d to 14 + d bytes (2.7–3.5×). The node stores the *dequantised* copy as the server view, so quantisation error is corrected by the normal staleness mechanism (error feedback). Pilot: NSL-KDD evolving reaches the same quality with 2–3× fewer bytes. For fairness, **every baseline in E13/E14 uses the same 8-bit encoding.**

## R2.3 Real, naturally drifting and naturally partitioned data

**Weakness attacked:** only synthetic or artificially streamed static data. We added three real concept-drift streams, downloaded from GitHub mirrors:

| Dataset | Why it matters |
|---|---|
| **Intel Berkeley Lab** (54 motes, 5 weeks) | *Naturally partitioned* (one zone of motes per node) and naturally drifting (daily cycles, battery decay, sensor faults) |
| **Gas Sensor Array Drift** (36 months, 6 gases, 128 features) | Real sensor ageing |
| **CoverType in original order** | A standard real concept-drift benchmark |

## R2.4 Fixing the under-spending flaw → FedCAST-v3

E13 showed that FedCAST-v2 **plateaus** at loose budgets: once every remaining update has zero exact value, it stops sending even with budget to spare (NSL-KDD static stuck at 1.043 at 1 %, 3 % and 10 % of raw traffic). Two principled fixes, together **FedCAST-v3**:
1. **Value first, information second:** change magnitude, with weight 0.1, breaks ties among zero-value refinements.
2. **Use-it-or-lose-it:** tokens above the bucket capacity are lost, so when the bucket is about to overflow a send has zero opportunity cost and the threshold is waived.

## R2.5 Final combined evaluation (E14; 12 settings, 5 seeds, every method 8-bit)

Paired against top-k magnitude + 8-bit (same budget, same seed):

| Setting group | pairs | FedCAST-v3 better | mean cost-ratio gain | p (one-sided Wilcoxon) |
|---|---|---|---|---|
| Static | 100 | 35 % | −0.003 | 0.97 |
| Drifting (SynDrift) | 25 | **80 %** | +0.062 | **1.1×10⁻⁴** |
| Evolving real streams | 100 | **68 %** | +0.017 | **1.5×10⁻⁴** |
| **Naturally drifting real streams** | 75 | **75 %** | +0.069 | **2.3×10⁻⁶** |
| **All non-stationary** | 200 | **72 %** | +0.042 | **3.9×10⁻¹¹** |
| All | 300 | 60 % | +0.027 | 2.4×10⁻⁸ |

At matched budgets (Friedman over 31 blocks, p = 7×10⁻¹²), FedCAST-v3 (mean rank 1.68) and top-k + 8-bit (1.61) are statistically tied, and **both are far ahead of Periodic-Δ (3.23) and k-FED (3.48)**. FedCAST-v3 wins the *tight-budget* blocks on non-stationary data. For example, at 1 % of raw traffic: SynDrift 1.015 vs 1.052, Intel Lab 1.217 vs 1.429; at 3 %: Pen-Digits evolving 1.049 vs 1.114, Gas 1.127 vs 1.210. Top-k is marginally better on static streams at loose budgets.

## R2.6 Real multi-container deployment (D1)

Each of 10 edge nodes runs in its **own container** (own network stack, real TCP) against a real Kafka broker, with **kernel-enforced bandwidth caps** (`tc tbf`: 100 Mbit / 5 Mbit / 1 Mbit / 128 kbit per node). The sandbox kernel lacks `netem`, so delay and loss stay in the in-app emulator. Quality is measured offline from the coordinator's logged models against the simulated centralised learner.

| Stream | FedCAST-v3 | Top-k + 8-bit | Periodic-Δ + 8-bit |
|---|---|---|---|
| SynDrift | **35.9 kB → 1.006** | 40.5 kB → 1.019 | 30.1 kB → 1.154 |
| Intel Lab (natural partition) | **35.4 kB → 1.091** | 37.9 kB → 1.188 | 19.6 kB → 2.547 |

All summaries were applied, with 0 delivery errors, a median end-to-end latency of 31–39 ms, and about 110 MB per edge container. These are single runs, consistent with the simulations.

## R2.7 Updated rating

| Dimension | Before round 2 | After round 2 |
|---|---|---|
| Research novelty | 6/10 | **7/10**: a principled, *exact* value-of-update criterion with a closed-form derivation, significant on real naturally drifting data (p = 2×10⁻⁶) and on all non-stationary streams (p = 4×10⁻¹¹) against the strongest FL baseline |
| Evidence quality | synthetic and shuffled benchmarks, one machine | real drifting and naturally partitioned datasets, quantisation-fair baselines, multi-container deployment with kernel bandwidth caps |
| Honest limits | — | ties top-k on static streams; **k-FED still owns the very-low-byte end on Gas and CoverType** (fig_v3); Gas stays above 1.10 for every method; delay and loss not kernel-emulated; single-seed deployment runs; k known |

**The claim a paper can now defend:** *For federated clustering of **evolving** streams under a byte budget, the value of an update is its exact reduction of the server's Lloyd excess cost. Communicating by value, rather than by change magnitude (the federated-learning standard) or by schedule (periodic, k-FED), significantly improves quality per byte on synthetic, evolving and naturally drifting real streams, and is on par on static ones.*

**What would still raise it:** the unknown-k aggregator, a physical multi-device run with real wireless delay and loss, differential privacy on the quantised deltas, and a regret-style analysis of the value-greedy send policy.

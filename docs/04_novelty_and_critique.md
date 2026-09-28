# Brutally honest assessment: how good and how novel is FedCAST?

This document is written as a hostile reviewer would write it, then answers each criticism with an experiment. Every number comes from `results/` (experiments E1–E10, K1–K4).

## 1. Rating

| Dimension | Score | Why |
|---|---|---|
| **As a course project** (Stream Processing Analytics) | **9/10** | Kafka is essential to the design (ordering, idempotence, replay, compaction) and tested under crashes; real system plus simulator with one code base; 3,000+ seeded runs; statistics; under 1 GB of RAM. |
| **Engineering quality** | 8/10 | Clean package, 13 tests (including numerical checks of the theory), resumable experiment runner, a real bug found and fixed by the 50-node run. Missing: CI, type checking, multi-machine runs. |
| **Research novelty (before this review)** | **5/10** | Most building blocks are known (see §2). The one component carrying the gains was untested against its obvious competitor. |
| **Research novelty (after this review)** | **6–6.5/10** | The novelty is now *isolated and proven where it holds* (evolving streams), the theory has a real end-to-end guarantee, and the limits are measured, not guessed. |
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

**E10 (the four real datasets made into evolving streams):** see §5.

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
| Evolving versions of the four real datasets (E10) | %E10% | %E10VERDICT% |

## 6. What would make it a solid Q1 paper

1. **Reframe the paper** around §4: "objective-aware communication matters under concept evolution", with top-k magnitude as the main competitor. Drop the novelty-override and link-price claims (or present them as negative results).
2. **Replace Propositions 1–2** with the theorem in §4.2.
3. **Real deployment:** run the Kafka experiments on 3–5 physical laptops or Raspberry Pis over Wi-Fi/4G, and measure energy.
4. **A naturally partitioned, naturally drifting dataset** (e.g. per-sensor IoT or per-subnet network traces). Our datasets are static benchmarks turned into streams.
5. **Stronger baselines:** top-k magnitude (done), quantised deltas (e.g. 8-bit CFs), and a recent federated clustering method with an unknown k (AFCL, AAAI 2025).
6. **Solve the low-budget regime** properly. Adaptive resolution helps on some data; a learned or feedback-driven resolution (increase q while the server's cost keeps improving) is the natural next step.

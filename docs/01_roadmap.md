# Roadmap (4 weeks, laptops only)

| Week | Goal | Deliverables | Owner |
|---|---|---|---|
| 1 | Research plus testbed | `00_research_proposal.md` ✅; Kafka KRaft testbed ✅ (under 1 GB of RAM, with or without Docker); edge → Kafka → coordinator skeleton with Centralised-Raw and Naive-Federated baselines; data loaders and non-IID partitioners | Claude codes, team reviews |
| 2 | Core method | CF micro-clusters with delta encoding; staleness score (§5.3); dual-ascent send policy (§5.4); aggregation (§5.5); remaining baselines; in-app network emulator; unit tests | Claude codes, team reviews |
| 3 | Experiments | Experiment runner (YAML configs, seeds); RQ1–RQ5 runs; fault-injection runs; plots; statistics (Friedman/Nemenyi, Wilcoxon); proofs of Propositions 1 and 2 | Claude plus team (run on laptops) |
| 4 | Paper | LaTeX in the target venue's template (Elsevier FGCS or IEEE IoT-J); figures; reproducibility README; internal review; submission | Claude drafts, team edits |

## Decision gates
- **End of week 1:** the team approves the problem statement and method (FedCAST) before full implementation.
- **End of week 2:** a pilot result on synthetic data must show a better Pareto front than Periodic and Change-Threshold. If it does not, redesign the trigger before scaling up experiments.
- **End of week 3:** freeze results and pick the venue based on the strength of the theory and the results.

## Team tasks (things Claude cannot do for you)
- Download the datasets that need registration (CIC-IDS2017 / UNSW-NB15) if they can't be fetched automatically.
- Run long experiments on your laptops (scripts will be one command each).
- Check every reference against the original paper.
- Final authorship, affiliation, and cover letter; clear it with your professor.

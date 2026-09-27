# SPA: Budget-Aware Federated Stream Clustering over Apache Kafka

A course and research project for *Stream Processing Analytics*.

Edge nodes cluster their local, non-IID data streams into micro-clusters. They decide *when it is worth the network cost* to publish summary deltas to Kafka, and a coordinator builds a global clustering from what arrives, all within a per-node communication budget.

- 📄 Research proposal (problem, objectives, methodology, expected outcomes): [`docs/00_research_proposal.md`](docs/00_research_proposal.md)
- 🗺️ Roadmap: [`docs/01_roadmap.md`](docs/01_roadmap.md)
- 📘 One-document summary with flowcharts (PDF): [`docs/Project_Summary.pdf`](docs/Project_Summary.pdf)

## Quick start (Kafka testbed)

Requires Docker, Python 3.10+ and about 4 GB of free RAM.

```bash
docker compose -f docker/docker-compose.yml up -d        # Kafka (KRaft, single broker) + topic setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Repository layout
```
docs/      research proposal, roadmap, later the paper drafts
docker/    Kafka testbed (KRaft) and topic setup
src/       (week 1–2) edge node, coordinator, algorithms, baselines
experiments/ (week 3) configs, runners, plots
```

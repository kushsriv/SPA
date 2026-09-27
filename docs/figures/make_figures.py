"""Generate the proposal diagrams (Graphviz + matplotlib) into docs/figures/."""
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parent
FONT = "DejaVu Sans"

EDGE = "#2563eb"
KAFKA = "#111827"
COORD = "#059669"
DECIDE = "#d97706"
MUTED = "#6b7280"


def dot(name: str, src: str) -> None:
    subprocess.run(
        ["dot", "-Tpng", "-Gdpi=200", "-o", str(OUT / f"{name}.png")],
        input=src.encode(),
        check=True,
    )


ARCH = f"""
digraph G {{
  rankdir=LR; bgcolor="white"; compound=true; nodesep=0.3; ranksep=0.9; newrank=true;
  node [fontname="{FONT}", fontsize=11, style="rounded,filled", shape=box, penwidth=1.2];
  edge [fontname="{FONT}", fontsize=10, color="{MUTED}"];

  subgraph cluster_edge {{
    label="Edge layer: non-IID local streams"; fontname="{FONT}"; fontsize=12; style="rounded,dashed"; color="{EDGE}";
    e1 [label="Edge node 1\\nlocal stream → micro-clusters\\n+ cost-aware trigger", fillcolor="#dbeafe", color="{EDGE}"];
    e2 [label="Edge node 2\\nlocal stream → micro-clusters\\n+ cost-aware trigger", fillcolor="#dbeafe", color="{EDGE}"];
    e3 [label="Edge node m\\nlocal stream → micro-clusters\\n+ cost-aware trigger", fillcolor="#dbeafe", color="{EDGE}"];
  }}

  subgraph cluster_kafka {{
    label="Apache Kafka (KRaft)"; fontname="{FONT}"; fontsize=12; style="rounded"; color="{KAFKA}";
    t3 [label="fsc.global\\nlatest global model (compacted)", fillcolor="#f3f4f6", color="{KAFKA}"];
    t1 [label="fsc.summaries\\nkey = node_id → per-node order", fillcolor="#f3f4f6", color="{KAFKA}"];
    t2 [label="fsc.snapshots\\nlatest full summary (compacted)", fillcolor="#f3f4f6", color="{KAFKA}"];
    t4 [label="fsc.metrics\\nbytes, lag, latency", fillcolor="#f3f4f6", color="{KAFKA}"];
  }}

  subgraph cluster_coord {{
    label="Coordinator (consumer group)"; fontname="{FONT}"; fontsize=12; style="rounded,dashed"; color="{COORD}";
    c1 [label="1. Apply deltas (per-node order)\\n2. Staleness decay + node weighting\\n3. Heterogeneity-aware macro-clustering\\n→ global model C(t)", fillcolor="#d1fae5", color="{COORD}"];
  }}

  e2 -> t1 [ltail=cluster_edge, label="deltas, only when worth the cost"];
  e3 -> t2 [ltail=cluster_edge, style=dashed, label="periodic snapshot"];
  e1 -> t4 [ltail=cluster_edge, style=dotted, label="telemetry"];
  t1 -> c1 [label="consume"];
  t2 -> c1 [style=dashed, label="recovery / replay"];
  c1 -> t3 [color="{COORD}", label="publish C(t)"];
  t3 -> e1 [lhead=cluster_edge, color="{COORD}", style=bold, label="broadcast centers C(t)"];
  {{rank=same; t3; t1; t2; t4}}
}}
"""

EDGE_FLOW = f"""
digraph G {{
  rankdir=TB; bgcolor="white"; nodesep=0.3; ranksep=0.35;
  node [fontname="{FONT}", fontsize=11, style="rounded,filled", shape=box, fillcolor="#dbeafe", color="{EDGE}", penwidth=1.2];
  edge [fontname="{FONT}", fontsize=10, color="{MUTED}"];

  start [label="New point x arrives from local stream", shape=box, style="rounded,filled,bold"];
  upd   [label="Update / create micro-cluster CF = (n, LS, SS, t)\\nwith time decay"];
  glob  [label="Read latest global centers C from fsc.global\\n(if a new version arrived)"];
  delta [label="Compute staleness score Δ\\n= cost drift + mass shift + novelty\\n(vs. last summary sent to server)"];
  price [label="Estimate link price p (latency, loss, cap)\\nand pending delta size b (bytes)"];
  dec   [label="Δ ≥ λ · p · b ?\\nor novelty override", shape=diamond, fillcolor="#fef3c7", color="{DECIDE}"];
  tok   [label="Token bucket has\\nbudget left?", shape=diamond, fillcolor="#fef3c7", color="{DECIDE}"];
  send  [label="Publish delta to fsc.summaries (key = node_id)\\nidempotent producer; update 'last sent' copy", fillcolor="#bbf7d0", color="{COORD}"];
  skip  [label="Skip sending (save bandwidth)", fillcolor="#f3f4f6", color="{MUTED}"];
  lam   [label="End of window: dual ascent\\nλ ← max(λmin, λ + η·(bytes − B)/B)"];

  start -> upd -> glob -> delta -> price -> dec;
  dec -> tok [label="yes"];
  dec -> skip [label="no"];
  tok -> send [label="yes"];
  tok -> skip [label="no"];
  send -> lam; skip -> lam;
  lam -> start [label="next point", style=dashed];
}}
"""

COORD_FLOW = f"""
digraph G {{
  rankdir=TB; bgcolor="white"; nodesep=0.3; ranksep=0.35;
  node [fontname="{FONT}", fontsize=11, style="rounded,filled", shape=box, fillcolor="#d1fae5", color="{COORD}", penwidth=1.2];
  edge [fontname="{FONT}", fontsize=10, color="{MUTED}"];

  boot  [label="Start / restart coordinator", style="rounded,filled,bold"];
  rec   [label="Rebuild state: read compacted fsc.snapshots\\nthen replay fsc.summaries from committed offsets"];
  poll  [label="Poll fsc.summaries (consumer group)"];
  apply [label="Apply delta to node i's stored summary\\n(in per-node order, dedup by sequence no.)"];
  when  [label="Re-aggregate?\\n(every M deltas or T seconds)", shape=diamond, fillcolor="#fef3c7", color="{DECIDE}"];
  decay [label="Decay all summaries to now (staleness weighting)\\n+ node-balanced weights w = n^β"];
  macro [label="Macro-clustering on micro-clusters\\n(weighted k-means++/Lloyd or weighted DBSCAN)\\n+ protect node-exclusive clusters"];
  pub   [label="Publish global model C(t) to fsc.global\\n(compacted) + metrics to fsc.metrics"];
  commit[label="Commit offsets"];

  boot -> rec -> poll -> apply -> when;
  when -> decay [label="yes"];
  when -> commit [label="no"];
  decay -> macro -> pub -> commit;
  commit -> poll [style=dashed, label="loop"];
}}
"""

RESEARCH_FLOW = f"""
digraph G {{
  rankdir=LR; bgcolor="white"; nodesep=0.25; ranksep=0.45;
  node [fontname="{FONT}", fontsize=11, style="rounded,filled", shape=box, penwidth=1.2];
  edge [color="{MUTED}"];
  a [label="Phase 0\\nResearch &\\nproblem framing", fillcolor="#bbf7d0", color="{COORD}"];
  b [label="Phase 1\\nMethod design\\n(FedCAST)", fillcolor="#dbeafe", color="{EDGE}"];
  c [label="Phase 2\\nKafka testbed\\n+ implementation", fillcolor="#dbeafe", color="{EDGE}"];
  d [label="Phase 3\\nExperiments\\n(RQ1–RQ5)", fillcolor="#dbeafe", color="{EDGE}"];
  e [label="Phase 4\\nPaper + artifact\\n→ submission", fillcolor="#dbeafe", color="{EDGE}"];
  a -> b -> c -> d -> e;
}}
"""


def gantt() -> None:
    tasks = [
        ("Literature review & proposal", 0, 3, "done"),
        ("Kafka testbed (KRaft)", 2, 2, "done"),
        ("Edge/coordinator skeleton + raw/naive baselines", 3, 5, "todo"),
        ("Data loaders + non-IID partitioners", 4, 4, "todo"),
        ("FedCAST: CF deltas, trigger, dual ascent", 7, 6, "todo"),
        ("Aggregation + baselines + network emulator", 9, 5, "todo"),
        ("Experiment runner, RQ1–RQ5 runs", 14, 7, "todo"),
        ("Proofs (Prop. 1 & 2), statistics, plots", 16, 6, "todo"),
        ("Paper writing (LaTeX) + artifact", 21, 7, "todo"),
        ("Internal review & submission", 27, 3, "todo"),
    ]
    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=200)
    for i, (name, start, dur, status) in enumerate(tasks):
        ax.barh(i, dur, left=start, height=0.6,
                color=COORD if status == "done" else EDGE, alpha=0.9)
    ax.set_yticks(range(len(tasks)), [t[0] for t in tasks], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 30)
    ax.set_xticks([0, 7, 14, 21, 28], ["Week 1", "Week 2", "Week 3", "Week 4", "Submit"], fontsize=9)
    for x in (7, 14, 21, 28):
        ax.axvline(x, color="#e5e7eb", lw=1, zorder=0)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.bar(0, 0, color=COORD, label="Done")
    ax.bar(0, 0, color=EDGE, label="Planned")
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    ax.set_title("Four-week plan", fontsize=11, loc="left")
    fig.tight_layout()
    fig.savefig(OUT / "roadmap_gantt.png")
    plt.close(fig)


if __name__ == "__main__":
    dot("architecture", ARCH)
    dot("edge_flowchart", EDGE_FLOW)
    dot("coordinator_flowchart", COORD_FLOW)
    dot("research_phases", RESEARCH_FLOW)
    gantt()
    print("figures written to", OUT)

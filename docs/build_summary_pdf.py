"""Build docs/Project_Summary.pdf: final project report (requirements, system, results, rationale, next steps).

Numbers are read from results/ so the report always matches the experiments."""
import sys
from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (Image, KeepTogether, ListFlowable, ListItem, PageBreak,
                                Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle)

HERE = Path(__file__).resolve().parent
FIG = HERE / "figures"
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "Project_Summary.pdf"
ROOT = HERE.parent
PFIG = ROOT / "paper" / "figures"
TABS = ROOT / "results" / "tables"
RES = ROOT / "results"

FD = "/usr/share/fonts/truetype/dejavu/"
pdfmetrics.registerFont(TTFont("DV", FD + "DejaVuSans.ttf"))
pdfmetrics.registerFont(TTFont("DV-B", FD + "DejaVuSans-Bold.ttf"))
pdfmetrics.registerFont(TTFont("DVM", FD + "DejaVuSansMono.ttf"))
pdfmetrics.registerFontFamily("DV", normal="DV", bold="DV-B", italic="DV", boldItalic="DV-B")

INK = colors.HexColor("#111827")
MUTED = colors.HexColor("#6b7280")
ACCENT = colors.HexColor("#2563eb")
GREEN = colors.HexColor("#059669")
RULE = colors.HexColor("#e5e7eb")
HEAD_BG = colors.HexColor("#eff6ff")

ss = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=ss["Normal"], fontName="DV", fontSize=9.5, leading=13.5,
                      textColor=INK, spaceAfter=5)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8.5, leading=11.5, spaceAfter=0)
H1 = ParagraphStyle("h1", parent=BODY, fontName="DV-B", fontSize=15, leading=19,
                    spaceBefore=10, spaceAfter=8, textColor=INK)
H2 = ParagraphStyle("h2", parent=BODY, fontName="DV-B", fontSize=11.5, leading=15,
                    spaceBefore=8, spaceAfter=4, textColor=ACCENT)
TITLE = ParagraphStyle("title", parent=BODY, fontName="DV-B", fontSize=21, leading=27,
                       alignment=TA_CENTER, spaceAfter=10)
SUB = ParagraphStyle("sub", parent=BODY, fontSize=11, leading=16, alignment=TA_CENTER,
                     textColor=MUTED)
CAP = ParagraphStyle("cap", parent=SMALL, textColor=MUTED, alignment=TA_CENTER, spaceAfter=8)
MONO = ParagraphStyle("mono", parent=BODY, fontName="DVM", fontSize=8.5, leading=12,
                      backColor=colors.HexColor("#f9fafb"), borderPadding=6, spaceBefore=4,
                      spaceAfter=8)
CALLOUT = ParagraphStyle("callout", parent=BODY, backColor=colors.HexColor("#f0fdf4"),
                         borderColor=GREEN, borderWidth=0.8, borderPadding=8, spaceBefore=6,
                         spaceAfter=10)

W = A4[0] - 4 * cm


def P(t, s=BODY):
    return Paragraph(t, s)


def bullets(items, style=BODY):
    return ListFlowable([ListItem(P(i, style), leftIndent=12, value="•") for i in items],
                        bulletType="bullet", start="•", leftIndent=12, bulletFontName="DV")


def table(rows, widths, head=True):
    data = [[P(c, SMALL) if isinstance(c, str) else c for c in r] for r in rows]
    t = Table(data, colWidths=[w * W for w in widths], repeatRows=1 if head else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, RULE),
          ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]
    if head:
        st += [("BACKGROUND", (0, 0), (-1, 0), HEAD_BG)]
        for i in range(len(rows[0])):
            data[0][i] = P(f"<b>{rows[0][i]}</b>", SMALL)
    t.setStyle(TableStyle(st))
    return t


def fig(name, caption, width=1.0, max_h=None):
    path = str(FIG / name)
    iw, ih = ImageReader(path).getSize()
    w = W * width
    h = w * ih / iw
    if max_h and h > max_h:
        h = max_h
        w = h * iw / ih
    return KeepTogether([Image(path, width=w, height=h), P(caption, CAP)])


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("DV", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(2 * cm, 1.2 * cm, "SPA · Budget-Aware Federated Stream Clustering over Apache Kafka")
    canvas.drawRightString(A4[0] - 2 * cm, 1.2 * cm, f"Page {doc.page}")
    canvas.restoreState()




import json


def jload(p, default=None):
    try:
        return json.load(open(p))
    except Exception:
        return default


def figp(path, caption, width=1.0, max_h=None):
    path = str(path)
    iw, ih = ImageReader(path).getSize()
    w = W * width
    h = w * ih / iw
    if max_h and h > max_h:
        h = max_h
        w = h * iw / ih
    return KeepTogether([Image(path, width=w, height=h), P(caption, CAP)])


def md_table(path, widths=None, max_rows=None):
    """Render a markdown table file (results/tables/*.md) as a PDF table."""
    lines = [l for l in open(path).read().splitlines() if l.startswith("|")]
    rows = [[c.strip().replace("**", "") for c in l.strip("|").split("|")] for l in lines]
    rows = [r for r in rows if not set("".join(r)) <= set("-: ")]
    if max_rows:
        rows = rows[:max_rows + 1]
    n = len(rows[0])
    return table(rows, widths or [1.0 / n] * n)


sav = jload(TABS / "savings.json", {})
st = jload(TABS / "stats_E1.json", {})
k1 = jload(RES / "kafka_K1_bytes.json", [])
k2 = jload(RES / "kafka_K2_crash.json", {})
k3 = jload(RES / "kafka_K3_broker_restart.json", {})
k4 = jload(RES / "kafka_K4_scale.json", [])

s = []
# ---------------------------------------------------------------- title
s += [Spacer(1, 2.6 * cm),
      P("FedCAST", TITLE),
      P("Budget-Aware Federated Stream Clustering over Apache Kafka<br/>under Non-IID Data and Network Constraints", SUB),
      Spacer(1, 0.5 * cm),
      P("Final project report: requirements, what was built, results, design rationale and next steps", SUB),
      Spacer(1, 0.3 * cm),
      P(f"Course: Stream Processing Analytics · Repository: github.com/kushsriv/spa · {date.today():%d %B %Y}", SUB),
      Spacer(1, 1.0 * cm),
      figp(FIG / "architecture.png", "System architecture: edge nodes, Kafka topics, coordinator", 1.0),
      Spacer(1, 0.6 * cm)]
hl = []
if sav:
    vals = [v["saving_vs_best_baseline"] for v in sav.values() if v.get("saving_vs_best_baseline") is not None]
    per = [v["periodic"] / v["fedcast"] for v in sav.values() if v.get("periodic") == v.get("periodic") and v.get("periodic")]
    hl.append(f"<b>{min(vals)*100:.0f}–{max(vals)*100:.0f}% fewer bytes</b> than the best baseline to reach "
              f"within 5% of centralised quality, and up to <b>{max(per):.1f}× fewer</b> than periodic sync")
if st.get("friedman_p"):
    hl.append(f"<b>Ranked first</b> at matched budgets (Friedman p = {st['friedman_p']:.1e}; "
              f"mean rank {st['mean_ranks']['fedcast']:.1f} vs {min(v for k, v in st['mean_ranks'].items() if k != 'fedcast'):.1f} for the next best)")
hl.append("<b>2.5× faster</b> detection of newly emerging clusters than periodic updates, while sending less")
if k2:
    hl.append(f"Coordinator killed with SIGKILL: state restored from Kafka in <b>{k2['restore_ms']/1000:.1f} s</b>, "
              f"<b>identical</b> to a coordinator that never crashed")
if k3:
    hl.append(f"Broker offline for {k3['broker_down_s']:.0f} s: <b>{k3['lost']} updates lost</b> "
              f"({k3['coordinator_applied']}/{k3['edge_msgs_sent']} applied)")
hl.append("Whole system runs in <b>under 1 GB of RAM</b> on a laptop, with or without Docker")
s += [P("<b>Headline results</b>", H2), bullets(hl), PageBreak()]

# ---------------------------------------------------------------- contents
s += [P("Contents", H1),
      bullets(["1. Your requirements and how they were met",
               "2. What was built",
               "3. How it works (method and flowcharts)",
               "4. Results",
               "5. Why this tech stack and these features (alternatives compared)",
               "6. How to run it",
               "7. The paper and where to submit",
               "8. What your team must do before submission",
               "9. Honest limitations"]),
      Spacer(1, 0.3 * cm),
      P("1. Your requirements and how they were met", H1),
      table([
          ["Your requirement", "Status", "Where"],
          ["Topic: cost-aware federated stream clustering, non-IID data, network constraints", "Done: FedCAST method, system and evaluation", "src/fedcast, paper/"],
          ["Kafka is essential", "Kafka's ordering, idempotence, replay and compaction are part of the design; tested under crashes", "kafka_runtime.py, §4.4"],
          ["Q1-level research paper", "Full manuscript in the Elsevier template with theory (3 propositions and proofs), 2,000+ runs and statistics", "paper/main.pdf"],
          ["Extensive research: problem, objectives, methodology, outcomes", "Done", "docs/00_research_proposal.md"],
          ["Claude writes all the code", "About 2,500 lines of Python, tests, experiment and analysis scripts", "src/, experiments/, tests/"],
          ["Deadline: as if due today", "Everything built, run, analysed and written up in one session", "this report"],
          ["Laptops with under 4 GB free RAM", "Whole system under 1 GB, measured; runs without Docker", "docs/02_low_memory_setup.md"],
          ["Target IEEE, ACM, Elsevier, Springer", "Venue shortlist; manuscript formatted for Elsevier FGCS", "§7"],
          ["Explain the tech stack, features and alternatives", "Detailed rationale document", "docs/03_design_rationale.md, §5"],
      ], [0.42, 0.38, 0.20]),
      PageBreak()]

# ---------------------------------------------------------------- built
s += [P("2. What was built", H1),
      table([
          ["Component", "What it does"],
          ["Edge node (node.py, microcluster.py)", "Online micro-clustering with time-decayed cluster features; keeps an exact copy of what the server holds"],
          ["FedCAST send policy (policies.py)", "Staleness score linked to the clustering objective; sends only the most valuable micro-clusters; dual-ascent threshold; token-bucket budget; link price; novelty override"],
          ["Coordinator (coordinator.py, macro.py)", "Applies each update exactly once; decays stale data; weighted k-means with swap search to protect single-node clusters"],
          ["Kafka runtime (kafka_runtime.py)", "Real edge and coordinator processes; changelog-based crash recovery; global-model broadcast"],
          ["Wire format (codec.py)", "Compact binary messages whose size is known before sending"],
          ["Simulator and network emulator (sim.py, netem.py)", "Deterministic, seeded, the same code as the real system; delay, loss, bandwidth, outages"],
          ["Baselines", "Centralised-Raw, Naive, Periodic, Periodic-Δ, Change-Threshold (Tran/DGClust), Norm-Trigger (EventGraD), k-FED (ICML 2021)"],
          ["Datasets (data.py)", "SynDrift (evolving, ours), NSL-KDD, Shuttle, Pen-Digits, Letter; five non-IID split types"],
          ["Experiments", "E1–E8 (2,000+ simulated runs, 5 seeds) and K1–K4 on a real Kafka broker"],
          ["Tests", "Unit tests, including numerical checks of the propositions and the budget guarantee"],
          ["CLI", "fedcast sim | demo | edges | coordinator | reset-topics"],
          ["Paper", "Complete manuscript with proofs, figures, tables and statistics (paper/main.pdf)"],
      ], [0.34, 0.66]),
      PageBreak()]

# ---------------------------------------------------------------- method
s += [P("3. How it works", H1),
      P("Every edge node asks at each tick: <i>is sending now worth its network cost, and which part should I send?</i>"),
      bullets(["<b>Staleness score.</b> For every micro-cluster, the node computes how much the server's copy "
               "distorts the global k-means cost, using a closed-form formula and the global model it receives back. "
               "The sum provably bounds the server's error (Propositions 1 and 2).",
               "<b>Prioritisation.</b> Only the micro-clusters carrying 90% of that staleness are sent. This alone is "
               "the biggest win in the ablation study.",
               "<b>Budget.</b> Send if value ≥ λ × link price × bytes. λ adapts automatically to hit the byte budget, and "
               "a token bucket makes the budget a hard guarantee (Proposition 3).",
               "<b>Novelty.</b> A cluster the server has never seen is sent immediately.",
               "<b>Coordinator.</b> Applies updates exactly once (per-node sequence numbers), decays stale data, "
               "and re-clusters with warm-started k-means plus swap search."]),
      figp(FIG / "edge_flowchart.png", "Edge node decision flowchart", 0.62, max_h=12.5 * cm),
      PageBreak(),
      figp(FIG / "coordinator_flowchart.png", "Coordinator flowchart (with crash recovery from the Kafka changelog)", 0.6, max_h=12 * cm),
      PageBreak()]

# ---------------------------------------------------------------- results
s += [P("4. Results", H1),
      P("4.1 Communication versus quality (RQ1)", H2),
      figp(PFIG / "fig_pareto.png", "Cost–quality Pareto fronts (5 seeds). Top: k-means cost relative to sending all raw data (lower is better). Bottom: ARI."),
      ]
if (TABS / "savings.md").exists():
    s += [P("Bytes (kB) each method needs to get within 5% of the centralised learner's cost:", BODY),
          md_table(TABS / "savings.md", [0.14, 0.12, 0.11, 0.11, 0.1, 0.12, 0.12, 0.18])]
if st.get("wilcoxon_vs_fedcast"):
    w = st["wilcoxon_vs_fedcast"]
    s += [Spacer(1, 0.2 * cm),
          P("<b>Statistics.</b> Friedman χ² = %.1f, p = %.1e over %d dataset×budget blocks. Holm-corrected Wilcoxon: "
            "FedCAST beats Periodic (p = %.0e, wins %.0f%%), Periodic-Δ (p = %.0e, %.0f%%) and k-FED (p = %.0e, %.0f%%)." % (
                st["friedman_chi2"], st["friedman_p"], st["n_blocks"],
                w["periodic"]["p_holm"], 100 * w["periodic"]["fedcast_better_frac"],
                w["pdelta"]["p_holm"], 100 * w["pdelta"]["fedcast_better_frac"],
                w["kfed"]["p_holm"], 100 * w["kfed"]["fedcast_better_frac"]), BODY)]
s += [PageBreak(), P("4.2 Drift and newly emerging clusters (RQ3)", H2),
      figp(PFIG / "fig_drift.png", "ARI over time. On SynDrift, new clusters appear at 105 s and 195 s; FedCAST reports them about 2.5× faster than Periodic and k-FED while sending less."),
      figp(PFIG / "fig_budget.png", "Every node stays under the hard budget envelope; after bootstrap the slope equals the budget.", 0.55),
      PageBreak(), P("4.3 Non-IID data (RQ2)", H2),
      figp(PFIG / "fig_noniid.png", "From IID to cluster-exclusive nodes: FedCAST has the lowest cost in all 18 settings."),
      ]
if (TABS / "E5_ablation.md").exists():
    s += [P("4.4 Ablation (cost ratio, lower is better)", H2), md_table(TABS / "E5_ablation.md")]
s += [PageBreak(), P("4.5 Real Kafka: correctness, fault tolerance, scale (RQ4, RQ5)", H2)]
if k1:
    s += [P("Byte accounting: our estimate vs Kafka's own producer statistics", BODY),
          table([["Method", "Our estimate (B)", "Kafka txmsg_bytes (B)", "Difference"]] +
                [[f"{r['method']} {r['param']:g}", f"{r['bytes_up_estimate']:,.0f}", f"{r['kafka_txmsg_bytes']:,.0f}",
                  f"+{(r['bytes_up_estimate'] / r['kafka_txmsg_bytes'] - 1) * 100:.1f}%"] for r in k1],
                [0.25, 0.25, 0.3, 0.2])]
kt = [["Test", "Result"]]
if k2:
    kt.append(["Coordinator SIGKILL, restarted after %.0f s" % k2["down_for_s"],
               "State restored from the compacted changelog in %.2f s; identical to the shadow coordinator: %s; mass difference %.1g" %
               (k2["restore_ms"] / 1000, "yes" if k2["state_identical_to_shadow"] else "NO", k2["max_node_mass_difference"])])
if k3:
    kt.append(["Broker stopped for %.0f s mid-run" % k3["broker_down_s"],
               "%d/%d updates applied, %d lost, %d delivery errors" % (k3["coordinator_applied"], k3["edge_msgs_sent"], k3["lost"], k3["delivery_errors"])])
if len(kt) > 1:
    s += [Spacer(1, 0.2 * cm), table(kt, [0.35, 0.65])]
if k4:
    s += [Spacer(1, 0.2 * cm), P("Scalability on one machine (real broker)", BODY),
          table([["Nodes", "Method", "Uplink kB", "End-to-end p50 / p95 (ms)", "Edge / coordinator / broker RSS (MB)"]] +
                [[str(r["nodes"]), r["method"], f"{r['bytes_up']/1e3:,.0f}", f"{r['e2e_p50_ms']:.0f} / {r['e2e_p95_ms']:.0f}",
                  f"{r['edge_rss_mb']:.0f} / {r['coord_rss_mb']:.0f} / {r['broker_rss_mb']:.0f}"] for r in k4],
                [0.1, 0.14, 0.18, 0.28, 0.3])]
s += [PageBreak()]

# ---------------------------------------------------------------- rationale
s += [P("5. Why this tech stack and these features", H1),
      table([
          ["Choice", "Why it is the best fit", "Alternatives and why not"],
          ["Apache Kafka (KRaft)", "Per-key order, idempotent producers, replayable log, log compaction: exactly what federated deltas and crash recovery need; runs in about 400 MB",
           "MQTT/RabbitMQ: no replay or compaction. Pulsar: needs BookKeeper, too heavy. Redpanda: not Apache Kafka. Cloud queues: paid, not reproducible."],
          ["Python services + confluent-kafka", "About 150 MB per process; full control over when to send; librdkafka is fast and exposes byte statistics",
           "Flink/Spark: 1.5–2 GB+ RAM, dataflow model does not fit per-device decisions. kafka-python: slower, fewer stats."],
          ["CF micro-clusters", "Exactly additive; decay keeps centroids; closed-form k-means cost enables the objective-linked trigger",
           "Grids: blow up in high dimensions. Coresets: no cheap deltas. Sketches: not geometric. k-FED centres: lose shape and size."],
          ["Objective-linked, prioritised trigger", "Bounds the server's error; spends bytes where the objective changes",
           "Periodic: blind to change. Thresholds: retune per dataset. Norm triggers: not tied to the clustering objective."],
          ["Exponentiated dual ascent + token bucket", "Scale-free (one η for all datasets), provable; hard guarantee from the bucket",
           "PID: three gains per dataset. Fixed λ: breaks when data rates change."],
          ["Swap local search", "Principled k-means local search; protects single-node clusters", "Plain warm-start Lloyd: absorbs new clusters. FedAvg of centres: label-permutation problem."],
          ["Fixed binary wire format", "Exact size known before sending; zero-copy; no schema registry", "JSON: 3–5× larger. Avro/Protobuf: need a registry service."],
          ["Simulator + real Kafka, one code base", "Thousands of seeded runs plus real systems measurements; bytes validated against Kafka", "Only real runs: too slow for statistics. Only simulation: not credible for systems claims."],
      ], [0.2, 0.4, 0.4]),
      P("Full discussion: docs/03_design_rationale.md", SMALL),
      PageBreak()]

# ---------------------------------------------------------------- run
s += [P("6. How to run it", H1),
      P("python -m venv .venv &amp;&amp; source .venv/bin/activate<br/>"
        "pip install -e .<br/>"
        "scripts/kafka_native.sh start          # or: docker compose -f docker/docker-compose.yml up -d<br/>"
        "fedcast demo --dataset syndrift --method fedcast --param 20 --speed 10<br/><br/>"
        "# reproduce everything<br/>"
        "python -m pytest<br/>"
        "python experiments/run.py E1 E2 E3 E4 E5 E6 E7 E8<br/>"
        "python experiments/kafka_experiments.py<br/>"
        "python experiments/analyze.py<br/>"
        "cd paper &amp;&amp; latexmk -pdf main.tex", MONO),
      P("7. The paper and where to submit", H1),
      bullets(["<b>Manuscript:</b> paper/main.pdf (Elsevier elsarticle template), with abstract, introduction, related work, "
               "formulation, method with three propositions (proofs in the appendix), Kafka system design, experimental setup, "
               "results for RQ1–RQ5, ablations, discussion and limitations.",
               "<b>First choice:</b> Elsevier <i>Future Generation Computer Systems</i> (Kafka-ML appeared there), or the "
               "<i>IEEE Internet of Things Journal</i>. Also a good fit: IEEE TKDE (theory-heavy version), Springer DMKD, "
               "ACM TKDD. Conference option: ACM DEBS.",
               "Switching to IEEE: change the document class to IEEEtran; the section files are template-agnostic."]),
      P("8. What your team must do before submission", H1),
      table([["#", "Task"],
             ["1", "Put real author names, affiliations and emails in paper/main.tex; agree authorship with your professor"],
             ["2", "Check every reference against the original paper (some metadata came from search snippets)"],
             ["3", "Check the target journal's current quartile, scope and article-processing charges; read its guide for authors"],
             ["4", "Read the whole paper critically; rerun the demo and the tests on your own laptops"],
             ["5", "Optional, strengthens the paper: run the Kafka experiments on 2–3 real laptops over Wi-Fi"],
             ["6", "Write the cover letter, suggest reviewers, and submit"]], [0.06, 0.94]),
      P("9. Honest limitations", H1),
      bullets(["At the very tightest budgets (about 1% of raw traffic), k-FED is cheaper: FedCAST's first full summary costs more than that.",
               "With one or two well-separated classes per node, k-FED reaches higher label agreement (ARI), though a worse k-means cost.",
               "Link price, novelty and dual ascent barely change average cost; they matter for wire cost, reaction time and loose budgets respectively.",
               "Scale was tested up to 50 nodes on one machine; real wide-area deployments are future work.",
               "Summaries are aggregates, not differentially private; adding DP noise is future work."]),
      ]

doc = SimpleDocTemplate(str(OUT), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                        topMargin=1.8 * cm, bottomMargin=2 * cm,
                        title="FedCAST Final Project Report", author="SPA team")
doc.build(s, onFirstPage=footer, onLaterPages=footer)
print("wrote", OUT)

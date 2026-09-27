"""Build docs/Project_Summary.pdf: requirements, research plan, flowcharts, progress."""
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


s = []

# ---------------------------------------------------------------- title
s += [Spacer(1, 3.5 * cm),
      P("Budget-Aware Federated Stream Clustering over Apache Kafka", TITLE),
      P("under Non-IID Data Distribution and Network Constraints", SUB),
      Spacer(1, 0.6 * cm),
      P("Project summary: requirements, research plan, architecture and progress", SUB),
      Spacer(1, 0.3 * cm),
      P(f"Course: Stream Processing Analytics · Repository: github.com/kushsriv/spa · {date.today():%d %B %Y}", SUB),
      Spacer(1, 1.2 * cm),
      fig("research_phases.png", "Project phases, from research to submission", 0.95),
      Spacer(1, 0.8 * cm),
      P("<b>Status:</b> Phase 0 (research and problem framing) is complete and the Kafka testbed is "
        "running. The method, proofs and results described here are a <b>plan</b>. Nothing has been "
        "implemented or measured beyond the Kafka setup yet.", CALLOUT),
      PageBreak()]

# ---------------------------------------------------------------- contents
s += [P("Contents", H1),
      bullets(["1. Your requirements (what you asked for)",
               "2. Work done so far",
               "3. Research summary and gap",
               "4. Problem statement and research questions",
               "5. Objectives",
               "6. Methodology, with architecture and flowcharts",
               "7. Experimental plan",
               "8. Expected outcomes",
               "9. Target venues",
               "10. Roadmap and timeline",
               "11. What your team needs to do next",
               "12. Key references"]),
      Spacer(1, 0.4 * cm)]

# ---------------------------------------------------------------- 1 requirements
s += [P("1. Your requirements", H1),
      P("These are the requirements you gave me across our conversation, collected in one place."),
      table([
          ["Requirement", "What you said", "How the plan meets it"],
          ["Course", "Stream Processing Analytics", "Stream-native design: online micro-clusters, "
           "Kafka topics, latency and consumer-lag metrics"],
          ["Topic", "Cost-Aware Federated Stream Clustering under Non-IID Data Distribution and "
           "Network Constraints", "Kept; sharpened into a budget-constrained problem with a new method "
           "(working name FedCAST)"],
          ["Kafka", "Using Kafka is essential; the most important part", "Kafka is part of the algorithm, "
           "not just a pipe: keyed ordering, log compaction, offset replay for recovery, global-model "
           "broadcast, idempotent producers"],
          ["Paper quality", "Q1-level or above research paper", "Theory (two propositions), a real system, "
           "5 research questions, 6+ baselines, 5 datasets, statistical tests"],
          ["Research first", "Extensive research: problem statement, objectives, methodology, expected "
           "outcomes", "Done: docs/00_research_proposal.md and this PDF"],
          ["Coding", "Claude writes all the code", "Claude implements everything; the team reviews, runs "
           "experiments and checks references"],
          ["Timeline", "About one month", "Four-week plan with decision gates (Section 10)"],
          ["Hardware", "Laptops only", "Single-broker Kafka in KRaft mode with a 1 GB heap; Python edge "
           "nodes; no Flink; everything in about 8 GB of RAM"],
          ["Venues", "IEEE, ACM, Elsevier and Springer are all targets", "Shortlist across all four "
           "publishers (Section 9)"],
          ["Repository", "New repo called spa", "github.com/kushsriv/spa; Phase 0 pushed to main"],
      ], [0.16, 0.37, 0.47]),
      PageBreak()]

# ---------------------------------------------------------------- 2 done so far
s += [P("2. Work done so far", H1),
      table([
          ["#", "Item", "Status", "Where"],
          ["1", "Literature search: distributed stream clustering, federated clustering, federated "
           "learning on streams, event-triggered communication, Kafka for ML, private stream clustering",
           "Done", "Proposal §2"],
          ["2", "Gap analysis against the closest prior work (Cormode 2007, DGClust 2011, Tran 2013, "
           "k-FED 2021, AFCL 2025, FedCluLearn 2025, DFAS 2025, Kafka-ML)", "Done", "Proposal §2.8"],
          ["3", "Problem statement, 5 research questions, 6 objectives", "Done", "Proposal §3–4"],
          ["4", "Method design: staleness score, budget-aware send rule, aggregation, Kafka protocol",
           "Designed, not coded", "Proposal §5"],
          ["5", "Experimental design: datasets, non-IID schemes, baselines, metrics, statistics",
           "Done", "Proposal §6"],
          ["6", "Target venue shortlist and a four-week roadmap", "Done", "Proposal §9, 01_roadmap.md"],
          ["7", "Kafka testbed: Docker Compose, KRaft single broker, 5 topics (2 log-compacted)",
           "Done and tested", "docker/docker-compose.yml"],
          ["8", "Tested: Kafka started, topics created, idempotent produce and consume round trip "
           "succeeded from Python", "Verified", "In this session"],
          ["9", "Architecture diagram, edge and coordinator flowcharts, Gantt chart",
           "Done", "docs/figures/"],
          ["10", "Pushed to github.com/kushsriv/spa (branch main)", "Done", "Git"],
      ], [0.05, 0.6, 0.15, 0.2]),
      Spacer(1, 0.3 * cm),
      P("<b>Not done yet:</b> any clustering code, the baselines, the experiments, the proofs and the "
        "paper itself. Some references were taken from search results because Springer and arXiv pages "
        "were blocked in my environment; they are flagged for your team to check.", CALLOUT),
      PageBreak()]

# ---------------------------------------------------------------- 3 research
s += [P("3. Research summary and gap", H1),
      P("Edge devices produce fast, unlabeled streams. Each device sees a different slice of the data "
        "(non-IID), over slow or unreliable links. We want one continuously updated global clustering, "
        "without raw data leaving any device, and within a byte budget per device."),
      table([
          ["Line of work", "Examples", "What is missing for our setting"],
          ["Stream clustering (local building block)", "CluStream (2003), DenStream (2006), "
           "DBSTREAM (2016)", "Single site only; but their micro-cluster summaries (CF vectors) are "
           "additive and cheap to send, so we reuse them"],
          ["Distributed stream clustering", "Cormode et al., ICDE 2007; DGClust, 2011; "
           "Tran, 2013", "Fixed thresholds or periods; no byte budget; no link awareness; ignore non-IID "
           "data; no real messaging system"],
          ["Federated clustering", "k-FED (ICML 2021), FFCM (2022), AFCL (AAAI 2025)",
           "Static datasets only: no streams, no drift"],
          ["Federated learning on streams", "FedCluLearn (ECML-PKDD 2025), DFAS (WSDM 2025), "
           "Silva et al. (2022)", "Supervised learning or anomaly detection, not clustering the data "
           "under a budget"],
          ["Event-triggered communication", "EventGraD (2021), decentralized event-triggered FL (2022)",
           "Trigger on weight or gradient change, not on clustering quality; no hard budget guarantee"],
          ["Kafka for ML", "Kafka-ML (FGCS 2022), FL in Kafka-ML (2024)", "Neural-network training; "
           "Kafka is a transport, not part of the algorithm's recovery story"],
      ], [0.24, 0.3, 0.46]),
      Spacer(1, 0.3 * cm),
      P("<b>Gap:</b> no existing method keeps a continuously updated global clustering from non-IID "
        "edge streams while provably respecting per-node communication budgets under changing network "
        "conditions, using a send trigger tied to the clustering objective, and validated on a real "
        "fault-tolerant messaging system.", CALLOUT),
      PageBreak()]

# ---------------------------------------------------------------- 4 problem
s += [P("4. Problem statement and research questions", H1),
      P("<b>Setting.</b> There are m edge nodes. Node i sees a stream from its own changing distribution "
        "P<sub>i</sub>(t), and different nodes see different distributions (some clusters may exist at only "
        "one node). Node i keeps micro-cluster summaries CF = (n, LS, SS, t) with time decay, has a link "
        "with a changing price p<sub>i</sub>(t) (latency, loss, metered bytes), and a byte budget "
        "B<sub>i</sub> per time window. The coordinator only holds the summaries each node last sent, "
        "and from them builds global cluster centres C(t)."),
      P("<b>Objective.</b> Choose when each node sends so that the global clustering stays as close as "
        "possible to what a central learner with all raw data would get, while every node stays within "
        "its budget and raw data never leaves a node."),
      P("minimise   (1/T) Σ<sub>t</sub> [ J(C(t)) − J(C*(t)) ]<br/>"
        "subject to (1/T) Σ<sub>t</sub> bytes<sub>i</sub>(t) ≤ B<sub>i</sub> / W for every node i", MONO),
      P("Research questions", H2),
      table([
          ["RQ", "Question"],
          ["RQ1 Cost vs accuracy", "How many bytes does a budget-aware, quality-linked trigger save "
           "compared with periodic, change-based and norm-based triggers, at equal clustering quality?"],
          ["RQ2 Non-IID", "Does heterogeneity-aware aggregation keep clusters that only one node sees? "
           "How does quality change with the degree of skew?"],
          ["RQ3 Drift", "How fast does the global model recover after sudden, gradual or local-only "
           "drift, under a fixed budget?"],
          ["RQ4 Network", "How robust is the system to latency, loss, bandwidth caps and node crashes, "
           "and what do Kafka's retention and replay contribute?"],
          ["RQ5 Scale", "How do throughput, end-to-end latency and consumer lag grow with the number of "
           "nodes and the stream rate on laptop hardware?"],
      ], [0.22, 0.78]),
      P("5. Objectives", H1),
      table([
          ["#", "Objective"],
          ["O1", "Design a staleness score, computed locally from micro-cluster summaries, that measures "
           "how much the server's copy of a node hurts the global clustering cost."],
          ["O2", "Design a send rule that meets a byte budget per node, adapts to link conditions, and "
           "comes with a proof of budget satisfaction and a bound on the server's cost error."],
          ["O3", "Design aggregation that down-weights stale data and protects minority and "
           "single-node clusters."],
          ["O4", "Build an open-source, Kafka-native, reproducible testbed that runs on a laptop, with "
           "network-fault injection."],
          ["O5", "Evaluate against at least 6 baselines, on at least 4 datasets and at least 3 non-IID "
           "schemes, with at least 5 random seeds and significance tests."],
          ["O6", "Produce a submission-ready manuscript and a reproducibility package."],
      ], [0.07, 0.93]),
      PageBreak()]

# ---------------------------------------------------------------- 6 method
s += [P("6. Methodology", H1),
      P("6.1 System architecture", H2),
      P("Edge nodes cluster their own streams and publish small deltas to Kafka only when worthwhile. "
        "The coordinator consumes them, builds the global model and publishes it back on a compacted "
        "topic, which every edge node reads to decide when its next update is worth sending."),
      fig("architecture.png", "Figure 1. System architecture: edge layer, Kafka topics and coordinator"),
      table([
          ["Kafka topic", "Role", "Kafka feature used"],
          ["fsc.summaries", "Delta updates from each node", "Key = node_id gives per-node ordering; "
           "idempotent producer prevents duplicates"],
          ["fsc.snapshots", "Latest full summary per node", "Log compaction keeps the latest per key "
           "indefinitely, so a crashed coordinator can rebuild its state"],
          ["fsc.global", "Global cluster centres, broadcast to edges", "Log compaction keeps only the "
           "latest model"],
          ["fsc.metrics", "Bytes, lag, latency telemetry", "Used for the evaluation"],
          ["fsc.raw", "Raw points for the centralised baseline only", "Upper bound on quality and bytes"],
      ], [0.2, 0.35, 0.45]),
      PageBreak(),
      P("6.2 Edge node: when to send (the cost-aware filter)", H2),
      P("For each micro-cluster, the k-means cost of assigning it to a centre c has a closed form, "
        "SS − 2·LS·c + n·‖c‖², so a node can compute exactly how much its unsent changes would move the "
        "global clustering cost, without sending any data."),
      P("Δ = Σ<sub>j</sub> |cost drift in global cluster j| + ρ · (mass shifted between clusters) "
        "+ κ · (mass far from every global centre)<br/>"
        "send if  Δ ≥ λ · p · b      (b = delta size in bytes, p = link price)<br/>"
        "per window: λ ← max(λ<sub>min</sub>, λ + η · (bytes sent − B) / B)", MONO),
      P("The last term in Δ is a novelty signal: a new cluster seen only at this node is sent quickly, "
        "which is the key non-IID case. The multiplier λ rises when a node overspends and falls when it "
        "underspends, so the node tracks its budget automatically. A token bucket enforces the budget "
        "exactly during bursts."),
      fig("edge_flowchart.png", "Figure 2. Edge node decision flowchart", 0.78, max_h=15 * cm),
      PageBreak(),
      P("6.3 Coordinator: aggregation and recovery", H2),
      bullets(["<b>Staleness weighting:</b> every stored summary is decayed to the current time, so "
               "nodes that stopped sending fade out instead of dominating.",
               "<b>Node balancing:</b> weight w = n<super>β</super> so one fast node cannot drown out "
               "slow ones.",
               "<b>Macro-clustering:</b> weighted k-means++ and Lloyd over micro-clusters, seeded from "
               "per-node centres (following k-FED); or weighted DBSCAN when the number of clusters is "
               "unknown.",
               "<b>Exclusive-cluster protection:</b> a dense group supported by a single node and far "
               "from every centre stays its own cluster.",
               "<b>Recovery:</b> after a crash the coordinator reads the compacted snapshots, then "
               "replays deltas from its committed offsets. No summary is lost."]),
      fig("coordinator_flowchart.png", "Figure 3. Coordinator flowchart", 0.7, max_h=14 * cm),
      P("6.4 Theory (to be proven in week 3)", H2),
      bullets(["<b>Proposition 1 (staleness bound):</b> if each node keeps Δ<sub>i</sub> ≤ "
               "τ<sub>i</sub>, the server's estimate of the global cost is off by at most "
               "Σ<sub>i</sub> τ<sub>i</sub>.",
               "<b>Proposition 2 (budget):</b> each node's long-run average bytes stay within "
               "B<sub>i</sub>/W plus a term that shrinks as 1/(ηT).",
               "Together: budget → λ → threshold → bounded error, an explicit cost–accuracy trade-off."]),
      PageBreak()]

# ---------------------------------------------------------------- 7 experiments
s += [P("7. Experimental plan", H1),
      P("Datasets (labels used only for evaluation)", H2),
      table([
          ["Dataset", "Why"],
          ["Synthetic Gaussian / RBF streams with drift", "Ground truth; full control of drift and skew"],
          ["KDD Cup 99 / NSL-KDD", "Classic stream-clustering benchmark (known to be dated, so not used alone)"],
          ["CIC-IDS2017 or UNSW-NB15", "Modern network traffic; natural edge-network story"],
          ["Forest Covertype", "Standard stream benchmark with 7 classes"],
          ["Intel Lab sensors or Electricity (ELEC2)", "Real IoT drift with natural per-sensor splits"],
      ], [0.4, 0.6]),
      P("Non-IID splits", H2),
      bullets(["Label skew with Dirichlet(α), α ∈ {0.1, 0.5, 1, 100}",
               "Cluster-exclusive nodes: each node sees k' ∈ {1, 2, √k, k} clusters",
               "Quantity skew (power-law rates), drift at only some nodes, and natural per-sensor splits"]),
      P("Baselines", H2),
      bullets(["Centralised-Raw (all points sent; best quality, most bytes)",
               "Naive-Federated (full summary on every update); Periodic-T (every T seconds)",
               "Change-Threshold (Tran 2013 / DGClust style); Norm-Event-Triggered (EventGraD style)",
               "k-FED re-run periodically; plus ablations removing each part of FedCAST"]),
      P("Metrics", H2),
      table([
          ["Category", "Metrics"],
          ["Quality", "ARI, NMI, purity, SSQ ratio to centralised, CMM, recall of minority and "
           "single-node clusters"],
          ["Cost", "Bytes on the wire, messages sent, bytes as a share of Centralised-Raw"],
          ["Timeliness", "End-to-end latency, Kafka consumer lag, drift-recovery time"],
          ["System", "Throughput per node, coordinator CPU and memory, crash-recovery time"],
          ["Statistics", "5–10 seeds, 95% confidence intervals, Friedman + Nemenyi, Wilcoxon"],
      ], [0.2, 0.8]),
      P("8. Expected outcomes", H1),
      bullets(["<b>Contributions:</b> a new problem formulation, the FedCAST algorithm, two theoretical "
               "results, an open-source Kafka-native system, and an extensive evaluation.",
               "<b>H1:</b> at least 50% fewer bytes than Periodic and Change-Threshold at the same ARI "
               "(within 2%).",
               "<b>H2:</b> better recall of single-node clusters than naive aggregation for α ≤ 0.5.",
               "<b>H3:</b> faster recovery from drift than Periodic at the same budget.",
               "<b>H4:</b> no lost summaries and bounded recovery time after node or coordinator crashes.",
               "These are hypotheses to test. They will be reported honestly whether or not they hold."]),
      PageBreak()]

# ---------------------------------------------------------------- 9 venues
s += [P("9. Target venues", H1),
      P("All journals below accept submissions year-round; review usually takes 2–6 months. Check "
        "current quartiles on SJR or JCR before submitting."),
      table([
          ["Publisher", "Venue", "Fit"],
          ["IEEE", "IEEE Internet of Things Journal", "Edge, IoT and federated systems. Strong fit."],
          ["IEEE", "IEEE Trans. on Knowledge and Data Engineering", "Stream mining with theory; highest bar"],
          ["IEEE", "IEEE Trans. on Parallel and Distributed Systems", "If the systems part dominates"],
          ["Elsevier", "Future Generation Computer Systems", "Kafka-ML was published here. Very good fit."],
          ["Elsevier", "Information Sciences; Knowledge-Based Systems", "Algorithm focus"],
          ["Springer", "Data Mining and Knowledge Discovery; Machine Learning", "Stream-clustering community"],
          ["Springer", "Journal of Big Data; Cluster Computing", "Faster review; fallback"],
          ["ACM", "ACM Trans. on Knowledge Discovery from Data", "Stream mining"],
          ["ACM", "ACM Trans. on Internet of Things", "Edge systems"],
          ["Conferences", "ACM DEBS, IEEE BigData, ECML-PKDD, PAKDD, CIKM, ICDM, ICDE",
           "DEBS is the most natural home for Kafka work; check current deadlines"],
      ], [0.14, 0.46, 0.4]),
      P("<b>Recommendation:</b> first submission to Elsevier FGCS or the IEEE Internet of Things Journal; "
        "keep IEEE TKDE for a stronger version if the theory is clean.", CALLOUT),
      PageBreak(),
      P("10. Roadmap and timeline", H1),
      fig("roadmap_gantt.png", "Figure 4. Four-week plan (green = done, blue = planned)"),
      table([
          ["Week", "Deliverables"],
          ["1", "Research and proposal ✓; Kafka testbed ✓; edge and coordinator skeleton; raw and "
           "naive baselines; data loaders and non-IID splitters"],
          ["2", "FedCAST core: summary deltas, staleness score, budget controller, aggregation; other "
           "baselines; network-fault injection; unit tests"],
          ["3", "Experiment runner; RQ1–RQ5 runs; crash tests; plots; statistics; proofs"],
          ["4", "Paper in the venue's LaTeX template; reproducibility package; review; submission"],
      ], [0.1, 0.9]),
      P("<b>Decision gates:</b> end of week 1, your team approves the method; end of week 2, a pilot "
        "on synthetic data must beat Periodic and Change-Threshold, otherwise we redesign the trigger "
        "before scaling up; end of week 3, freeze results and pick the venue.", BODY),
      PageBreak()]

# ---------------------------------------------------------------- 11 next
s += [P("11. What your team needs to do next", H1),
      table([
          ["#", "Task", "Who"],
          ["1", "Read docs/00_research_proposal.md §3–5 and approve or change the direction", "Team"],
          ["2", "Check every reference against the original paper (some came from search snippets)", "Team"],
          ["3", "Check journal quartiles on SJR / JCR", "Team"],
          ["4", "Download datasets that need registration (CIC-IDS2017 / UNSW-NB15) if needed", "Team"],
          ["5", "Clear the topic and target venue with your professor", "Team"],
          ["6", "Say \"go\" so Claude starts week-1 coding: edge node, coordinator, baselines, data "
           "loaders", "Team → Claude"],
          ["7", "Run long experiments on your laptops in week 3 (one command per experiment)", "Team"],
      ], [0.06, 0.76, 0.18]),
      P("How to run the Kafka testbed now", H2),
      P("docker compose -f docker/docker-compose.yml up -d<br/>"
        "python -m venv .venv &amp;&amp; source .venv/bin/activate<br/>"
        "pip install -r requirements.txt", MONO),
      P("12. Key references", H1)]
refs = [
    "Aggarwal et al. A framework for clustering evolving data streams (CluStream). VLDB 2003.",
    "Cao et al. Density-based clustering over an evolving data stream with noise (DenStream). SDM 2006.",
    "Hahsler &amp; Bolaños. Clustering data streams based on shared density between micro-clusters. IEEE TKDE 2016.",
    "Cormode, Muthukrishnan &amp; Zhuang. Conquering the divide: continuous clustering of distributed data streams. ICDE 2007.",
    "Gama, Rodrigues &amp; Lopes. Clustering distributed sensor data streams using local processing and reduced communication. Intelligent Data Analysis 2011.",
    "Tran. Communication-efficient exact clustering of distributed streaming data. ICCSA 2013 (arXiv:1209.4257).",
    "Balcan, Ehrlich &amp; Liang. Distributed k-means and k-median clustering on general topologies. NeurIPS 2013.",
    "Dennis, Li &amp; Smith. Heterogeneity for the win: one-shot federated clustering. ICML 2021.",
    "Stallmann &amp; Wilbik. Towards federated clustering: a federated fuzzy c-means algorithm. arXiv:2201.07316.",
    "Zhang et al. Asynchronous federated clustering with unknown number of clusters. AAAI 2025.",
    "Angelova et al. FedCluLearn: federated continual learning using stream micro-cluster indexing. ECML-PKDD 2025.",
    "Silva, Vinagre &amp; Gama. Federated anomaly detection over distributed data streams. arXiv:2205.07829.",
    "Li et al. Density-aware and cluster-based federated anomaly detection on data streams. WSDM 2025.",
    "EventGraD: event-triggered communication in parallel machine learning. arXiv:2103.07454.",
    "Decentralized event-triggered federated learning with heterogeneous communication thresholds. arXiv:2204.03726.",
    "Martín et al. Kafka-ML: connecting the data stream with ML/AI frameworks. FGCS 2022.",
    "Towards flexible data stream collaboration: federated learning in Kafka-ML. Internet of Things, 2024.",
    "Epasto et al. Differentially private clustering in data streams. arXiv:2307.07449.",
    "Neely. Stochastic Network Optimization with Application to Communication and Queueing Systems. 2010.",
    "Montiel et al. River: machine learning for streaming data in Python. JMLR 2021.",
]
s.append(ListFlowable([ListItem(P(r, SMALL), leftIndent=16) for r in refs], bulletType="1",
                      leftIndent=16, bulletFontName="DV", bulletFontSize=8.5))

doc = SimpleDocTemplate(str(OUT), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                        topMargin=1.8 * cm, bottomMargin=2 * cm,
                        title="SPA Project Summary", author="SPA team")
doc.build(s, onFirstPage=footer, onLaterPages=footer)
print("wrote", OUT)

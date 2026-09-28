"""Turn results/*.jsonl into the paper's figures and tables.

  python experiments/analyze.py            -> paper/figures/*.pdf|png, results/tables/*.tex|md|json
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker  # noqa: F401
import numpy as np
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
FIG = ROOT / "paper" / "figures"
TAB = RES / "tables"
FIG.mkdir(parents=True, exist_ok=True)
TAB.mkdir(parents=True, exist_ok=True)

DS_ORDER = ["syndrift", "nslkdd", "shuttle", "pendigits", "letter"]
DS_LABEL = {"syndrift": "SynDrift", "nslkdd": "NSL-KDD", "shuttle": "Shuttle",
            "pendigits": "Pen-Digits", "letter": "Letter"}
METHODS = ["fedcast", "periodic", "pdelta", "kfed", "change", "norm", "naive", "raw"]
M_LABEL = {"fedcast": "FedCAST (ours)", "periodic": "Periodic", "pdelta": "Periodic-Δ",
           "kfed": "k-FED", "change": "Change-Thr.", "norm": "Norm-Trigger", "naive": "Naive",
           "raw": "Centralised-Raw"}
COLOR = {"fedcast": "#d62728", "periodic": "#1f77b4", "pdelta": "#17becf", "kfed": "#2ca02c",
         "change": "#9467bd", "norm": "#8c564b", "naive": "#7f7f7f", "raw": "#000000"}
MARK = {"fedcast": "o", "periodic": "s", "pdelta": "D", "kfed": "^", "change": "v", "norm": "P",
        "naive": "x", "raw": "*"}

plt.rcParams.update({"font.size": 8, "axes.titlesize": 9, "axes.labelsize": 8, "legend.fontsize": 7,
                     "figure.dpi": 150, "savefig.bbox": "tight", "axes.spines.top": False,
                     "axes.spines.right": False})


def load(name):
    p = RES / f"{name}.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.open()]


def savefig(fig, name):
    fig.savefig(FIG / f"{name}.pdf")
    fig.savefig(FIG / f"{name}.png", dpi=200)
    plt.close(fig)


def raw_ssq(rows):
    """Centralised-Raw SSQ per (dataset, partition, links, seed) for the cost ratio."""
    ref = {}
    for r in rows:
        c = r["config"]
        if c["method"] == "raw":
            ref[(r["dataset"], c["partition"], tuple(c["links"]), c["outage_nodes"], c["nodes"], c["seed"])] = r["ssq"]
    return ref


def ratio(r, ref):
    c = r["config"]
    base = ref.get((r["dataset"], c["partition"], tuple(c["links"]), c["outage_nodes"], c["nodes"], c["seed"]))
    return r["ssq"] / base if base else float("nan")


def mean_ci(x):
    x = np.asarray([v for v in x if not (isinstance(v, float) and math.isnan(v))], float)
    if len(x) == 0:
        return float("nan"), float("nan")
    if len(x) == 1:
        return float(x[0]), 0.0
    h = stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / math.sqrt(len(x))
    return float(x.mean()), float(h)


# ======================================================================= E1
def e1():
    rows = load("E1")
    if not rows:
        return
    ref = raw_ssq(rows)
    agg = defaultdict(list)  # (ds, method, param) -> list of (bytes, ari, ratio, nmi)
    for r in rows:
        c = r["config"]
        agg[(r["dataset"], c["method"], c["param"])].append(
            (r["bytes_up"], r["ari"], ratio(r, ref), r["nmi"], c["seed"]))
    dsets = [d for d in DS_ORDER if any(k[0] == d for k in agg)]
    fig, axes = plt.subplots(2, len(dsets), figsize=(2.3 * len(dsets), 4.2))
    axes = np.atleast_2d(axes).reshape(2, -1)
    for j, ds in enumerate(dsets):
        for m in METHODS:
            pts = sorted([(np.mean([v[0] for v in vals]), np.mean([v[2] for v in vals]), np.mean([v[1] for v in vals]))
                          for (d, mm, p), vals in agg.items() if d == ds and mm == m])
            if not pts:
                continue
            x = np.array([p[0] for p in pts]) / 1e3
            kw = dict(color=COLOR[m], marker=MARK[m], ms=3.5 if m != "raw" else 6, lw=1.4 if m == "fedcast" else 0.9,
                      label=M_LABEL[m], zorder=5 if m == "fedcast" else 2)
            axes[0, j].plot(x, np.minimum([p[1] for p in pts], 1.35), **kw)
            axes[1, j].plot(x, [p[2] for p in pts], **kw)
        axes[0, j].set_title(DS_LABEL[ds])
        for i in range(2):
            axes[i, j].set_xscale("log")
            axes[i, j].grid(alpha=0.25, lw=0.5)
        axes[0, j].set_ylim(0.99, None)
        axes[1, j].set_xlabel("uplink bytes (kB, log)")
    axes[0, 0].set_ylabel("k-means cost / Centralised-Raw\n(lower is better; clipped at 1.35)")
    axes[1, 0].set_ylabel("ARI (higher is better)")
    h, lab = axes[0, 0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=8, frameon=False, bbox_to_anchor=(0.5, -0.03))
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    savefig(fig, "fig_pareto")

    # ---------- budget-matched comparison: interpolate each method's curve per seed
    levels = {"low": 0.01, "mid": 0.03, "high": 0.10}   # fraction of Centralised-Raw bytes
    raw_bytes = {ds: np.mean([v[0] for (d, m, p), vals in agg.items() if d == ds and m == "raw" for v in vals])
                 for ds in dsets}
    fams = ["fedcast", "periodic", "pdelta", "kfed", "change", "norm"]
    table = {}   # (ds, level, method) -> list over seeds of metric (nan = infeasible)
    for ds in dsets:
        seeds = sorted({v[4] for (d, m, p), vals in agg.items() if d == ds for v in vals})
        for lev, frac in levels.items():
            target = frac * raw_bytes[ds]
            for m in fams:
                for s in seeds:
                    pts = sorted([(v[0], v[2], v[1]) for (d, mm, p), vals in agg.items()
                                  if d == ds and mm == m for v in vals if v[4] == s])
                    if len(pts) < 1:
                        continue
                    b = np.array([p[0] for p in pts])
                    if target < b.min() * 0.999:
                        val = (float("nan"), float("nan"))   # cannot operate within this budget
                    else:
                        # best quality achievable within the budget (monotone envelope)
                        ok = [p for p in pts if p[0] <= target * 1.0001]
                        # log-linear interpolation to the budget when a point above exists
                        above = [p for p in pts if p[0] > target]
                        best_r = min(p[1] for p in ok)
                        best_a = max(p[2] for p in ok)
                        if above:
                            lo = max(ok, key=lambda p: p[0])
                            hi = min(above, key=lambda p: p[0])
                            w = (math.log(target) - math.log(lo[0])) / max(math.log(hi[0]) - math.log(lo[0]), 1e-12)
                            best_r = min(best_r, lo[1] + w * (hi[1] - lo[1]))
                            best_a = max(best_a, lo[2] + w * (hi[2] - lo[2]))
                        val = (best_r, best_a)
                    table.setdefault((ds, lev, m), []).append(val)
    # markdown + latex table (cost ratio, mean over seeds; "--" infeasible)
    md = ["| Dataset | Budget | " + " | ".join(M_LABEL[m] for m in fams) + " |",
          "|---|---|" + "---|" * len(fams)]
    tex = []
    summary = {}
    for ds in dsets:
        for lev in levels:
            cells_md, cells_tex = [], []
            vals = {}
            for m in fams:
                v = table.get((ds, lev, m), [])
                rr = [x[0] for x in v]
                vals[m] = np.nanmean(rr) if rr and not all(math.isnan(x) for x in rr) else float("nan")
            finite = {m: x for m, x in vals.items() if not math.isnan(x)}
            best = min(finite.values()) if finite else None
            for m in fams:
                x = vals[m]
                if math.isnan(x):
                    cells_md.append("—")
                    cells_tex.append("--")
                else:
                    s = f"{x:.3f}"
                    cells_md.append(f"**{s}**" if best is not None and abs(x - best) < 1e-9 else s)
                    cells_tex.append(f"\\textbf{{{s}}}" if best is not None and abs(x - best) < 1e-9 else s)
            summary[f"{ds}|{lev}"] = vals
            md.append(f"| {DS_LABEL[ds]} | {lev} ({int(levels[lev]*100)}%) | " + " | ".join(cells_md) + " |")
            tex.append(f"{DS_LABEL[ds]} & {lev} & " + " & ".join(cells_tex) + " \\\\")
    (TAB / "budget_matched.md").write_text("\n".join(md) + "\n")
    (TAB / "budget_matched.tex").write_text("\n".join(tex) + "\n")
    json.dump({k: {m: (None if math.isnan(v) else v) for m, v in d.items()} for k, d in summary.items()},
              open(TAB / "budget_matched.json", "w"), indent=1)

    # ---------- statistics over feasible methods at all levels (blocks = dataset x level)
    feas = ["fedcast", "periodic", "pdelta", "kfed"]
    blocks = []
    for ds in dsets:
        for lev in levels:
            row = [np.nanmean([x[0] for x in table.get((ds, lev, m), [(np.nan,)])]) for m in feas]
            if not any(math.isnan(v) for v in row):
                blocks.append(row)
    out = {"methods": feas, "n_blocks": len(blocks)}
    if len(blocks) >= 3:
        B = np.array(blocks)
        ranks = np.apply_along_axis(stats.rankdata, 1, B)  # lower cost ratio -> rank 1
        out["mean_ranks"] = dict(zip(feas, ranks.mean(0).round(3).tolist()))
        chi, p = stats.friedmanchisquare(*B.T)
        out["friedman_chi2"], out["friedman_p"] = float(chi), float(p)
        k, n = len(feas), len(blocks)
        q05 = {2: 1.960, 3: 2.343, 4: 2.569, 5: 2.728, 6: 2.850}[k]
        out["nemenyi_cd"] = float(q05 * math.sqrt(k * (k + 1) / (6 * n)))
        cd_diagram(out["mean_ranks"], out["nemenyi_cd"], "fig_cd")
    # paired Wilcoxon per seed (FedCAST vs each), Holm-corrected
    pairs = {}
    for m in feas[1:]:
        a, b = [], []
        for ds in dsets:
            for lev in levels:
                va, vb = table.get((ds, lev, "fedcast"), []), table.get((ds, lev, m), [])
                for x, y in zip(va, vb):
                    if not (math.isnan(x[0]) or math.isnan(y[0])):
                        a.append(x[0])
                        b.append(y[0])
        if len(a) >= 6:
            w = stats.wilcoxon(a, b, alternative="less")
            pairs[m] = {"n": len(a), "p": float(w.pvalue), "fedcast_better_frac": float(np.mean(np.array(a) < np.array(b))),
                        "median_ratio_improvement": float(np.median(np.array(b) / np.array(a)))}
    ps = sorted(pairs.items(), key=lambda kv: kv[1]["p"])
    for i, (m, d) in enumerate(ps):
        d["p_holm"] = float(min(1.0, d["p"] * (len(ps) - i)))
    out["wilcoxon_vs_fedcast"] = pairs
    json.dump(out, open(TAB / "stats_E1.json", "w"), indent=1)
    print("E1 stats:", json.dumps(out, indent=1))


def cd_diagram(ranks: dict, cd: float, name: str):
    items = sorted(ranks.items(), key=lambda kv: kv[1])
    k = len(items)
    fig, ax = plt.subplots(figsize=(3.6, 1.2))
    lo, hi = 1, k
    ax.set_xlim(lo - 0.3, hi + 0.3)
    ax.set_ylim(0, 1.25)
    ax.axis("off")
    ax.hlines(0.8, lo, hi, color="k", lw=0.8)
    for r in range(lo, hi + 1):
        ax.vlines(r, 0.78, 0.82, color="k", lw=0.8)
        ax.text(r, 0.9, str(r), ha="center", fontsize=7)
    for i, (m, r) in enumerate(items):
        y = 0.55 - 0.16 * (i % 3)
        ax.plot([r, r], [0.8, y], color=COLOR[m], lw=0.8)
        ax.text(r, y - 0.07, f"{M_LABEL[m]} ({r:.2f})", ha="center", fontsize=6.5, color=COLOR[m])
    ax.hlines(1.1, lo, lo + cd, color="k", lw=1.5)
    ax.text(lo + cd / 2, 1.15, f"CD = {cd:.2f}", ha="center", fontsize=6.5)
    savefig(fig, name)


# ======================================================================= E2
def e2():
    rows = load("E2")
    if not rows:
        return
    ref = raw_ssq(rows)
    parts = ["iid", "dirichlet:1.0", "dirichlet:0.3", "dirichlet:0.1", "exclusive:2", "exclusive:1"]
    plab = {"iid": "IID", "dirichlet:1.0": "Dir(1.0)", "dirichlet:0.3": "Dir(0.3)", "dirichlet:0.1": "Dir(0.1)",
            "exclusive:2": "Excl(2)", "exclusive:1": "Excl(1)"}
    tags = ["fedcast", "fedcast-noswap", "fedcast-beta0.5", "periodic", "pdelta", "kfed", "raw"]
    tl = {"fedcast": "FedCAST", "fedcast-noswap": "FedCAST w/o swap", "fedcast-beta0.5": "FedCAST β=0.5",
          "periodic": "Periodic", "pdelta": "Periodic-Δ", "kfed": "k-FED", "raw": "Centralised-Raw"}
    tc = {"fedcast": COLOR["fedcast"], "fedcast-noswap": "#ff9896", "fedcast-beta0.5": "#e377c2",
          "periodic": COLOR["periodic"], "pdelta": COLOR["pdelta"], "kfed": COLOR["kfed"], "raw": "k"}
    dsets = [d for d in DS_ORDER if any(r["dataset"] == d for r in rows)]
    res = defaultdict(list)
    for r in rows:
        res[(r["dataset"], r["config"]["partition"], r["tag"])].append(r)
    fig, axes = plt.subplots(2, len(dsets), figsize=(2.6 * len(dsets), 3.9))
    axes = np.atleast_2d(axes).reshape(2, -1)
    md = ["| Dataset | Partition | " + " | ".join(tl[t] for t in tags) + " |", "|---|---|" + "---|" * len(tags)]
    for j, ds in enumerate(dsets):
        for t in tags:
            ar = [np.mean([r["ari"] for r in res[(ds, p, t)]]) if res[(ds, p, t)] else np.nan for p in parts]
            cr = [np.mean([ratio(r, ref) for r in res[(ds, p, t)]]) if res[(ds, p, t)] else np.nan for p in parts]
            ls = "--" if t in ("fedcast-noswap", "fedcast-beta0.5") else "-"
            axes[0, j].plot(range(len(parts)), cr, color=tc[t], ls=ls, marker="o", ms=3, lw=1, label=tl[t])
            axes[1, j].plot(range(len(parts)), ar, color=tc[t], ls=ls, marker="o", ms=3, lw=1, label=tl[t])
        axes[0, j].set_title(DS_LABEL[ds])
        for i in range(2):
            axes[i, j].set_xticks(range(len(parts)), [plab[p] for p in parts], rotation=40, fontsize=6.5)
            axes[i, j].grid(alpha=0.25, lw=0.5)
        for p in parts:
            cells = []
            for t in tags:
                v = res[(ds, p, t)]
                cells.append(f"{np.mean([r['ari'] for r in v]):.3f} / {np.mean([ratio(r, ref) for r in v]):.3f}" if v else "—")
            md.append(f"| {DS_LABEL[ds]} | {plab[p]} | " + " | ".join(cells) + " |")
    axes[0, 0].set_ylabel("cost / Centralised-Raw")
    axes[1, 0].set_ylabel("ARI")
    h, lab = axes[0, 0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=7, frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    savefig(fig, "fig_noniid")
    (TAB / "noniid.md").write_text("ARI / cost ratio (mean of seeds)\n\n" + "\n".join(md) + "\n")


# ======================================================================= E3
def detection_delays(r, ds_obj_cache={}):
    """Delay until each emerging class (or post-drift class) is recalled >= 0.5."""
    c = r["config"]
    dur = c["duration"]
    out = []
    if r["dataset"] == "syndrift":
        sys.path.insert(0, str(ROOT / "src"))
        from fedcast.data import load as dload
        if "syn" not in ds_obj_cache:
            d = dload("syndrift", max_points=60000)
            ds_obj_cache["syn"] = {int(k): float(d.u[d.y == k].min()) for k in np.unique(d.y)}
        starts = {k: v * dur for k, v in ds_obj_cache["syn"].items() if v > 0.2}
        for k, t0 in starts.items():
            hit = [e["t"] for e in r["series"] if e["t"] >= t0 and e["recall"].get(str(k), 0) >= 0.5]
            out.append((hit[0] - t0) if hit else dur - t0)
    else:  # class-rotation drift at 0.5*duration: time until macro recall recovers to 95% of pre-drift
        t0 = 0.5 * dur
        pre = [e["macro_recall"] for e in r["series"] if 0.3 * dur <= e["t"] < t0]
        base = np.mean(pre) if pre else 0
        hit = [e["t"] for e in r["series"] if e["t"] > t0 and e["macro_recall"] >= 0.95 * base]
        out.append((hit[0] - t0) if hit else dur - t0)
    return float(np.mean(out)) if out else float("nan")


def e3():
    rows = load("E3")
    if not rows:
        return
    ref = raw_ssq(rows)
    groups = defaultdict(list)
    for r in rows:
        groups[(r["dataset"], r["config"]["method"], r["config"]["param"])].append(r)
    dsets = [d for d in DS_ORDER if any(k[0] == d for k in groups)]
    show = [("raw", 0), ("fedcast", 10), ("periodic", 60), ("periodic", 120), ("kfed", 60), ("change", 3.0)]
    fig, axes = plt.subplots(1, len(dsets), figsize=(2.8 * len(dsets), 2.3))
    axes = np.atleast_1d(axes)
    md = ["| Dataset | Method | kB | ARI | cost ratio | detection / recovery delay (s) |", "|---|---|---|---|---|---|"]
    for j, ds in enumerate(dsets):
        for m, p in show:
            rs = groups.get((ds, m, float(p)), [])
            if not rs:
                continue
            T = [e["t"] for e in rs[0]["series"]]
            A = np.mean([[e["ari"] for e in r["series"]][:len(T)] for r in rs], axis=0)
            lab = M_LABEL[m] + (f" ({p:g})" if m != "raw" else "")
            axes[j].plot(T, A, color=COLOR[m], lw=1.2 if m == "fedcast" else 0.8,
                         ls="--" if (m == "periodic" and p == 120) else "-", label=lab)
        axes[j].set_title(DS_LABEL[ds] + (" (new clusters at 105 s, 195 s)" if ds == "syndrift" else " (drift at 150 s)"),
                          fontsize=7.5)
        axes[j].set_xlabel("stream time (s)")
        axes[j].grid(alpha=0.25, lw=0.5)
        for (d, m, p), rs in sorted(groups.items()):
            if d != ds:
                continue
            dl = [detection_delays(r) for r in rs]
            md.append(f"| {DS_LABEL[ds]} | {M_LABEL[m]} {p:g} | {np.mean([r['bytes_up'] for r in rs])/1e3:.1f} | "
                      f"{np.mean([r['ari'] for r in rs]):.3f} | {np.mean([ratio(r, ref) for r in rs]):.3f} | "
                      f"{np.mean(dl):.1f} ± {mean_ci(dl)[1]:.1f} |")
    axes[0].set_ylabel("ARI (20 s window)")
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=6, frameon=False, bbox_to_anchor=(0.5, -0.1))
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    savefig(fig, "fig_drift")
    (TAB / "drift.md").write_text("\n".join(md) + "\n")

    # budget tracking from FedCAST runs (cumulative bytes per node vs budget line)
    rs = groups.get(("syndrift", "fedcast", 10.0), []) + groups.get(("syndrift", "fedcast", 20.0), [])
    if rs:
        fig, ax = plt.subplots(figsize=(3.4, 2.2))
        for r in rs[:1] + [x for x in rs if x["config"]["param"] == 20.0][:1]:
            B = r["config"]["param"]
            T = np.array([e["t"] for e in r["series"]])
            NB = np.array([e["node_bytes"] for e in r["series"]])
            for i in range(NB.shape[1]):
                ax.plot(T, NB[:, i] / 1e3, color=COLOR["fedcast"] if B == 10 else "#ff7f0e", lw=0.5, alpha=0.6)
            q, d = r["config"]["mc"]["max_mc"], r["dim"]
            kappa = max(B * r["config"]["window"], 1.05 * (20 + q * (24 + 4 * d) + 24))
            ax.plot(T, (kappa + B * T) / 1e3, color="k", lw=1.0, ls="--")
            ax.plot(T, B * T / 1e3, color="k", lw=0.7, ls=":")
            ax.text(T[-1], (kappa + B * T[-1]) / 1e3, f" κ+Bt, B={B:g}", fontsize=6, va="center")
        ax.set_xlabel("stream time (s)")
        ax.set_ylabel("cumulative uplink per node (kB)")
        ax.set_title("Per-node spend: hard envelope κ+Bt (dashed), rate Bt (dotted)", fontsize=7)
        ax.grid(alpha=0.25, lw=0.5)
        savefig(fig, "fig_budget")


# ======================================================================= E4 - E7
def simple_table(name, key_fn, title, keys_order=None, extra_ref=()):
    rows = load(name)
    if not rows:
        return None
    ref = raw_ssq(rows + [r for e in extra_ref for r in load(e)])
    g = defaultdict(list)
    for r in rows:
        g[key_fn(r)].append(r)
    lines = [f"### {title}", "", "| Group | runs | kB up | wire kB | ARI | cost ratio | focus recall |", "|---|---|---|---|---|---|---|"]
    out = {}
    for k in (keys_order or sorted(g)):
        if k not in g:
            continue
        rs = g[k]
        a = mean_ci([r["ari"] for r in rs])
        c = mean_ci([ratio(r, ref) for r in rs])
        out[k] = dict(kb=np.mean([r["bytes_up"] for r in rs]) / 1e3, wire=np.mean([r["wire_up"] for r in rs]) / 1e3,
                      ari=a[0], ari_ci=a[1], ratio=c[0], ratio_ci=c[1],
                      focus=float(np.nanmean([r["focus_recall"] for r in rs])))
        o = out[k]
        lines.append(f"| {k} | {len(rs)} | {o['kb']:.1f} | {o['wire']:.1f} | {o['ari']:.3f} ± {o['ari_ci']:.3f} | "
                     f"{o['ratio']:.3f} ± {o['ratio_ci']:.3f} | {o['focus']:.3f} |")
    (TAB / f"{name}.md").write_text("\n".join(lines) + "\n")
    return out


def e4():
    out = simple_table("E4", lambda r: f"{DS_LABEL[r['dataset']]} | {r['tag']}", "Network heterogeneity and outages")
    rows = load("E4")
    if not rows:
        return
    # bytes spent by poor-link nodes: FedCAST with vs without link price
    g = defaultdict(list)
    for r in rows:
        if r["tag"].endswith("|fedcast") or r["tag"].endswith("|fedcast-noprice"):
            links = r["config"]["links"]
            per = r["bytes_up_per_node"]
            poor = [per[i] for i in range(len(per)) if links[i % len(links)] == "poor"]
            good = [per[i] for i in range(len(per)) if links[i % len(links)] in ("lan", "wan")]
            if poor and good:
                g[(r["dataset"], r["tag"])].append(np.mean(poor) / np.mean(good))
    lines = ["| Dataset | Scenario / variant | bytes(poor)/bytes(good) |", "|---|---|---|"]
    for k, v in sorted(g.items()):
        lines.append(f"| {DS_LABEL[k[0]]} | {k[1]} | {np.mean(v):.2f} |")
    (TAB / "E4_price.md").write_text("\n".join(lines) + "\n")


def e5():
    order = ["fedcast", "no-dual", "no-price", "no-novelty", "no-priority", "full-summary", "no-swap", "no-decay"]
    rows = load("E5")
    if not rows:
        return
    ref = raw_ssq(load("E1"))  # same base configuration as E1 (dirichlet:0.3, wan/cellular)
    g = defaultdict(list)
    for r in rows:
        g[(r["dataset"], r["tag"])].append(r)
    dsets = [d for d in DS_ORDER if any(k[0] == d for k in g)]
    lines = ["| Variant | " + " | ".join(DS_LABEL[d] for d in dsets) + " | mean kB | budget use |",
             "|---|" + "---|" * (len(dsets) + 2)]
    tex = []
    for t in order:
        cells, kbs, use = [], [], []
        for d in dsets:
            rs = g.get((d, t), [])
            if not rs:
                cells.append("—")
                continue
            cr = np.mean([ratio(r, ref) for r in rs])
            cells.append(f"{cr:.3f}")
            kbs.append(np.mean([r["bytes_up"] for r in rs]) / 1e3)
            use.append(np.mean([r["bytes_up"] / (r["config"]["param"] * r["config"]["duration"] * r["config"]["nodes"]) for r in rs]))
        lines.append(f"| {t} | " + " | ".join(cells) + f" | {np.mean(kbs):.1f} | {np.mean(use):.2f} |")
        tex.append(f"{t} & " + " & ".join(cells) + f" & {np.mean(kbs):.1f} & {np.mean(use):.2f} \\\\")
    (TAB / "E5_ablation.md").write_text("cost / Centralised-Raw (lower is better)\n\n" + "\n".join(lines) + "\n")
    (TAB / "E5_ablation.tex").write_text("\n".join(tex) + "\n")


def e6():
    import re

    def pretty(tag):
        m = re.search(r"(half_life|max_mc)=MCParams\(.*?\1=([0-9.]+)", tag)
        return f"{m.group(1)}={m.group(2)}" if m else tag.replace("default=", "default")
    simple_table("E6", lambda r: f"{DS_LABEL[r['dataset']]} | {pretty(r['tag'])}", "Sensitivity (FedCAST, B = 20)",
                 extra_ref=("E1",))


def e7():
    rows = load("E7")
    if not rows:
        return
    g = defaultdict(list)
    for r in rows:
        g[(r["config"]["method"], r["config"]["nodes"])].append(r)
    fig, axes = plt.subplots(1, 2, figsize=(5.2, 2.0))
    for m in ["fedcast", "periodic", "pdelta", "kfed"]:
        ns = sorted(n for (mm, n) in g if mm == m)
        if not ns:
            continue
        axes[0].plot(ns, [np.mean([r["bytes_up"] / r["config"]["nodes"] for r in g[(m, n)]]) / 1e3 for n in ns],
                     color=COLOR[m], marker=MARK[m], ms=3, lw=1, label=M_LABEL[m])
        axes[1].plot(ns, [np.mean([r["ari"] for r in g[(m, n)]]) for n in ns], color=COLOR[m], marker=MARK[m], ms=3, lw=1)
    axes[0].set_xlabel("nodes")
    axes[1].set_xlabel("nodes")
    axes[0].set_ylabel("uplink kB per node")
    axes[1].set_ylabel("ARI")
    for a in axes:
        a.grid(alpha=0.25, lw=0.5)
        a.set_xscale("log")
        a.set_xticks([5, 10, 20, 50], ["5", "10", "20", "50"])
    axes[0].legend(frameon=False, fontsize=6)
    fig.tight_layout()
    savefig(fig, "fig_scale_sim")
    simple_table("E7", lambda r: f"{r['config']['method']} | nodes={r['config']['nodes']}", "Scale in number of nodes")


def kafka():
    p = RES / "kafka_K4_scale.json"
    if p.exists():
        rows = json.load(open(p))
        fig, axes = plt.subplots(1, 2, figsize=(5.2, 2.0))
        for m, c in [("fedcast", COLOR["fedcast"]), ("raw", "k")]:
            rs = sorted([r for r in rows if r["method"] == m], key=lambda r: r["nodes"])
            n = [r["nodes"] for r in rs]
            axes[0].plot(n, [r["e2e_p50_ms"] for r in rs], color=c, marker="o", ms=3, lw=1, label=f"{M_LABEL[m]} p50")
            axes[0].plot(n, [r["e2e_p95_ms"] for r in rs], color=c, marker="o", ms=3, lw=1, ls="--", label=f"{M_LABEL[m]} p95")
            axes[1].plot(n, [r["bytes_up"] / 1e6 for r in rs], color=c, marker="o", ms=3, lw=1, label=M_LABEL[m])
        axes[0].set_ylabel("end-to-end latency (ms)")
        axes[1].set_ylabel("uplink MB (Kafka)")
        axes[1].set_yscale("log")
        for a in axes:
            a.set_xlabel("nodes")
            a.grid(alpha=0.25, lw=0.5)
        axes[0].legend(frameon=False, fontsize=5.5, loc="center right", bbox_to_anchor=(1.0, 0.62))
        axes[1].legend(frameon=False, fontsize=6)
        fig.tight_layout()
        savefig(fig, "fig_kafka_scale")




def e1_savings(threshold: float = 1.05):
    """Bytes each method needs (mean curve, log-interpolated) to reach cost ratio <= threshold."""
    rows = load("E1")
    ref = raw_ssq(rows)
    agg = defaultdict(list)
    for r in rows:
        c = r["config"]
        agg[(r["dataset"], c["method"], c["param"])].append((r["bytes_up"], ratio(r, ref)))
    out = {}
    lines = [f"Bytes (kB) needed to reach cost ratio <= {threshold}", "",
             "| Dataset | " + " | ".join(M_LABEL[m] for m in ["fedcast", "periodic", "pdelta", "kfed", "change", "norm"]) + " | saving vs best baseline |",
             "|---|" + "---|" * 7]
    for ds in DS_ORDER:
        need = {}
        for m in ["fedcast", "periodic", "pdelta", "kfed", "change", "norm"]:
            pts = sorted((np.mean([v[0] for v in vals]), np.mean([v[1] for v in vals]))
                         for (d, mm, p), vals in agg.items() if d == ds and mm == m)
            val = float("nan")
            for (b0, r0), (b1, r1) in zip([(None, None)] + pts[:-1], pts):
                if r1 <= threshold:
                    if b0 is None or r0 <= threshold:
                        val = b1
                    else:  # interpolate in log-bytes between the last point above and first below
                        w = (r0 - threshold) / max(r0 - r1, 1e-12)
                        val = math.exp(math.log(b0) + w * (math.log(b1) - math.log(b0)))
                    break
            need[m] = val
        base = [v for m, v in need.items() if m != "fedcast" and not math.isnan(v)]
        best = min(base) if base else float("nan")
        out[ds] = {**need, "saving_vs_best_baseline": (1 - need["fedcast"] / best) if base else None}
        cells = ["—" if math.isnan(need[m]) else f"{need[m]/1e3:.0f}" for m in need]
        sv = out[ds]["saving_vs_best_baseline"]
        lines.append(f"| {DS_LABEL[ds]} | " + " | ".join(cells) + f" | {'—' if sv is None else f'{sv*100:.0f}%'} |")
    (TAB / "savings.md").write_text("\n".join(lines) + "\n")
    json.dump(out, open(TAB / "savings.json", "w"), indent=1)
    print("\n".join(lines))

def e8():
    """Targeted ablations: novelty -> detection delay; dual ascent -> spend under a loose budget;
    link price -> where the bytes go under loose budgets on heterogeneous links."""
    rows = load("E8")
    if not rows:
        return
    ref = raw_ssq(load("E1") + load("E4"))
    lines = ["| Component | Setting | Variant | kB up | wire kB | cost ratio | target metric |", "|---|---|---|---|---|---|---|"]
    out = {}
    g = defaultdict(list)
    for r in rows:
        g[(r["dataset"], r["tag"], r["config"]["param"])].append(r)
    for (ds, tag, B), rs in sorted(g.items()):
        kb = np.mean([r["bytes_up"] for r in rs]) / 1e3
        wire = np.mean([r["wire_up"] for r in rs]) / 1e3
        cr = np.nanmean([ratio(r, ref) for r in rs])
        if tag.startswith("novelty"):
            dl = [detection_delays(r) for r in rs]
            m, h = mean_ci(dl)
            metric = f"detection delay {m:.1f} ± {h:.1f} s"
            comp = "novelty override"
        elif tag.startswith("dual"):
            use = np.mean([r["bytes_up"] / (r["config"]["param"] * r["config"]["duration"] * r["config"]["nodes"]) for r in rs])
            metric = f"budget used {use*100:.0f}%"
            comp = "dual ascent"
        else:
            ratios = []
            for r in rs:
                links = r["config"]["links"]
                per = r["bytes_up_per_node"]
                poor = [per[i] for i in range(len(per)) if links[i % len(links)] in ("poor", "cellular")]
                good = [per[i] for i in range(len(per)) if links[i % len(links)] in ("lan", "wan")]
                ratios.append(np.mean(poor) / np.mean(good))
            metric = f"bytes(bad links)/bytes(good links) {np.mean(ratios):.2f}"
            comp = "link price"
        out[f"{ds}|{tag}|{B}"] = dict(kb=kb, wire=wire, ratio=cr, metric=metric)
        lines.append(f"| {comp} | {DS_LABEL[ds]}, B={B:g} | {tag} | {kb:.1f} | {wire:.1f} | {cr:.3f} | {metric} |")
    (TAB / "E8_targeted.md").write_text("\n".join(lines) + "\n")
    json.dump(out, open(TAB / "E8_targeted.json", "w"), indent=1)
    print("\n".join(lines))

def e9():
    """Novelty check: ranking (objective vs top-k magnitude vs model-free bound) and
    budget-adaptive resolution, against the E1 baselines on the same configuration."""
    e1, e9r = load("E1"), load("E9")
    if not e9r:
        return
    ref = raw_ssq(e1)
    pts = defaultdict(list)   # (ds, variant, B) -> [(seed, bytes, ratio)]
    for r in e1:
        c = r["config"]
        if c["method"] in ("fedcast", "kfed", "periodic", "pdelta"):
            pts[(r["dataset"], "objective" if c["method"] == "fedcast" else c["method"], c["param"])].append(
                (c["seed"], r["bytes_up"], ratio(r, ref)))
    for r in e9r:
        pts[(r["dataset"], r["tag"], r["config"]["param"])].append((r["config"]["seed"], r["bytes_up"], ratio(r, ref)))
    VL = {"objective": "FedCAST (objective rank)", "norm": "top-k magnitude rank", "uniform": "model-free bound rank",
          "aq": "FedCAST + adaptive resolution (q≥k)", "aq2": "FedCAST + adaptive resolution (q≥2k)",
          "kfed": "k-FED", "periodic": "Periodic", "pdelta": "Periodic-Δ"}
    VC = {"objective": COLOR["fedcast"], "norm": "#9467bd", "uniform": "#8c564b", "aq": "#ff7f0e", "aq2": "#e377c2",
          "kfed": COLOR["kfed"], "periodic": COLOR["periodic"], "pdelta": COLOR["pdelta"]}
    dsets = [d for d in DS_ORDER if any(k[0] == d for k in pts)]
    fig, axes = plt.subplots(1, len(dsets), figsize=(2.5 * len(dsets), 2.6))
    for j, ds in enumerate(dsets):
        ax = axes[j]
        for v in ["periodic", "kfed", "norm", "uniform", "aq", "aq2", "objective"]:
            cur = sorted((np.mean([x[1] for x in vals]), np.mean([x[2] for x in vals]))
                         for (d, vv, B), vals in pts.items() if d == ds and vv == v and B <= 150)
            if not cur:
                continue
            ax.plot([c[0] / 1e3 for c in cur], np.minimum([c[1] for c in cur], 1.35), marker="o", ms=2.5,
                    lw=1.5 if v in ("objective", "aq") else 0.9, color=VC[v], label=VL[v],
                    ls="--" if v in ("norm", "uniform", "aq2") else "-")
        ax.set_xscale("log")
        ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        ax.set_title(DS_LABEL[ds])
        ax.set_xlabel("uplink kB (log)")
        ax.grid(alpha=0.25, lw=0.5)
    axes[0].set_ylabel("cost / Centralised-Raw (clipped 1.35)")
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=4, frameon=False, bbox_to_anchor=(0.5, -0.12))
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    savefig(fig, "fig_novelty")

    # paired test: objective vs each other ranking at equal budget (same seed, same B)
    out = {}
    lines = ["| Comparison | Data | pairs | objective better | median cost-ratio gain | one-sided Wilcoxon p |",
             "|---|---|---|---|---|---|"]
    for other in ["norm", "uniform"]:
        for group, dl in [("drifting (SynDrift)", ["syndrift"]), ("static (4 real datasets)", ["nslkdd", "shuttle", "pendigits", "letter"]),
                          ("all", dsets)]:
            a, b = [], []
            for (d, v, B), vals in pts.items():
                if v != "objective" or d not in dl or B > 150:
                    continue
                o = {x[0]: x[2] for x in pts.get((d, other, B), [])}
                for sd, _, rr in vals:
                    if sd in o:
                        a.append(rr)
                        b.append(o[sd])
            if len(a) >= 6:
                a, b = np.array(a), np.array(b)
                w = stats.wilcoxon(a, b, alternative="less")
                out[f"objective_vs_{other}|{group}"] = dict(n=len(a), better=float((a < b).mean()),
                                                           median_gain=float(np.median(b - a)), p=float(w.pvalue))
                lines.append(f"| objective vs {other} | {group} | {len(a)} | {(a < b).mean()*100:.0f}% | "
                             f"{np.median(b - a):+.4f} | {w.pvalue:.2g} |")
    # savings including new variants
    def need(ds, v, thr=1.05):
        cur = sorted((np.mean([x[1] for x in vals]), np.mean([x[2] for x in vals]))
                     for (d, vv, B), vals in pts.items() if d == ds and vv == v)
        prev = None
        for b1, r1 in cur:
            if r1 <= thr:
                if prev is None or prev[1] <= thr:
                    return b1
                w = (prev[1] - thr) / max(prev[1] - r1, 1e-12)
                return math.exp(math.log(prev[0]) + w * (math.log(b1) - math.log(prev[0])))
            prev = (b1, r1)
        return float("nan")
    lines += ["", "kB needed to reach cost ratio <= 1.05 (and <= 1.10)", "",
              "| Data | objective | + adaptive q≥k | + adaptive q≥2k | top-k magnitude | model-free | k-FED | Periodic-Δ |",
              "|---|---|---|---|---|---|---|---|"]
    for ds in dsets:
        cells = []
        for v in ["objective", "aq", "aq2", "norm", "uniform", "kfed", "pdelta"]:
            a5, a10 = need(ds, v, 1.05), need(ds, v, 1.10)
            cells.append(("—" if math.isnan(a5) else f"{a5/1e3:.0f}") + " / " + ("—" if math.isnan(a10) else f"{a10/1e3:.0f}"))
        lines.append(f"| {DS_LABEL[ds]} | " + " | ".join(cells) + " |")
    (TAB / "E9_novelty.md").write_text("\n".join(lines) + "\n")
    json.dump(out, open(TAB / "E9_novelty.json", "w"), indent=1)
    print("\n".join(lines))

def e10():
    """Objective vs magnitude vs model-free ranking on real evolving streams."""
    rows = load("E10")
    if not rows:
        return
    ref = raw_ssq(rows)
    pts = defaultdict(list)
    for r in rows:
        if r["tag"] == "raw":
            continue
        pts[(r["dataset"], r["tag"], r["config"]["param"])].append((r["config"]["seed"], r["bytes_up"], ratio(r, ref)))
    dsets = [d for d in DS_ORDER if any(k[0] == d for k in pts)]
    VL = {"objective": "FedCAST (objective rank)", "norm": "top-k magnitude rank", "uniform": "model-free bound rank",
          "aq2": "FedCAST + adaptive resolution", "kfed": "k-FED", "pdelta": "Periodic-Δ"}
    VC = {"objective": COLOR["fedcast"], "norm": "#9467bd", "uniform": "#8c564b", "aq2": "#ff7f0e",
          "kfed": COLOR["kfed"], "pdelta": COLOR["pdelta"]}
    fig, axes = plt.subplots(1, len(dsets), figsize=(2.6 * len(dsets), 2.6))
    for j, ds in enumerate(dsets):
        for v in ["pdelta", "kfed", "uniform", "norm", "aq2", "objective"]:
            cur = sorted((np.mean([x[1] for x in vals]), np.mean([x[2] for x in vals]))
                         for (d, vv, B), vals in pts.items() if d == ds and vv == v)
            if cur:
                axes[j].plot([c[0] / 1e3 for c in cur], np.minimum([c[1] for c in cur], 1.35), marker="o", ms=2.5,
                             color=VC[v], lw=1.5 if v == "objective" else 0.9,
                             ls="--" if v in ("norm", "uniform") else "-", label=VL[v])
        axes[j].set_xscale("log")
        axes[j].xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        axes[j].set_title(DS_LABEL[ds] + " (evolving)")
        axes[j].set_xlabel("uplink kB (log)")
        axes[j].grid(alpha=0.25, lw=0.5)
    axes[0].set_ylabel("cost / Centralised-Raw")
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=6, frameon=False, bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    savefig(fig, "fig_evolving")
    out = {}
    lines = ["| Comparison | Data | pairs | objective better | median cost-ratio gain | one-sided Wilcoxon p |",
             "|---|---|---|---|---|---|"]
    for other in ["norm", "uniform"]:
        for group, dl in [(DS_LABEL[d], [d]) for d in dsets] + [("all 4 real evolving", dsets)]:
            a, b = [], []
            for (d, v, B), vals in pts.items():
                if v != "objective" or d not in dl:
                    continue
                o = {x[0]: x[2] for x in pts.get((d, other, B), [])}
                for sd, _, rr in vals:
                    if sd in o:
                        a.append(rr)
                        b.append(o[sd])
            if len(a) >= 6:
                a, b = np.array(a), np.array(b)
                w = stats.wilcoxon(a, b, alternative="less")
                out[f"objective_vs_{other}|{group}"] = dict(n=len(a), better=float((a < b).mean()),
                                                           median_gain=float(np.median(b - a)), p=float(w.pvalue))
                lines.append(f"| objective vs {other} | {group} | {len(a)} | {(a < b).mean()*100:.0f}% | "
                             f"{np.median(b - a):+.4f} | {w.pvalue:.2g} |")

    def need(ds, v, thr):
        cur = sorted((np.mean([x[1] for x in vals]), np.mean([x[2] for x in vals]))
                     for (d, vv, B), vals in pts.items() if d == ds and vv == v)
        prev = None
        for b1, r1 in cur:
            if r1 <= thr:
                if prev is None or prev[1] <= thr:
                    return b1
                w = (prev[1] - thr) / max(prev[1] - r1, 1e-12)
                return math.exp(math.log(prev[0]) + w * (math.log(b1) - math.log(prev[0])))
            prev = (b1, r1)
        return float("nan")
    lines += ["", "kB needed to reach cost ratio <= 1.05 / <= 1.10 on evolving streams", "",
              "| Data | objective | top-k magnitude | model-free | + adaptive res. | k-FED | Periodic-Δ |",
              "|---|---|---|---|---|---|---|"]
    for ds in dsets:
        cells = []
        for v in ["objective", "norm", "uniform", "aq2", "kfed", "pdelta"]:
            a5, a10 = need(ds, v, 1.05), need(ds, v, 1.10)
            cells.append(("—" if math.isnan(a5) else f"{a5/1e3:.0f}") + " / " + ("—" if math.isnan(a10) else f"{a10/1e3:.0f}"))
        lines.append(f"| {DS_LABEL[ds]} | " + " | ".join(cells) + " |")
    (TAB / "E10_evolving.md").write_text("\n".join(lines) + "\n")
    json.dump(out, open(TAB / "E10_evolving.json", "w"), indent=1)
    print("\n".join(lines))

def e11():
    """Hybrid ranking vs objective vs top-k magnitude, paired, on static, drifting and evolving streams."""
    e1, e9, e10, e11r = load("E1"), load("E9"), load("E10"), load("E11")
    if not e11r:
        return
    ref = raw_ssq(e1 + e10)
    val = defaultdict(dict)   # (setting, ds, B, seed) -> {rank: ratio}; plus bytes
    byt = defaultdict(dict)

    def setting(r):
        if r["config"]["partition"].startswith("evolve"):
            return "evolving real"
        return "drifting (SynDrift)" if r["dataset"] == "syndrift" else "static real"

    def add(r, rank):
        c = r["config"]
        if c["method"] != "fedcast" or c.get("adaptive_q"):
            return
        key = (setting(r), r["dataset"], c["param"], c["seed"])
        val[key][rank] = ratio(r, ref)
        byt[key][rank] = r["bytes_up"]
    for r in e1:
        add(r, "objective")
    for r in e9:
        if r["tag"] in ("objective", "norm", "uniform"):
            add(r, r["tag"])
    for r in e10:
        if r["tag"] in ("objective", "norm", "uniform"):
            add(r, r["tag"])
    for r in e11r:
        add(r, "hybrid")
    lines = ["| Setting | vs | pairs | hybrid better | median gain | one-sided Wilcoxon p | Holm p |",
             "|---|---|---|---|---|---|---|"]
    tests = []
    for setg in ["static real", "drifting (SynDrift)", "evolving real", "all"]:
        for other in ["norm", "objective"]:
            a, b = [], []
            for key, d in val.items():
                if (setg == "all" or key[0] == setg) and "hybrid" in d and other in d:
                    a.append(d["hybrid"])
                    b.append(d[other])
            if len(a) >= 6:
                a, b = np.array(a), np.array(b)
                w = stats.wilcoxon(a, b, alternative="less")
                tests.append([setg, other, len(a), float((a < b).mean()), float(np.median(b - a)), float(w.pvalue)])
    ps = sorted(range(len(tests)), key=lambda i: tests[i][5])
    for rank_i, i in enumerate(ps):
        tests[i].append(min(1.0, tests[i][5] * (len(tests) - rank_i)))
    for t in tests:
        lines.append(f"| {t[0]} | {t[1]} | {t[2]} | {t[3]*100:.0f}% | {t[4]:+.4f} | {t[5]:.2g} | {t[6]:.2g} |")
    # Friedman over all paired blocks with all three rankings
    blocks = [[d["hybrid"], d["objective"], d["norm"]] for d in val.values() if {"hybrid", "objective", "norm"} <= set(d)]
    if len(blocks) >= 5:
        B = np.array(blocks)
        ranks = np.apply_along_axis(stats.rankdata, 1, B).mean(0)
        chi, p = stats.friedmanchisquare(*B.T)
        lines += ["", f"Friedman over {len(blocks)} paired blocks (dataset x setting x budget x seed): "
                      f"mean ranks hybrid {ranks[0]:.2f}, objective {ranks[1]:.2f}, top-k magnitude {ranks[2]:.2f}; "
                      f"chi2 = {chi:.1f}, p = {p:.2g}"]
    (TAB / "E11_hybrid.md").write_text("\n".join(lines) + "\n")
    json.dump({"tests": tests}, open(TAB / "E11_hybrid.json", "w"), indent=1)
    print("\n".join(lines))


if __name__ == "__main__":
    which = sys.argv[1:] or ["e1", "e1_savings", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10", "e11", "kafka"]
    for w in which:
        globals()[w]()
        print("done", w, flush=True)

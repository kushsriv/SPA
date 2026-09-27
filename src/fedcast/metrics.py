"""Clustering quality metrics on a sliding window of recent points."""
from __future__ import annotations

import numpy as np
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

from .macro import sqdist


def evaluate(X: np.ndarray, y: np.ndarray, C: np.ndarray, focus: list[int] | None = None) -> dict:
    D = sqdist(X, C)
    lab = D.argmin(1)
    ssq = float(D[np.arange(len(X)), lab].mean())
    k = len(C)
    classes = np.unique(y)
    # majority label per predicted cluster
    M = np.zeros((k, int(y.max()) + 1))
    np.add.at(M, (lab, y), 1)
    purity = float(M.max(1).sum() / len(y))
    major = M.argmax(1)
    recall = {int(c): float(((y == c) & (major[lab] == c)).sum() / max((y == c).sum(), 1)) for c in classes}
    out = {
        "ari": float(adjusted_rand_score(y, lab)),
        "nmi": float(normalized_mutual_info_score(y, lab)),
        "purity": purity,
        "ssq": ssq,
        "macro_recall": float(np.mean(list(recall.values()))),
        "recall": recall,
    }
    if focus:
        vals = [recall[c] for c in focus if c in recall]
        out["focus_recall"] = float(np.mean(vals)) if vals else float("nan")
    return out

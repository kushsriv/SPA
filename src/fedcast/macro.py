"""Macro-clustering over CF vectors (weighted k-means with swap local search).

Everything here works on sets of CFs (n, LS, SS), never on raw points.

Key identity (used by the coordinator and by the edge trigger): the k-means
cost of assigning every point summarised by CF to a centre c is

    cost(CF, c) = SS - 2 LS.c + n ||c||^2  =  n ||mu - c||^2 + (SS - n ||mu||^2)

so the assignment of a whole CF depends only on its centroid mu = LS / n.
"""
from __future__ import annotations

import numpy as np


def sqdist(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    a = np.einsum("ij,ij->i", A, A)
    b = np.einsum("ij,ij->i", B, B)
    return np.maximum(a[:, None] + b[None, :] - 2.0 * A @ B.T, 0.0)


def cf_cost(n: np.ndarray, LS: np.ndarray, SS: np.ndarray, C: np.ndarray) -> np.ndarray:
    """(m, k) matrix of cost(CF_i, c_j)."""
    cc = np.einsum("ij,ij->i", C, C)
    return np.maximum(SS[:, None] - 2.0 * LS @ C.T + n[:, None] * cc[None, :], 0.0)


def kmeanspp(X: np.ndarray, w: np.ndarray, k: int, rng: np.random.Generator) -> np.ndarray:
    m = X.shape[0]
    idx = [int(rng.choice(m, p=w / w.sum()))]
    d2 = sqdist(X, X[idx]).ravel()
    for _ in range(1, k):
        p = w * d2
        if p.sum() <= 0:
            idx.append(int(rng.integers(m)))
        else:
            idx.append(int(rng.choice(m, p=p / p.sum())))
        d2 = np.minimum(d2, sqdist(X, X[idx[-1:]]).ravel())
    return X[idx].copy()


def lloyd(X: np.ndarray, w: np.ndarray, C: np.ndarray, iters: int = 25, tol: float = 1e-6):
    """Weighted Lloyd on centroids X with weights w. Returns (C, labels, cost)."""
    C = C.copy()
    prev = np.inf
    for _ in range(iters):
        D = sqdist(X, C)
        lab = D.argmin(1)
        cost = float((w * D[np.arange(len(X)), lab]).sum())
        W = np.bincount(lab, weights=w, minlength=len(C))
        S = np.zeros_like(C)
        np.add.at(S, lab, w[:, None] * X)
        nz = W > 0
        C[nz] = S[nz] / W[nz, None]
        # an empty centre is re-seeded at the worst-served point
        for j in np.flatnonzero(~nz):
            far = int(np.argmax(w * D[np.arange(len(X)), lab]))
            C[j] = X[far]
        if prev - cost <= tol * max(prev, 1e-12):
            break
        prev = cost
    D = sqdist(X, C)
    lab = D.argmin(1)
    cost = float((w * D[np.arange(len(X)), lab]).sum())
    return C, lab, cost


def weighted_kmeans(X: np.ndarray, w: np.ndarray, k: int, rng: np.random.Generator,
                    init: np.ndarray | None = None, swaps: int = 0, restarts: int = 1):
    """Weighted k-means over CF centroids.

    swaps > 0 enables swap local search (Kanungo et al., 2004): move the centre
    whose removal costs least to the worst-served CF, re-run Lloyd, and keep
    the move only if the total cost drops. With warm starts this is what lets a
    cluster that only one node sees acquire its own centre instead of being
    absorbed by a larger neighbour ("exclusive-cluster protection").
    """
    m = X.shape[0]
    if m == 0:
        raise ValueError("no micro-clusters")
    if m <= k:
        C = np.vstack([X, X[rng.integers(m, size=k - m)]]) if m < k else X.copy()
        return lloyd(X, w, C, iters=1)
    best = None
    for r in range(restarts):
        C0 = init.copy() if (init is not None and r == 0 and len(init) == k) else kmeanspp(X, w, k, rng)
        C, lab, cost = lloyd(X, w, C0)
        for _ in range(swaps):
            D = sqdist(X, C)
            served = w * D[np.arange(m), lab]
            cand = int(np.argmax(served))
            if served[cand] <= 0:
                break
            W = np.bincount(lab, weights=w, minlength=k)
            # Ward merge cost of each centre with its nearest other centre
            DC = sqdist(C, C)
            np.fill_diagonal(DC, np.inf)
            nn = DC.argmin(1)
            merge = W * W[nn] / np.maximum(W + W[nn], 1e-12) * DC[np.arange(k), nn]
            drop = int(np.argmin(merge))
            C_try = C.copy()
            C_try[drop] = X[cand]
            C2, lab2, cost2 = lloyd(X, w, C_try)
            if cost2 < cost * (1 - 1e-4):
                C, lab, cost = C2, lab2, cost2
            else:
                break
        if best is None or cost < best[2]:
            best = (C, lab, cost)
    return best


def center_stats(n, LS, SS, w_scale, C, lab):
    """Per-centre RMS radius and weight, from CFs (w_scale = weight per unit n)."""
    k = len(C)
    cost = cf_cost(n, LS, SS, C)[np.arange(len(n)), lab]
    W = np.bincount(lab, weights=n * w_scale, minlength=k)
    E = np.bincount(lab, weights=cost * w_scale, minlength=k)
    radii = np.sqrt(np.where(W > 0, E / np.maximum(W, 1e-12), 0.0))
    return radii, W

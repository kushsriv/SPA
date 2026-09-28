"""Non-IID partitioning of a dataset into per-node timed streams.

Schemes (spec strings):
  iid              points dealt uniformly at random
  dirichlet:A      label skew: each class is split across nodes by Dir(A)
  exclusive:K      each node sees K classes (with m*K <= #classes every class is
                   seen by exactly one node: the k-FED heterogeneous regime)
  drift:K          exclusive:K in the first half; in the second half every node
                   switches to the classes of the next node (sudden drift)
  quantity:A       iid content but node rates follow a power law with exponent A
  natural          the dataset's own device ids (e.g. sensor motes), grouped into m
                   contiguous blocks of ids (spatial zones for Intel Lab)
  evolve:A         dirichlet:A split, and the stream *evolves*: every class c gets a
                   birth time b_c (a few classes at 0, the rest uniform in [0, 0.6]) and
                   its points arrive only after b_c (concept evolution / emerging
                   clusters, built from a static dataset as in MOA-style generators)
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class NodeStream:
    idx: np.ndarray    # dataset row indices, in time order
    times: np.ndarray  # arrival times (seconds)


def _times(u: np.ndarray, duration: float) -> np.ndarray:
    return np.sort(u) * duration


def partition(y: np.ndarray, m: int, spec: str, duration: float, seed: int = 0,
              u: np.ndarray | None = None, groups: np.ndarray | None = None) -> list[NodeStream]:
    rng = np.random.default_rng(seed)
    n = len(y)
    classes = np.unique(y)
    K = len(classes)
    kind, _, arg = spec.partition(":")
    if kind == "evolve":
        birth = {int(c): 0.0 for c in classes}
        later = rng.permutation(classes)[max(1, K // 4):]
        for c in later:
            birth[int(c)] = float(rng.uniform(0.0, 0.6))
        b = np.array([birth[int(c)] for c in y])
        u = b + (1.0 - b) * rng.uniform(0, 1, n)
        kind = "dirichlet"
    elif u is None:
        u = rng.uniform(0, 1, n)
    owner = np.empty(n, dtype=int)

    if kind == "natural":
        if groups is None:
            raise ValueError("partition 'natural' needs device ids (Dataset.groups)")
        ug = np.unique(groups)
        block = {g: i * m // len(ug) for i, g in enumerate(ug)}
        owner = np.array([block[g] for g in groups])
    elif kind == "iid" or kind == "quantity":
        if kind == "quantity":
            a = float(arg or 1.0)
            p = 1.0 / np.arange(1, m + 1) ** a
            p /= p.sum()
            owner = rng.choice(m, size=n, p=p)
        else:
            owner = rng.integers(0, m, n)
    elif kind == "dirichlet":
        alpha = float(arg)
        for c in classes:
            rows = np.flatnonzero(y == c)
            rng.shuffle(rows)
            p = rng.dirichlet(alpha * np.ones(m))
            cuts = (np.cumsum(p)[:-1] * len(rows)).astype(int)
            for node, part in enumerate(np.split(rows, cuts)):
                owner[part] = node
    elif kind in ("exclusive", "drift"):
        kk = int(arg)
        order = rng.permutation(classes)
        sets = [order[(i * kk + np.arange(kk)) % K] for i in range(m)]
        holders = {c: [i for i in range(m) if c in sets[i]] for c in classes}
        late = u >= 0.5
        for c in classes:
            rows = np.flatnonzero(y == c)
            hs = holders[c]
            if not hs:  # class nobody holds: give it to a random node
                hs = [int(rng.integers(m))]
            owner[rows] = rng.choice(hs, size=len(rows))
            if kind == "drift":
                lr = rows[late[rows]]
                # in the second half, class c is held by the nodes *before* its holders
                owner[lr] = (rng.choice(hs, size=len(lr)) - 1) % m
    else:
        raise ValueError(f"unknown partition {spec}")

    streams = []
    for node in range(m):
        rows = np.flatnonzero(owner == node)
        o = np.argsort(u[rows], kind="stable")
        streams.append(NodeStream(rows[o], u[rows][o] * duration))
    return streams


def exclusive_classes(y: np.ndarray, streams: list[NodeStream]) -> list[int]:
    """Classes whose points (>= 95%) sit on a single node."""
    out = []
    for c in np.unique(y):
        counts = np.array([(y[s.idx] == c).sum() for s in streams])
        if counts.sum() and counts.max() >= 0.95 * counts.sum() and (counts > 0).sum() >= 1:
            if (counts >= 0.05 * counts.sum()).sum() == 1:
                out.append(int(c))
    return out

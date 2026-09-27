"""Coordinator: applies node summaries and builds the global model."""
from __future__ import annotations

import numpy as np

from .codec import KIND_FULL, GlobalModel, Summary
from .macro import center_stats, weighted_kmeans
from .microcluster import MCParams, MicroClusterModel, decay_factor


class Coordinator:
    """Holds the last CFs received from every node and re-clusters them.

    staleness_decay: decay every stored CF to "now" with the same half-life
        the nodes use, so a node that went silent fades out instead of
        freezing its old mass into the global model.
    beta: node balancing; node i's total mass N_i is rescaled to N_i**beta
        (beta = 1 is plain mass weighting, beta < 1 stops a single fast node
        from dominating the global model).
    swaps: swap local-search moves per aggregation (exclusive-cluster protection).
    """

    def __init__(self, k: int, dim: int, half_life: float, beta: float = 1.0, swaps: int = 3,
                 staleness_decay: bool = True, warm_start: bool = True, seed: int = 0,
                 restarts: int = 2):
        self.k, self.dim, self.half_life = k, dim, half_life
        self.beta, self.swaps = beta, swaps
        self.staleness_decay, self.warm_start = staleness_decay, warm_start
        self.restarts = restarts
        self.rng = np.random.default_rng(seed)
        self.nodes: dict[int, dict[int, tuple[float, np.ndarray, float, float]]] = {}
        self.last_seq: dict[int, int] = {}
        self.model: GlobalModel | None = None
        self.version = 0
        self.applied = 0
        self.duplicates = 0

    # ------------------------------------------------------------------ state
    def apply(self, s: Summary) -> bool:
        """Apply a summary once (duplicates from producer retries or offset replay are ignored)."""
        if s.seq <= self.last_seq.get(s.node, -1):
            self.duplicates += 1
            return False
        self.last_seq[s.node] = s.seq
        st = self.nodes.setdefault(s.node, {})
        if s.kind == KIND_FULL:
            st.clear()
        for j, i in enumerate(s.ids):
            st[int(i)] = (float(s.n[j]), s.LS[j], float(s.SS[j]), float(s.t[j]))
        for i in s.deleted:
            st.pop(int(i), None)
        self.applied += 1
        return True

    def arrays(self, now: float):
        node_of, n, LS, SS, t = [], [], [], [], []
        for node, st in self.nodes.items():
            for (a, b, c, d) in st.values():
                node_of.append(node)
                n.append(a)
                LS.append(b)
                SS.append(c)
                t.append(d)
        if not n:
            return None
        node_of = np.array(node_of)
        n, LS, SS, t = np.array(n), np.array(LS), np.array(SS), np.array(t)
        f = decay_factor(now, t, self.half_life) if self.staleness_decay else np.ones_like(n)
        return node_of, n * f, LS * f[:, None], SS * f

    # ------------------------------------------------------------------ model
    def aggregate(self, now: float) -> GlobalModel | None:
        arr = self.arrays(now)
        if arr is None:
            return None
        node_of, n, LS, SS = arr
        keep = n > 1e-9
        node_of, n, LS, SS = node_of[keep], n[keep], LS[keep], SS[keep]
        if len(n) == 0:
            return None
        scale = np.ones_like(n)
        if self.beta != 1.0:
            for node in np.unique(node_of):
                m = node_of == node
                N = n[m].sum()
                scale[m] = N ** (self.beta - 1.0)
        w = n * scale
        X = LS / n[:, None]
        init = self.model.centers if (self.warm_start and self.model is not None) else None
        C, lab, _ = weighted_kmeans(X, w, self.k, self.rng, init=init, swaps=self.swaps,
                                    restarts=self.restarts)
        radii, W = center_stats(n, LS, SS, scale, C, lab)
        self.version += 1
        self.model = GlobalModel(self.version, now, C, radii, W)
        return self.model


class CentralCoordinator(Coordinator):
    """Centralised-Raw baseline: raw points arrive and one big micro-cluster
    model is maintained at the server (the quality upper bound)."""

    def __init__(self, k: int, dim: int, params: MCParams, **kw):
        super().__init__(k, dim, params.half_life, **kw)
        self.mc = MicroClusterModel(dim, params)

    def apply_raw(self, X: np.ndarray, times: np.ndarray) -> None:
        for x, tt in zip(X, times):
            self.mc.insert(x, float(tt))

    def arrays(self, now: float):
        mc = self.mc
        act = np.flatnonzero(mc.active)
        if act.size == 0:
            return None
        f = decay_factor(now, mc.t[act], self.half_life)
        return np.zeros(act.size, int), mc.n[act] * f, mc.LS[act] * f[:, None], mc.SS[act] * f

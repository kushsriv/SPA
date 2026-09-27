"""Online micro-clustering with time-decayed cluster features (CF vectors).

Each micro-cluster (MC) keeps the additive summary CF = (n, LS, SS, t):
    n  - decayed number of points
    LS - decayed linear sum of the points (vector, length d)
    SS - decayed sum of squared norms (scalar)
    t  - time of the last update (decay reference)

Decay multiplies n, LS and SS by the same factor 2^{-(now - t)/H}, so the
centroid LS/n and the RMS radius are invariant under decay. Only the weight
changes. This is what lets the coordinator reproduce the node's decayed state
exactly from the last CF it received plus its timestamp.

The update rule follows CluStream (absorb into the nearest MC if inside its
boundary, otherwise open a new MC) with DenStream-style decay and pruning.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field

import numpy as np


def decay_factor(now: float, t: np.ndarray | float, half_life: float) -> np.ndarray | float:
    if half_life <= 0 or np.isinf(half_life):
        return np.ones_like(t, dtype=np.float64) if isinstance(t, np.ndarray) else 1.0
    return np.exp2(-(now - t) / half_life)


@dataclass
class MCParams:
    max_mc: int = 50            # capacity q
    radius_mult: float = 2.0    # boundary = radius_mult * RMS radius
    min_radius: float = 0.25    # floor for the boundary of young MCs (standardised units)
    half_life: float = 120.0    # seconds; H in 2^{-dt/H}
    min_weight: float = 0.5     # MCs whose decayed weight drops below this are pruned


@dataclass
class MicroClusterModel:
    dim: int
    params: MCParams = field(default_factory=MCParams)

    def __post_init__(self) -> None:
        q, d = self.params.max_mc, self.dim
        self.n = np.zeros(q)
        self.LS = np.zeros((q, d))
        self.SS = np.zeros(q)
        self.t = np.zeros(q)
        self.ids = np.full(q, -1, dtype=np.int64)
        self.active = np.zeros(q, dtype=bool)
        self.mod = np.zeros(q, dtype=np.int64)      # modification counter of each slot
        self.counter = 0                             # global modification clock
        self.next_id = 0
        self.del_counter: list[int] = []            # counters of deletions (increasing)
        self.del_ids: list[int] = []                # ids deleted, aligned with del_counter
        self.points_seen = 0
        # nearest-neighbour cache between MC centroids (exact closest pair in O(q))
        self.nn_dist = np.full(q, np.inf)
        self.nn_idx = np.full(q, -1, dtype=np.int64)

    # ------------------------------------------------------------------ helpers
    def _touch(self, slot: int) -> None:
        self.counter += 1
        self.mod[slot] = self.counter
        self._refresh_nn(slot)

    def _row(self, slot: int) -> np.ndarray:
        act = self.active.copy()
        act[slot] = False
        d2 = np.full(len(self.n), np.inf)
        idx = np.flatnonzero(act)
        if idx.size:
            mu = self.LS[idx] / self.n[idx, None]
            c = self.LS[slot] / self.n[slot]
            d2[idx] = np.einsum("ij,ij->i", mu - c, mu - c)
        return d2

    def _recompute(self, slot: int) -> None:
        d2 = self._row(slot)
        j = int(np.argmin(d2))
        self.nn_dist[slot] = d2[j]
        self.nn_idx[slot] = j if np.isfinite(d2[j]) else -1

    def _refresh_nn(self, slot: int) -> None:
        """slot's centroid changed (or slot is new): update the NN cache exactly."""
        d2 = self._row(slot)
        j = int(np.argmin(d2))
        self.nn_dist[slot] = d2[j]
        self.nn_idx[slot] = j if np.isfinite(d2[j]) else -1
        closer = d2 < self.nn_dist
        self.nn_dist[closer] = d2[closer]
        self.nn_idx[closer] = slot
        stale = np.flatnonzero(self.active & (self.nn_idx == slot) & (d2 > self.nn_dist))
        for o in stale:
            if o != slot:
                self._recompute(int(o))

    def _decay_slot(self, slot: int, now: float) -> None:
        f = decay_factor(now, self.t[slot], self.params.half_life)
        self.n[slot] *= f
        self.LS[slot] *= f
        self.SS[slot] *= f
        self.t[slot] = now

    def _delete(self, slot: int) -> None:
        self.counter += 1
        self.del_counter.append(self.counter)
        self.del_ids.append(int(self.ids[slot]))
        self.active[slot] = False
        self.n[slot] = 0.0
        self.ids[slot] = -1
        self.nn_dist[slot] = np.inf
        self.nn_idx[slot] = -1
        for o in np.flatnonzero(self.active & (self.nn_idx == slot)):
            self._recompute(int(o))

    def _new(self, slot: int, x: np.ndarray, now: float) -> None:
        self.n[slot] = 1.0
        self.LS[slot] = x
        self.SS[slot] = float(x @ x)
        self.t[slot] = now
        self.ids[slot] = self.next_id
        self.next_id += 1
        self.active[slot] = True
        self._touch(slot)

    def centroids(self) -> np.ndarray:
        idx = np.flatnonzero(self.active)
        return self.LS[idx] / self.n[idx, None]

    def rms_radius(self, slots: np.ndarray) -> np.ndarray:
        mu = self.LS[slots] / self.n[slots, None]
        var = self.SS[slots] / self.n[slots] - np.einsum("ij,ij->i", mu, mu)
        return np.sqrt(np.maximum(var, 0.0))

    # ------------------------------------------------------------------ update
    def insert(self, x: np.ndarray, now: float) -> None:
        self.points_seen += 1
        act = np.flatnonzero(self.active)
        if act.size == 0:
            self._new(0, x, now)
            return
        mu = self.LS[act] / self.n[act, None]
        d2 = np.einsum("ij,ij->i", mu - x, mu - x)
        j = int(np.argmin(d2))
        slot = int(act[j])
        # boundary: radius_mult * RMS radius; singletons use the distance to the
        # nearest other MC (CluStream rule), both floored by min_radius
        if self.n[slot] * decay_factor(now, self.t[slot], self.params.half_life) > 1.5:
            bound = self.params.radius_mult * float(self.rms_radius(np.array([slot]))[0])
        elif act.size > 1:
            d2_other = np.delete(np.einsum("ij,ij->i", mu - mu[j], mu - mu[j]), j)
            bound = float(np.sqrt(d2_other.min())) / 2.0
        else:
            bound = self.params.min_radius
        bound = max(bound, self.params.min_radius)
        if d2[j] <= bound * bound:
            self._decay_slot(slot, now)
            self.n[slot] += 1.0
            self.LS[slot] += x
            self.SS[slot] += float(x @ x)
            self._touch(slot)
            return
        free = np.flatnonzero(~self.active)
        if free.size:
            self._new(int(free[0]), x, now)
            return
        # full: delete the lightest MC if it is stale, otherwise merge the two closest
        w = self.n[act] * decay_factor(now, self.t[act], self.params.half_life)
        lightest = int(act[np.argmin(w)])
        if w.min() < self.params.min_weight:
            self._delete(lightest)
            self._new(lightest, x, now)
            return
        a = int(act[np.argmin(self.nn_dist[act])])
        b = int(self.nn_idx[a])
        self._decay_slot(a, now)
        self._decay_slot(b, now)
        self.n[a] += self.n[b]
        self.LS[a] += self.LS[b]
        self.SS[a] += self.SS[b]
        self._touch(a)
        self._delete(b)
        self._new(b, x, now)

    @staticmethod
    def _closest_pair(act: np.ndarray, mu: np.ndarray) -> tuple[int, int]:
        sq = np.einsum("ij,ij->i", mu, mu)
        D = sq[:, None] + sq[None, :] - 2.0 * mu @ mu.T
        np.fill_diagonal(D, np.inf)
        i, j = np.unravel_index(int(np.argmin(D)), D.shape)
        return int(act[i]), int(act[j])

    def prune(self, now: float) -> None:
        act = np.flatnonzero(self.active)
        if act.size == 0:
            return
        w = self.n[act] * decay_factor(now, self.t[act], self.params.half_life)
        for slot in act[w < self.params.min_weight]:
            self._delete(int(slot))

    # ------------------------------------------------------------------ views
    def state(self) -> dict[int, tuple[float, np.ndarray, float, float]]:
        """Current CFs as {id: (n, LS, SS, t)} (not decayed to now)."""
        return {int(self.ids[s]): (float(self.n[s]), self.LS[s].copy(), float(self.SS[s]), float(self.t[s]))
                for s in np.flatnonzero(self.active)}

    def changed_since(self, counter: int) -> tuple[list[int], list[int]]:
        """Slots modified and ids deleted after the given modification counter."""
        slots = [int(s) for s in np.flatnonzero(self.active & (self.mod > counter))]
        dels = self.del_ids[bisect_right(self.del_counter, counter):]
        return slots, dels

    def total_weight(self, now: float) -> float:
        act = np.flatnonzero(self.active)
        return float((self.n[act] * decay_factor(now, self.t[act], self.params.half_life)).sum())

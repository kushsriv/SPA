"""When should an edge node send? FedCAST and the baseline send policies.

Every policy answers, once per tick, with None (stay silent) or a Decision.
Policies see the node (its micro-clusters and its exact copy of the server
view) and nothing else, so all of them are implementable on a real device.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from .macro import cf_cost, sqdist


@dataclass
class Decision:
    full: bool = False
    only_slots: list[int] | None = None
    kfed: bool = False


class Policy:
    name = "policy"

    def decide(self, node, now: float) -> Decision | None:
        raise NotImplementedError

    def on_sent(self, nbytes: int) -> None:
        pass

    def tick(self, now: float) -> None:
        pass


# ---------------------------------------------------------------- staleness
def staleness(cur, sent, centers: np.ndarray, radii: np.ndarray, rho: float = 1.0,
              far_factor: float = 3.0, r_floor: float = 0.1):
    """Objective-linked staleness of the server's copy of one node.

    cur, sent: (n, LS, SS) arrays decayed to now (current state, server copy).
    Returns (delta, novelty_mass, cost_term, mass_term), where

      cost_term = sum_j | J_j(cur) - J_j(sent) |                 (Prop. 1)
      mass_term = sum_j ||LS_j - LS^_j|| + ||c_j|| |n_j - n^_j|   (Prop. 2)
      delta     = cost_term + rho * mass_term

    J_j is the k-means cost of the CFs assigned to global centre c_j, and
    novelty_mass is the not-yet-reported mass lying outside every centre's
    radius (a cluster the server has not seen).
    """
    k = len(centers)
    r = np.maximum(radii, r_floor)

    def per_center(n, LS, SS):
        if len(n) == 0:
            return np.zeros(k), np.zeros(k), np.zeros((k, LS.shape[1] if LS.ndim == 2 else centers.shape[1])), 0.0
        mu = LS / np.maximum(n, 1e-12)[:, None]
        D = sqdist(mu, centers)
        lab = D.argmin(1)
        J = cf_cost(n, LS, SS, centers)[np.arange(len(n)), lab]
        Jk = np.bincount(lab, weights=J, minlength=k)
        nk = np.bincount(lab, weights=n, minlength=k)
        Lk = np.zeros((k, centers.shape[1]))
        np.add.at(Lk, lab, LS)
        far = float(n[np.sqrt(D[np.arange(len(n)), lab]) > far_factor * r[lab]].sum())
        return Jk, nk, Lk, far

    J1, n1, L1, far1 = per_center(*cur)
    J0, n0, L0, far0 = per_center(*sent)
    cost_term = float(np.abs(J1 - J0).sum())
    mass_term = float(np.linalg.norm(L1 - L0, axis=1).sum()
                      + (np.linalg.norm(centers, axis=1) * np.abs(n1 - n0)).sum())
    return cost_term + rho * mass_term, max(0.0, far1 - far0), cost_term, mass_term


def per_mc_staleness(node, now: float, centers: np.ndarray, radii: np.ndarray, rho: float = 1.0,
                     far_factor: float = 3.0, r_floor: float = 0.1):
    """Per-micro-cluster decomposition of the staleness score.

    For every MC id, e_id = |J_id(cur) - J_id(sent)| + rho * (||LS - LS^|| + ||c|| |n - n^|),
    where J_id is the MC's k-means cost at its nearest global centre. Because
    |J(S) - J(S^)| <= sum_id |J_id(cur) - J_id(sent)| (pairing MCs by id), the sum
    Delta = sum_id e_id upper-bounds the server's cost error (Prop. 1) and the
    centroid displacement (Prop. 2); an MC that has not changed since it was
    sent contributes exactly 0 because both copies decay identically.

    Returns (slots, e_slots, del_ids, e_dels, novelty_mass) for the dirty slots.
    """
    mc = node.mc
    slots, dels = node.dirty()
    if not slots and not dels:
        return [], np.zeros(0), [], np.zeros(0), 0.0
    r = np.maximum(radii, r_floor)
    cn = np.linalg.norm(centers, axis=1)
    hl = node.params.half_life
    from .microcluster import decay_factor

    def cost_of(n, LS, SS):
        mu = LS / np.maximum(n, 1e-12)[:, None]
        D = sqdist(mu, centers)
        lab = D.argmin(1)
        J = cf_cost(n, LS, SS, centers)[np.arange(len(n)), lab]
        return J, lab, np.sqrt(D[np.arange(len(n)), lab])

    e_slots = np.zeros(len(slots))
    novelty = 0.0
    if slots:
        sl = np.array(slots)
        f = decay_factor(now, mc.t[sl], hl)
        n1, L1, S1 = mc.n[sl] * f, mc.LS[sl] * f[:, None], mc.SS[sl] * f
        J1, lab1, dist1 = cost_of(n1, L1, S1)
        prev = [node.sent.get(int(i)) for i in mc.ids[sl]]
        has = np.array([p is not None for p in prev])
        n0 = np.zeros(len(sl))
        L0 = np.zeros_like(L1)
        S0 = np.zeros(len(sl))
        for j, pv in enumerate(prev):
            if pv is not None:
                f0 = decay_factor(now, pv[3], hl)
                n0[j], L0[j], S0[j] = pv[0] * f0, pv[1] * f0, pv[2] * f0
        J0 = np.zeros(len(sl))
        c0 = np.zeros(len(sl))
        if has.any():
            J0h, lab0, _ = cost_of(n0[has], L0[has], S0[has])
            J0[has] = J0h
            c0[has] = cn[lab0]
        cmax = np.maximum(cn[lab1], c0)
        e_slots = np.abs(J1 - J0) + rho * (np.linalg.norm(L1 - L0, axis=1) + cmax * np.abs(n1 - n0))
        far = dist1 > far_factor * r[lab1]
        novelty = float((n1 * far * ~has).sum())  # mass of new MCs outside every known cluster
    e_dels = np.zeros(len(dels))
    if dels:
        vals = [node.sent[i] for i in dels]
        f0 = decay_factor(now, np.array([v[3] for v in vals]), hl)
        n0 = np.array([v[0] for v in vals]) * f0
        L0 = np.array([v[1] for v in vals]) * f0[:, None]
        S0 = np.array([v[2] for v in vals]) * f0
        J0, lab0, _ = cost_of(n0, L0, S0)
        e_dels = J0 + rho * (np.linalg.norm(L0, axis=1) + cn[lab0] * n0)
    return slots, e_slots, dels, e_dels, novelty


# ---------------------------------------------------------------- FedCAST
class FedCAST(Policy):
    """Budget-constrained, link-aware, objective-linked send policy.

    1. Score every dirty micro-cluster by its contribution e_id to the
       staleness bound; Delta = sum e_id.
    2. Choose the smallest set of MCs (highest e first) that covers a fraction
       gamma of Delta, plus all deletions: payload b bytes, covered mass D_c.
    3. Send iff  D_c >= lambda * p * b  (or a novelty override: new mass
       outside every global cluster), and the token bucket holds b bytes.
    4. Once per budget window, lambda <- lambda * exp(eta * (spent - B) / B):
       exponentiated dual ascent on the byte budget, which is scale-free.
    """
    name = "fedcast"

    def __init__(self, budget: float, window: float = 10.0, rho: float = 1.0, eta: float = 0.5,
                 gamma: float = 0.9, novelty_frac: float = 0.01, novelty_min: float = 3.0,
                 far_factor: float = 3.0, use_price: bool = True, use_dual: bool = True,
                 use_novelty: bool = True, use_bucket: bool = True, full: bool = False,
                 lam0: float | None = None):
        self.B, self.W, self.rho, self.eta, self.gamma = budget, window, rho, eta, gamma
        self.novelty_frac, self.novelty_min, self.far_factor = novelty_frac, novelty_min, far_factor
        self.use_price, self.use_dual, self.use_novelty = use_price, use_dual, use_novelty
        self.use_bucket, self.full = use_bucket, full
        self.lam = lam0
        self.tokens: float | None = None
        self.cap = 0.0
        self.last_t = 0.0
        self.win_start = 0.0
        self.win_bytes = 0
        self.last_delta = 0.0

    def _refill(self, node, now: float) -> None:
        if self.tokens is None:
            self.cap = max(self.B, 1.05 * node.full_bytes_max())
            self.tokens = self.cap
        self.tokens = min(self.cap, self.tokens + (now - self.last_t) * self.B / self.W)
        self.last_t = now

    def decide(self, node, now: float) -> Decision | None:
        from .codec import KAFKA_RECORD_OVERHEAD, summary_size
        self._refill(node, now)
        g = node.global_model
        if g is None:  # bootstrap: the server has no model yet
            size = node.pending_bytes(full=True)
            ok = size > 0 and not node.sent and ((not self.use_bucket) or self.tokens >= size)
            return Decision(full=True) if ok else None
        slots, e_s, dels, e_d, novelty = per_mc_staleness(node, now, g.centers, g.radii,
                                                          self.rho, self.far_factor)
        if not slots and not dels:
            return None
        delta = float(e_s.sum() + e_d.sum())
        self.last_delta = delta
        if self.full:
            chosen = None
            size = node.pending_bytes(full=True)
            covered = delta
        else:
            order = np.argsort(-e_s)
            target = self.gamma * delta - float(e_d.sum())
            cum = np.cumsum(e_s[order])
            cnt = int(np.searchsorted(cum, target) + 1) if target > 0 else 0
            cnt = min(cnt, len(order))
            chosen = [slots[i] for i in order[:cnt]]
            covered = float(cum[cnt - 1] if cnt else 0.0) + float(e_d.sum())
            size = summary_size(len(chosen), len(dels), node.dim) + KAFKA_RECORD_OVERHEAD
            if not chosen and not dels:
                return None
        affordable = (not self.use_bucket) or self.tokens >= size
        p = node.link_price() if self.use_price else 1.0
        score = covered / (p * size)
        if self.lam is None:
            self.lam = max(score, 1e-12)
        mass = float(node.mc.total_weight(now))
        novel = self.use_novelty and novelty >= max(self.novelty_min, self.novelty_frac * mass)
        if (score >= self.lam or novel) and affordable:
            return Decision(full=self.full, only_slots=chosen)
        return None

    def on_sent(self, nbytes: int) -> None:
        if self.tokens is not None:
            self.tokens -= nbytes
        self.win_bytes += nbytes

    def tick(self, now: float) -> None:
        if now - self.win_start >= self.W:
            if self.use_dual and self.lam is not None:
                g = (self.win_bytes - self.B) / self.B
                self.lam = max(self.lam * math.exp(self.eta * min(max(g, -1.0), 2.0)), 1e-12)
            self.win_start = now
            self.win_bytes = 0


# ---------------------------------------------------------------- baselines
class Naive(Policy):
    """Send the full summary at every tick in which anything changed."""
    name = "naive"

    def decide(self, node, now):
        return Decision(full=True) if node.pending_bytes() > 0 else None


class Periodic(Policy):
    """Send the full summary every T seconds (classic periodic federation)."""
    name = "periodic"

    def __init__(self, period: float, full: bool = True):
        self.T = period
        self.full = full
        self.last = -math.inf

    def decide(self, node, now):
        if now - self.last >= self.T and node.mc.active.any():
            self.last = now
            if not self.full and node.pending_bytes() == 0:
                return None
            return Decision(full=self.full)
        return None


class ChangeThreshold(Policy):
    """Send only micro-clusters that were created, deleted, or changed by more
    than eps (relative centroid shift or relative weight change) - the
    distributed stream clustering rule of Tran (2013) / DGClust-style state
    change reporting."""
    name = "change"

    def __init__(self, eps: float, r_floor: float = 0.1):
        self.eps = eps
        self.r_floor = r_floor

    def decide(self, node, now):
        slots, dels = node.dirty()
        if not slots and not dels:
            return None
        mc = node.mc
        from .microcluster import decay_factor
        chosen = []
        for s in slots:
            i = int(mc.ids[s])
            if i not in node.sent:
                chosen.append(s)
                continue
            n0, L0, S0, t0 = node.sent[i]
            f0 = decay_factor(now, t0, node.params.half_life)
            f1 = decay_factor(now, mc.t[s], node.params.half_life)
            n1 = mc.n[s] * f1
            mu1, mu0 = mc.LS[s] / mc.n[s], L0 / n0
            rad = max(float(mc.rms_radius(np.array([s]))[0]), self.r_floor)
            if (np.linalg.norm(mu1 - mu0) / rad > self.eps
                    or abs(n1 - n0 * f0) / max(n0 * f0, 1e-9) > self.eps):
                chosen.append(s)
        if not chosen and not dels:
            return None
        return Decision(only_slots=chosen)


class NormTrigger(Policy):
    """Event-triggered communication in the style of EventGraD: send when the
    relative norm of the change of the (flattened) summary exceeds delta."""
    name = "norm"

    def __init__(self, rel: float):
        self.rel = rel

    def decide(self, node, now):
        if node.pending_bytes() == 0:
            return None
        ids1, n1, L1, S1 = node.current_arrays(now)
        ids0, n0, L0, S0 = node.sent_arrays(now)
        if len(ids0) == 0:
            return Decision()
        v1 = {int(i): np.concatenate([[n1[j], S1[j]], L1[j]]) for j, i in enumerate(ids1)}
        v0 = {int(i): np.concatenate([[n0[j], S0[j]], L0[j]]) for j, i in enumerate(ids0)}
        zero = np.zeros(node.dim + 2)
        diff = sum(float(np.sum((v1.get(i, zero) - v0.get(i, zero)) ** 2)) for i in set(v1) | set(v0))
        base = sum(float(np.sum(v ** 2)) for v in v0.values())
        return Decision() if math.sqrt(diff) > self.rel * math.sqrt(max(base, 1e-12)) else None


class KFed(Policy):
    """k-FED (Dennis et al., ICML 2021) run periodically on the stream: every T
    seconds the node sends k' local centres (as CFs) and the server clusters
    the union of all local centres."""
    name = "kfed"

    def __init__(self, period: float, k_local: int):
        self.T = period
        self.k_local = k_local
        self.last = -math.inf

    def decide(self, node, now):
        if now - self.last >= self.T and node.mc.active.any():
            self.last = now
            return Decision(kfed=True)
        return None

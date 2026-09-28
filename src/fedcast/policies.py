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


def per_mc_staleness(node, now: float, centers: np.ndarray | None, radii: np.ndarray | None,
                     rho: float = 1.0, far_factor: float = 3.0, r_floor: float = 0.1,
                     rank: str = "objective"):
    """Per-micro-cluster decomposition of the staleness score.

    For every MC id, e_id = |J_id(cur) - J_id(sent)| + rho * (||LS - LS^|| + ||c|| |n - n^|),
    where J_id is the MC's k-means cost at its nearest global centre. Because
    |J(S) - J(S^)| <= sum_id |J_id(cur) - J_id(sent)| (pairing MCs by id), the sum
    Delta = sum_id e_id upper-bounds the server's cost error (Prop. 1) and the
    centroid displacement (Prop. 2); an MC that has not changed since it was
    sent contributes exactly 0 because both copies decay identically.

    rank selects how micro-clusters are scored (and hence prioritised):
      "objective"  e_id as above (FedCAST; needs the global centres C)
      "norm"       ||(dn, dLS, dSS)||, i.e. top-k sparsification by change
                   magnitude with implicit error feedback (the standard
                   federated-learning compressor), same budget controller
      "uniform"    |dSS| + 2R||dLS|| + R^2|dn| with R the node's data radius:
                   a bound on |J(CF,c) - J(CF^,c)| that holds for EVERY centre
                   with ||c|| <= R, so it needs no global model (no downlink)
      "hybrid"     objective + norm, each normalised to sum 1 (E11: worse than
                   objective alone; kept only to reproduce that negative result)

    Returns (slots, e_slots, del_ids, e_dels, novelty_mass) for the dirty slots.
    """
    mc = node.mc
    slots, dels = node.dirty()
    if not slots and not dels:
        return [], np.zeros(0), [], np.zeros(0), 0.0
    hl = node.params.half_life
    if rank == "hybrid":
        # scale-free blend: each signal normalised to sum 1 over the candidates, then added,
        # so an MC ranks high if it matters for the current model OR is a large change
        s1, eo_s, d1, eo_d, nov = per_mc_staleness(node, now, centers, radii, rho, far_factor, r_floor, "objective")
        _, en_s, _, en_d, _ = _per_mc_simple(node, now, slots, dels, "norm")
        zo = max(eo_s.sum() + eo_d.sum(), 1e-12)
        zn = max(en_s.sum() + en_d.sum(), 1e-12)
        return s1, eo_s / zo + en_s / zn, d1, eo_d / zo + en_d / zn, nov
    if rank != "objective":
        return _per_mc_simple(node, now, slots, dels, rank)
    r = np.maximum(radii, r_floor)
    cn = np.linalg.norm(centers, axis=1)
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


def _cf_pairs(node, now: float, slots, dels):
    """Decayed (current, server-copy) CF arrays for dirty slots and for deletions."""
    from .microcluster import decay_factor
    mc, hl, d = node.mc, node.params.half_life, node.dim
    sl = np.array(slots, dtype=int)
    f = decay_factor(now, mc.t[sl], hl) if len(sl) else np.zeros(0)
    n1, L1, S1 = mc.n[sl] * f, mc.LS[sl] * f[:, None], mc.SS[sl] * f
    n0, L0, S0 = np.zeros(len(sl)), np.zeros((len(sl), d)), np.zeros(len(sl))
    for j, i in enumerate(mc.ids[sl]):
        pv = node.sent.get(int(i))
        if pv is not None:
            f0 = decay_factor(now, pv[3], hl)
            n0[j], L0[j], S0[j] = pv[0] * f0, pv[1] * f0, pv[2] * f0
    vals = [node.sent[i] for i in dels]
    fd = decay_factor(now, np.array([v[3] for v in vals]), hl) if vals else np.zeros(0)
    nd = np.array([v[0] for v in vals]) * fd if vals else np.zeros(0)
    Ld = np.array([v[1] for v in vals]) * fd[:, None] if vals else np.zeros((0, d))
    Sd = np.array([v[2] for v in vals]) * fd if vals else np.zeros(0)
    return (n1, L1, S1), (n0, L0, S0), (nd, Ld, Sd)


def _per_mc_simple(node, now: float, slots, dels, rank: str):
    (n1, L1, S1), (n0, L0, S0), (nd, Ld, Sd) = _cf_pairs(node, now, slots, dels)
    dn, dL, dS = n1 - n0, np.linalg.norm(L1 - L0, axis=1), S1 - S0
    if rank == "norm":
        e_s = np.sqrt(dn ** 2 + dL ** 2 + dS ** 2)
        e_d = np.sqrt(nd ** 2 + np.linalg.norm(Ld, axis=1) ** 2 + Sd ** 2)
    elif rank == "uniform":
        act = np.flatnonzero(node.mc.active)
        mu = node.mc.LS[act] / np.maximum(node.mc.n[act], 1e-12)[:, None]
        R = float(np.linalg.norm(mu, axis=1).max()) if len(act) else 1.0
        e_s = np.abs(dS) + 2 * R * dL + R * R * np.abs(dn)
        e_d = np.abs(Sd) + 2 * R * np.linalg.norm(Ld, axis=1) + R * R * np.abs(nd)
    else:
        raise ValueError(rank)
    return slots, e_s, dels, e_d, 0.0


def _per_mc_lloyd(node, now: float, slots, dels, centers: np.ndarray, weights: np.ndarray,
                  with_assignment: bool = False):
    """Exact value of an update: the reduction of the server's Lloyd excess cost.

    With assignments fixed, the server's centre for cluster j is c_j = L^_j / N^_j, while
    the true centroid is (L^_j + dL_j) / (N^_j + dN_j). The k-means cost of cluster j at
    the server's centre exceeds its optimum by exactly

        excess_j = || dL_j - dN_j c_j ||^2 / N_j        (N_j: global mass of cluster j)

    The node knows c_j and N_j (broadcast), and its own contribution r_j to dL_j - dN_j c_j.
    Sending micro-cluster id removes its contribution u_id from r_j, reducing the excess by
    (2 u_id . r_j - ||u_id||^2) / N_j: its marginal value. Growth of a cluster in place
    (dLS ~ dn * c_j) is worth ~0; new mass far from every centre is worth a lot.
    with_assignment adds |dJ_id| (the cost change at the current centres, same units).
    """
    (n1, L1, S1), (n0, L0, S0), (nd, Ld, Sd) = _cf_pairs(node, now, slots, dels)
    k, d = centers.shape
    N = np.maximum(np.asarray(weights, float), 1e-9)

    def lab_of(n, L):
        if len(n) == 0:
            return np.zeros(0, int)
        return sqdist(L / np.maximum(n, 1e-12)[:, None], centers).argmin(1)
    has = n0 > 0
    a1, a0, ad = lab_of(n1, L1), np.zeros(len(n0), int), lab_of(nd, Ld)
    if has.any():
        a0[has] = lab_of(n0[has], L0[has])
    # per-slot contributions (up to two clusters touched) and per-deletion contributions
    U1 = L1 - n1[:, None] * centers[a1]                 # current copy, in its cluster
    U0 = np.where(has[:, None], L0 - n0[:, None] * centers[a0], 0.0)   # server copy
    Ud = -(Ld - nd[:, None] * centers[ad]) if len(nd) else np.zeros((0, d))
    r = np.zeros((k, d))
    np.add.at(r, a1, U1)
    np.add.at(r, a0, -U0)
    if len(nd):
        np.add.at(r, ad, Ud)

    def gain(j, u):
        return (2.0 * (u * r[j]).sum(-1) - (u * u).sum(-1)) / N[j]
    same = a1 == a0
    u_same = U1 - U0
    e_s = np.where(same, gain(a1, u_same), gain(a1, U1) + gain(a0, -U0) * has)
    e_d = gain(ad, Ud) if len(nd) else np.zeros(0)
    e_s, e_d = np.maximum(e_s, 0.0), np.maximum(e_d, 0.0)
    if with_assignment:
        cst = lambda n, L, S: cf_cost(n, L, S, centers).min(1) if len(n) else np.zeros(0)
        e_s = e_s + np.abs(cst(n1, L1, S1) - np.where(has, cst(np.where(has, n0, 1.0), L0, S0), 0.0))
        e_d = e_d + cst(nd, Ld, Sd)
    return slots, e_s, dels, e_d, 0.0


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
                 lam0: float | None = None, rank: str = "objective", tie_eps: float = 0.1,
                 use_overflow: bool = False, overflow_frac: float = 0.95):
        self.rank = rank
        self.tie_eps = tie_eps
        # use-it-or-lose-it: tokens above the bucket capacity are lost, so when the bucket is
        # about to overflow a send has no opportunity cost and the threshold is waived
        self.use_overflow, self.overflow_frac = use_overflow, overflow_frac
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
        if g is None and self.rank in ("objective", "hybrid", "lloyd", "lloydj", "lloyd+"):  # bootstrap: the server has no model yet
            size = node.pending_bytes(full=True)
            ok = size > 0 and not node.sent and ((not self.use_bucket) or self.tokens >= size)
            return Decision(full=True) if ok else None
        if self.rank in ("lloyd", "lloydj", "lloyd+"):
            slots, dels = node.dirty()
            if not slots and not dels:
                return None
            slots, e_s, dels, e_d, novelty = _per_mc_lloyd(node, now, slots, dels, g.centers, g.weights,
                                                           with_assignment=self.rank == "lloydj")
            if self.rank == "lloyd+":
                # value first, information second: exact value dominates the ranking, and change
                # magnitude (weight tie_eps) breaks ties among zero-value refinements, which are
                # then sent only when the dual threshold says the budget is abundant
                _, m_s, _, m_d, _ = _per_mc_simple(node, now, slots, dels, "norm")
                zv = e_s.sum() + e_d.sum()
                zm = max(m_s.sum() + m_d.sum(), 1e-12)
                scale = zv if zv > 0 else 1.0
                e_s = e_s + self.tie_eps * scale * m_s / zm
                e_d = e_d + self.tie_eps * scale * m_d / zm
        else:
            slots, e_s, dels, e_d, novelty = per_mc_staleness(
                node, now, g.centers if g is not None else None, g.radii if g is not None else None,
                self.rho, self.far_factor, rank=self.rank)
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
            size = node.summary_bytes(len(chosen), len(dels))
            if not chosen and not dels:
                return None
        affordable = (not self.use_bucket) or self.tokens >= size
        p = node.link_price() if self.use_price else 1.0
        score = covered / (p * size)
        if self.lam is None:
            self.lam = max(score, 1e-12)
        mass = float(node.mc.total_weight(now))
        novel = self.use_novelty and novelty >= max(self.novelty_min, self.novelty_frac * mass)
        overflow = self.use_overflow and self.tokens is not None and self.tokens >= self.overflow_frac * self.cap
        if (score >= self.lam or novel or overflow) and affordable:
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


# ---------------------------------------------------------------- multi-resolution
def coarsen_loss(n: np.ndarray, LS: np.ndarray, SS: np.ndarray, r: int, rng) -> tuple[float, int]:
    """Relative increase of within-cluster scatter when the node's CFs are merged into r
    groups (weighted k-means on centroids, exact CF sums). 0 = lossless."""
    from .macro import weighted_kmeans
    within = lambda a, b, c: float(np.maximum(c - np.einsum("ij,ij->i", b, b) / np.maximum(a, 1e-12), 0).sum())
    base = within(n, LS, SS)
    m = len(n)
    if r >= m:
        return 0.0, m
    mu = LS / np.maximum(n, 1e-12)[:, None]
    _, lab, _ = weighted_kmeans(mu, n, r, rng, restarts=1)
    gn = np.bincount(lab, weights=n, minlength=r)
    gL = np.zeros((r, LS.shape[1]))
    np.add.at(gL, lab, LS)
    gS = np.bincount(lab, weights=SS, minlength=r)
    keep = gn > 0
    return within(gn[keep], gL[keep], gS[keep]) / max(base, 1e-12) - 1.0, int(keep.sum())


class FedCASTMR(FedCAST):
    """FedCAST with fidelity-controlled multi-resolution summaries.

    Under a tight budget (a fine full summary costs more than `horizon` seconds of
    budget) the node sends *compressed full summaries*: its q fine micro-clusters
    merged into the smallest number r of CF groups whose within-cluster scatter
    grows by at most eps (fidelity chosen from the node's own data, no oracle k').
    When to send is decided exactly as in FedCAST: set-level objective staleness
    between the node's current fine state and what the server holds, against
    lambda * p * b, with dual ascent and the token bucket. Under a loose budget
    it behaves as FedCAST (prioritised fine deltas).

    This unifies k-FED (coarse, periodic, oracle k') and delta streaming (fine,
    frequent) under one budget-driven policy.
    """
    name = "fedcast-mr"
    CANDIDATES = (2, 3, 4, 6, 8, 12, 16, 24, 32, 48)

    def __init__(self, budget: float, eps: float = 0.5, horizon: float = 60.0, seed: int = 0, **kw):
        super().__init__(budget, **kw)
        self.eps, self.horizon = eps, horizon
        self.k_local = None
        self.rng = np.random.default_rng(seed + 99)
        self.coarse_mode: bool | None = None

    def _pick_r(self, node, now: float) -> int:
        _, n, LS, SS = node.current_arrays(now)
        for r in self.CANDIDATES:
            if r >= len(n):
                break
            loss, eff = coarsen_loss(n, LS, SS, r, self.rng)
            if loss <= self.eps:
                return eff
        return len(n)

    def decide(self, node, now: float) -> Decision | None:
        from .codec import KAFKA_RECORD_OVERHEAD, summary_size
        if self.coarse_mode is None:
            self.coarse_mode = node.full_bytes_max() > self.B / self.W * self.horizon
        if not self.coarse_mode:
            return super().decide(node, now)
        self._refill(node, now)
        if not node.mc.active.any():
            return None
        if self.k_local is None:
            self.k_local = self._pick_r(node, now)
        size = node.summary_bytes(self.k_local, 0)
        affordable = (not self.use_bucket) or self.tokens >= size
        g = node.global_model
        if g is None:
            return Decision(kfed=True) if (affordable and not node.sent) else None
        cur = node.current_arrays(now)[1:]
        old = node.sent_arrays(now)[1:]
        delta, _, _, _ = staleness(cur, old, g.centers, g.radii, self.rho, self.far_factor)
        p = node.link_price() if self.use_price else 1.0
        score = delta / (p * size)
        if self.lam is None:
            self.lam = max(score, 1e-12)
        if score >= self.lam and affordable:
            self.k_local = self._pick_r(node, now)   # refresh resolution at send time
            new_size = node.summary_bytes(self.k_local, 0)
            if (not self.use_bucket) or self.tokens >= new_size:
                return Decision(kfed=True)
        return None

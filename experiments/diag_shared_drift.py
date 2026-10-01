"""Diagnostic (D2): is drift shared across nodes, and would redundancy-aware sending help?

  python experiments/diag_shared_drift.py <setting> <budget>

coherence = ||sum_i r_ij||^2 / sum_i ||r_ij||^2 over clusters (server excess vs the sum of what
nodes see locally); transl/dir = share of each node residual explained by a leave-one-out
shared translation / direction. (excess_cut is not meaningful: the leave-one-out predictor
sums back to the total by construction.)
"""
import sys, json, numpy as np
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "src"))
from dataclasses import replace
from fedcast.sim import SimConfig, run
from fedcast.data import load
from fedcast.macro import sqdist

def make_probe(acc, start):
    def probe(t, nodes, coord):
        if t < start: return
        C = coord.model.centers; W = np.maximum(np.asarray(coord.model.weights, float), 1e-9)
        k, d = C.shape
        R = []; M = []
        for nd in nodes:
            r = np.zeros((k, d)); m = np.zeros(k)
            for arrs, sgn in ((nd.current_arrays(t), 1.0), (nd.sent_arrays(t), -1.0)):
                _, n, L, _ = arrs
                if len(n) == 0: continue
                a = sqdist(L / np.maximum(n, 1e-12)[:, None], C).argmin(1)
                np.add.at(r, a, sgn * (L - n[:, None] * C[a]))
                if sgn > 0: np.add.at(m, a, n)
            R.append(r); M.append(m)
        R = np.array(R); M = np.array(M)          # (nodes, k, d), (nodes, k)
        tot = (np.square(R.sum(0)).sum(1) / W).sum()
        ind = (np.square(R).sum(2).sum(0) / W).sum()
        # leave-one-out common translation per unit mass
        Rs, Ms = R.sum(0), M.sum(0)
        delta = (Rs[None] - R) / np.maximum(Ms[None] - M, 1e-9)[..., None]
        Rh = M[..., None] * delta
        res = R - Rh
        expl = 1 - np.square(res).sum() / max(np.square(R).sum(), 1e-12)
        after = (np.square(res.sum(0)).sum(1) / W).sum()
        # direction-only: project each node residual on LOO direction
        nrm = np.linalg.norm(Rs[None] - R, axis=2, keepdims=True)
        v = (Rs[None] - R) / np.maximum(nrm, 1e-12)
        proj = (R * v).sum(2, keepdims=True) * v
        dexpl = np.square(proj).sum() / max(np.square(R).sum(), 1e-12)
        acc.append((tot, ind, expl, after, dexpl))
    return probe

SET = {
 "pendigits-drift": ("pendigits", dict(partition="drift:2")),
 "syndrift-dir": ("syndrift", dict(partition="dirichlet:0.3")),
 "intel-natural": ("intel", dict(partition="natural")),
 "gas": ("gas", dict(partition="dirichlet:0.3")),
 "covtype": ("covtype", dict(partition="dirichlet:0.3")),
 "pendigits-evolve": ("pendigits", dict(partition="evolve:0.3")),
}
name, B = sys.argv[1], float(sys.argv[2])
dsn, kw = SET[name]
ds = load(dsn, max_points=60000)
out = {}
for seed in range(3):
    cfg = SimConfig(duration=300.0, nodes=10, links=["wan", "cellular"], method="fedcast", param=B, seed=seed, rank="lloyd+", quant=True, overflow=True, **kw)
    acc = []
    r = run(cfg, ds, probe=make_probe(acc, cfg.warmup * cfg.duration))
    a = np.array(acc)
    out[seed] = dict(coherence=float(a[:,0].sum()/a[:,1].sum()), transl_explained=float(np.mean(a[:,2])),
                     excess_cut=float(1 - a[:,3].sum()/a[:,0].sum()), dir_explained=float(np.mean(a[:,4])),
                     ssq=r["ssq"], bytes=r["bytes_up"])
print(name, B, json.dumps(out))

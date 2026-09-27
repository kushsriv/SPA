"""Datasets. Labels are only ever used for evaluation, never for clustering.

Real datasets are fetched from GitHub-hosted mirrors (NSL-KDD from the widely
used defcom17 mirror; Shuttle, Pen-Digits and Letter from the PMLB benchmark
suite) and cached under data/raw/. Features are standardised with dataset-level
statistics, the usual stream-clustering benchmark assumption of known sensor
ranges.
"""
from __future__ import annotations

import gzip
import io
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import numpy as np

CACHE = Path(__file__).resolve().parents[2] / "data" / "raw"

NSLKDD_URL = "https://raw.githubusercontent.com/defcom17/NSL_KDD/master/KDDTrain+.txt"
PMLB_URL = "https://media.githubusercontent.com/media/EpistasisLab/pmlb/master/datasets/{0}/{0}.tsv.gz"


@dataclass
class Dataset:
    name: str
    X: np.ndarray
    y: np.ndarray
    u: np.ndarray | None = None      # optional time fraction in [0, 1] per point
    class_names: list[str] | None = None

    @property
    def k(self) -> int:
        return int(len(np.unique(self.y)))


def _fetch(url: str, name: str) -> bytes:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / name
    if not path.exists():
        with urllib.request.urlopen(url, timeout=120) as r:
            path.write_bytes(r.read())
    return path.read_bytes()


def _standardise(X: np.ndarray) -> np.ndarray:
    X = X.astype(np.float64)
    sd = X.std(0)
    keep = sd > 1e-12
    return (X[:, keep] - X[:, keep].mean(0)) / sd[keep]


# ---------------------------------------------------------------- NSL-KDD
_ATTACK = {
    "normal": "normal",
    **{a: "dos" for a in ["back", "land", "neptune", "pod", "smurf", "teardrop", "apache2",
                          "mailbomb", "processtable", "udpstorm"]},
    **{a: "probe" for a in ["ipsweep", "nmap", "portsweep", "satan", "mscan", "saint"]},
    **{a: "r2l" for a in ["ftp_write", "guess_passwd", "imap", "multihop", "phf", "spy",
                          "warezclient", "warezmaster", "sendmail", "named", "snmpgetattack",
                          "snmpguess", "xlock", "xsnoop", "httptunnel"]},
    **{a: "u2r" for a in ["buffer_overflow", "loadmodule", "perl", "rootkit", "ps",
                          "sqlattack", "xterm"]},
}


def nslkdd() -> Dataset:
    raw = _fetch(NSLKDD_URL, "KDDTrain+.txt").decode()
    rows = [r.split(",") for r in raw.strip().splitlines()]
    proto = sorted({r[1] for r in rows})
    flag = sorted({r[3] for r in rows})
    num_idx = [i for i in range(41) if i not in (1, 2, 3)]
    Xn = np.array([[float(r[i]) for i in num_idx] for r in rows])
    Xn = np.log1p(np.maximum(Xn, 0.0))  # heavy-tailed byte and count features
    P = np.array([[r[1] == p for p in proto] for r in rows], float)
    F = np.array([[r[3] == f for f in flag] for r in rows], float)
    X = _standardise(np.hstack([Xn, P, F]))
    names = ["normal", "dos", "probe", "r2l", "u2r"]
    y = np.array([names.index(_ATTACK.get(r[41], "r2l")) for r in rows])
    return Dataset("nslkdd", X, y, class_names=names)


# ---------------------------------------------------------------- PMLB
def pmlb(name: str) -> Dataset:
    raw = gzip.decompress(_fetch(PMLB_URL.format(name), f"{name}.tsv.gz"))
    header = raw.split(b"\n", 1)[0].decode().split("\t")
    A = np.loadtxt(io.BytesIO(raw), delimiter="\t", skiprows=1)
    t = header.index("target")
    y_raw = A[:, t]
    X = _standardise(np.delete(A, t, axis=1))
    _, y = np.unique(y_raw, return_inverse=True)
    return Dataset(name, X, y)


# ---------------------------------------------------------------- synthetic
def synthetic(n: int = 60000, k: int = 15, d: int = 10, seed: int = 0, sep: float = 3.5,
              drift_clusters: int = 4, drift_dist: float = 5.0,
              emerging: tuple[float, ...] = (0.35, 0.65), vanishing: tuple[float, ...] = (0.5,)) -> Dataset:
    """Evolving anisotropic Gaussian mixture ("SynDrift").

    - k clusters with random orientation, per-axis scales in [0.3, 1.5] and
      unequal sizes (Dirichlet(2) mixing weights), centres at least `sep` apart;
    - drift_clusters clusters move drift_dist units along a line (gradual drift);
    - one cluster per value in `emerging` only exists after that time fraction
      (sudden new concepts), one per value in `vanishing` stops at that fraction.
    u (time fraction) is returned so the partitioner respects the evolution.
    """
    rng = np.random.default_rng(seed)
    centers = []
    while len(centers) < k:
        c = rng.uniform(-7, 7, d)
        if all(np.linalg.norm(c - o) > sep for o in centers):
            centers.append(c)
    centers = np.array(centers)
    rot = [np.linalg.qr(rng.normal(size=(d, d)))[0] for _ in range(k)]
    scales = rng.uniform(0.3, 1.5, (k, d))
    velocity = np.zeros_like(centers)
    movers = rng.choice(k, size=min(drift_clusters, k), replace=False)
    for j in movers:
        v = rng.normal(size=d)
        velocity[j] = drift_dist * v / np.linalg.norm(v)
    y = rng.choice(k, n, p=rng.dirichlet(2.0 * np.ones(k)))
    u = rng.uniform(0, 1, n)
    special = [c for c in range(k) if c not in movers]
    for j, start in enumerate(emerging):
        c = special[j]
        m = y == c
        u[m] = rng.uniform(start, 1.0, m.sum())
    for j, stop in enumerate(vanishing):
        c = special[len(emerging) + j]
        m = y == c
        u[m] = rng.uniform(0.0, stop, m.sum())
    Z = rng.normal(size=(n, d)) * scales[y]
    X = centers[y] + velocity[y] * u[:, None] + np.einsum("nij,nj->ni", np.stack(rot)[y], Z)
    return Dataset("syndrift", X, y, u=u)


def load(name: str, seed: int = 0, max_points: int | None = None) -> Dataset:
    if name in ("synthetic", "syndrift"):
        ds = synthetic(seed=seed)
    elif name == "nslkdd":
        ds = nslkdd()
    elif name in ("shuttle", "pendigits", "letter"):
        ds = pmlb(name)
    else:
        raise ValueError(f"unknown dataset {name}")
    if max_points and len(ds.y) > max_points:
        idx = np.random.default_rng(seed).choice(len(ds.y), max_points, replace=False)
        idx.sort()
        ds = Dataset(ds.name, ds.X[idx], ds.y[idx], None if ds.u is None else ds.u[idx], ds.class_names)
    return ds

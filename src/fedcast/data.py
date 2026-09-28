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
    groups: np.ndarray | None = None  # optional natural device id per point (partition "natural")
    k_override: int | None = None     # number of clusters when labels do not define it

    @property
    def k(self) -> int:
        return int(self.k_override or len(np.unique(self.y)))


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


# ---------------------------------------------------------------- real drifting streams
INTEL_URL = "https://raw.githubusercontent.com/linsea423/Intel_Lab_Data/master/data.zip"
GAS_URL = "https://raw.githubusercontent.com/HakanKARASU/Gas-Sensor-Array-Drift-Dataset/main/gas_sensor_data.csv"
COVTYPE_URL = "https://raw.githubusercontent.com/vlosing/driftDatasets/master/realWorld/covType/covType.arff"


def _subsample_ordered(n: int, max_points: int) -> np.ndarray:
    return np.arange(n) if n <= max_points else np.linspace(0, n - 1, max_points).astype(int)


def intel_lab(max_points: int = 60000, k: int = 8) -> Dataset:
    """Intel Berkeley Research Lab: 54 motes, 5 weeks, temperature/humidity/light/voltage.
    Naturally partitioned (by mote) and naturally drifting (daily cycles, battery decay,
    sensor faults). Unlabelled: evaluated with the k-means objective only."""
    import zipfile
    import pandas as pd
    raw = _fetch(INTEL_URL, "intel_data.zip")
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        df = pd.read_csv(z.open("data.txt"), sep=r"\s+", header=None, on_bad_lines="skip",
                         names=["date", "time", "epoch", "mote", "temp", "hum", "light", "volt"])
    df = df.dropna()
    df = df[(df.mote >= 1) & (df.mote <= 54) & df.temp.between(-10, 60) & df.hum.between(0, 100)
            & df.light.between(0, 3000) & df.volt.between(2.0, 3.3)]
    ts = pd.to_datetime(df.date + " " + df.time, errors="coerce", format="mixed")
    df = df.assign(ts=ts).dropna(subset=["ts"]).sort_values("ts")
    idx = _subsample_ordered(len(df), max_points)
    df = df.iloc[idx]
    X = _standardise(np.column_stack([df.temp, df.hum, np.log1p(df.light), df.volt]))
    t = (df.ts - df.ts.iloc[0]).dt.total_seconds().to_numpy()
    u = t / max(t[-1], 1.0)
    return Dataset("intel", X, np.zeros(len(df), int), u=u, groups=df.mote.to_numpy().astype(int), k_override=k)


def gas_drift(max_points: int = 60000) -> Dataset:
    """UCI Gas Sensor Array Drift (36 months, 10 batches, 6 gases, 128 features) in natural order."""
    import pandas as pd
    df = pd.read_csv(io.BytesIO(_fetch(GAS_URL, "gas_sensor_data.csv")))
    df = df.iloc[_subsample_ordered(len(df), max_points)]
    F = df[[c for c in df.columns if c.startswith("feature_")]].to_numpy(float)
    X = _standardise(np.sign(F) * np.log1p(np.abs(F)))
    _, y = np.unique(df.gas_label.to_numpy(), return_inverse=True)
    u = np.arange(len(df)) / max(len(df) - 1, 1)
    return Dataset("gas", X, y, u=u)


def covtype_natural(max_points: int = 60000) -> Dataset:
    """Forest CoverType in its original order (a standard real concept-drift stream)."""
    raw = _fetch(COVTYPE_URL, "covType.arff").decode(errors="ignore")
    body = raw[raw.lower().index("@data") + 5:]
    rows = [r for r in body.strip().splitlines() if r and not r.startswith("%")]
    idx = _subsample_ordered(len(rows), max_points)
    A = np.array([[float(v) for v in rows[i].split(",")] for i in idx])
    X = _standardise(A[:, :-1])
    _, y = np.unique(A[:, -1], return_inverse=True)
    u = np.arange(len(idx)) / max(len(idx) - 1, 1)
    return Dataset("covtype", X, y, u=u)


def _cached(name: str, builder, max_points: int) -> Dataset:
    """Cache processed arrays as .npz so repeated loads (and containers) skip the parsing."""
    path = CACHE / f"{name}_{max_points}.npz"
    if path.exists():
        z = np.load(path, allow_pickle=False)
        return Dataset(name, z["X"], z["y"], u=z["u"], groups=z["groups"] if z["groups"].size else None,
                       k_override=int(z["k"]) or None)
    ds = builder(max_points)
    CACHE.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, X=ds.X, y=ds.y, u=ds.u, groups=ds.groups if ds.groups is not None else np.zeros(0),
                        k=ds.k_override or 0)
    return ds


def load(name: str, seed: int = 0, max_points: int | None = None) -> Dataset:
    if name in ("synthetic", "syndrift"):
        ds = synthetic(seed=seed)
    elif name == "nslkdd":
        ds = nslkdd()
    elif name in ("shuttle", "pendigits", "letter"):
        ds = pmlb(name)
    elif name == "intel":
        return _cached("intel", intel_lab, max_points or 60000)
    elif name == "gas":
        return _cached("gas", gas_drift, max_points or 60000)
    elif name == "covtype":
        return _cached("covtype", covtype_natural, max_points or 60000)
    else:
        raise ValueError(f"unknown dataset {name}")
    if max_points and len(ds.y) > max_points:
        idx = np.random.default_rng(seed).choice(len(ds.y), max_points, replace=False)
        idx.sort()
        ds = Dataset(ds.name, ds.X[idx], ds.y[idx], None if ds.u is None else ds.u[idx], ds.class_names)
    return ds

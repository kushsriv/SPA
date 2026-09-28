"""Compact binary wire format for Kafka messages.

Fixed-layout little-endian records (struct + numpy) instead of JSON, msgpack,
Avro or Protobuf: sizes are exact and known *before* serialising (the send
policy needs the byte cost b of a pending delta), decoding is zero-copy, and
there is no schema registry to run on a laptop.

Message kinds
  SUMMARY (delta or full)   node -> coordinator   topic fsc.summaries / fsc.snapshots
  GLOBAL                    coordinator -> nodes  topic fsc.global
  RAW                       node -> coordinator   topic fsc.raw (centralised baseline)
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

import numpy as np

KIND_DELTA, KIND_FULL, KIND_GLOBAL, KIND_RAW = 1, 2, 3, 4
KIND_DELTA_Q, KIND_FULL_Q = 5, 6   # quantised variants (8-bit centroids)

# kind, flags, node, seq, time, n_upserts, n_deletes, dim
_SUMMARY_HDR = struct.Struct("<BBHIdHHH")
# kind, version, time, k, dim
_GLOBAL_HDR = struct.Struct("<BIdHH")
# kind, node, seq, time, count, dim
_RAW_HDR = struct.Struct("<BHIdHH")

# Kafka record-level overhead that our payload does not include: key bytes plus
# the v2 record header (attributes, timestamp/offset deltas, lengths, headers).
# Batch headers are amortised and ignored. Validated against producer stats.
KAFKA_RECORD_OVERHEAD = 24


def summary_size(n_up: int, n_del: int, dim: int) -> int:
    return _SUMMARY_HDR.size + n_up * (4 + 4 + 4 + 8 + 4 * dim) + 4 * n_del


def summary_size_q(n_up: int, n_del: int, dim: int) -> int:
    """Quantised record: id u32, n f32, variance f16, t-offset f32, centroid u8[d];
    per message: lo/span f16[d] (only when there are records)."""
    return _SUMMARY_HDR.size + (4 * dim if n_up else 0) + n_up * (4 + 4 + 2 + 4 + dim) + 4 * n_del


def global_size(k: int, dim: int) -> int:
    return _GLOBAL_HDR.size + k * (4 * dim + 4 + 4)


def raw_size(count: int, dim: int) -> int:
    return _RAW_HDR.size + count * 4 * dim


@dataclass
class Summary:
    kind: int
    node: int
    seq: int
    time: float
    ids: np.ndarray     # (u,) int
    n: np.ndarray       # (u,)
    LS: np.ndarray      # (u, d)
    SS: np.ndarray      # (u,)
    t: np.ndarray       # (u,) last-update times
    deleted: np.ndarray  # (r,) int

    @property
    def dim(self) -> int:
        return self.LS.shape[1]


def encode_summary(s: Summary) -> bytes:
    u, d = s.LS.shape
    head = _SUMMARY_HDR.pack(s.kind, 0, s.node, s.seq, s.time, u, len(s.deleted), d)
    return b"".join([
        head,
        np.asarray(s.ids, "<u4").tobytes(),
        np.asarray(s.n, "<f4").tobytes(),
        np.asarray(s.SS, "<f4").tobytes(),
        np.asarray(s.t, "<f8").tobytes(),
        np.asarray(s.LS, "<f4").tobytes(),
        np.asarray(s.deleted, "<u4").tobytes(),
    ])


def encode_summary_q(s: Summary) -> bytes:
    """8-bit centroid quantisation relative to the message's own range; the receiver
    reconstructs LS = n * mu^ and SS = n * (var + ||mu^||^2)."""
    u, d = s.LS.shape
    kind = KIND_FULL_Q if s.kind == KIND_FULL else KIND_DELTA_Q
    head = _SUMMARY_HDR.pack(kind, 0, s.node, s.seq, s.time, u, len(s.deleted), d)
    if u == 0:
        return head + np.asarray(s.deleted, "<u4").tobytes()
    n = np.maximum(np.asarray(s.n, float), 1e-12)
    mu = s.LS / n[:, None]
    lo16 = np.asarray(mu.min(0), "<f2")
    lo = lo16.astype(np.float64)
    hi = mu.max(0)
    span16 = np.asarray(np.where(hi - lo > 1e-6, (hi - lo) * 1.001, 1.0), "<f2")
    span = span16.astype(np.float64)
    q = np.clip(np.rint((mu - lo) / span * 255.0), 0, 255).astype(np.uint8)
    var = np.maximum(s.SS / n - np.einsum("ij,ij->i", mu, mu), 0.0)
    return b"".join([
        head,
        lo16.tobytes(), span16.tobytes(),
        np.asarray(s.ids, "<u4").tobytes(),
        np.asarray(s.n, "<f4").tobytes(),
        np.asarray(var, "<f2").tobytes(),
        np.asarray(s.t - s.time, "<f4").tobytes(),
        q.tobytes(),
        np.asarray(s.deleted, "<u4").tobytes(),
    ])


def decode_summary(buf: bytes) -> Summary:
    kind, _, node, seq, time, u, r, d = _SUMMARY_HDR.unpack_from(buf, 0)
    if kind in (KIND_DELTA_Q, KIND_FULL_Q):
        return _decode_summary_q(buf, kind, node, seq, time, u, r, d)
    o = _SUMMARY_HDR.size

    def take(dtype: str, count: int):
        nonlocal o
        a = np.frombuffer(buf, dtype=dtype, count=count, offset=o)
        o += a.nbytes
        return a

    ids = take("<u4", u).astype(np.int64)
    n = take("<f4", u).astype(np.float64)
    SS = take("<f4", u).astype(np.float64)
    t = take("<f8", u).copy()
    LS = take("<f4", u * d).astype(np.float64).reshape(u, d)
    dels = take("<u4", r).astype(np.int64)
    return Summary(kind, node, seq, time, ids, n, LS, SS, t, dels)


def _decode_summary_q(buf, kind, node, seq, time, u, r, d) -> Summary:
    o = _SUMMARY_HDR.size

    def take(dtype: str, count: int):
        nonlocal o
        a = np.frombuffer(buf, dtype=dtype, count=count, offset=o)
        o += a.nbytes
        return a
    base = KIND_FULL if kind == KIND_FULL_Q else KIND_DELTA
    if u == 0:
        dels = take("<u4", r).astype(np.int64)
        return Summary(base, node, seq, time, np.zeros(0, np.int64), np.zeros(0), np.zeros((0, d)),
                       np.zeros(0), np.zeros(0), dels)
    lo = take("<f2", d).astype(np.float64)
    span = take("<f2", d).astype(np.float64)
    ids = take("<u4", u).astype(np.int64)
    n = take("<f4", u).astype(np.float64)
    var = take("<f2", u).astype(np.float64)
    t = take("<f4", u).astype(np.float64) + time
    q = take("u1", u * d).astype(np.float64).reshape(u, d)
    dels = take("<u4", r).astype(np.int64)
    mu = lo + q / 255.0 * span
    LS = n[:, None] * mu
    SS = n * (var + np.einsum("ij,ij->i", mu, mu))
    return Summary(base, node, seq, time, ids, n, LS, SS, t, dels)


@dataclass
class GlobalModel:
    version: int
    time: float
    centers: np.ndarray  # (k, d)
    radii: np.ndarray    # (k,)
    weights: np.ndarray  # (k,)


def encode_global(g: GlobalModel) -> bytes:
    k, d = g.centers.shape
    return b"".join([
        _GLOBAL_HDR.pack(KIND_GLOBAL, g.version, g.time, k, d),
        np.asarray(g.centers, "<f4").tobytes(),
        np.asarray(g.radii, "<f4").tobytes(),
        np.asarray(g.weights, "<f4").tobytes(),
    ])


def decode_global(buf: bytes) -> GlobalModel:
    _, version, time, k, d = _GLOBAL_HDR.unpack_from(buf, 0)
    o = _GLOBAL_HDR.size
    C = np.frombuffer(buf, "<f4", k * d, o).astype(np.float64).reshape(k, d)
    o += 4 * k * d
    r = np.frombuffer(buf, "<f4", k, o).astype(np.float64)
    o += 4 * k
    w = np.frombuffer(buf, "<f4", k, o).astype(np.float64)
    return GlobalModel(version, time, C, r, w)


def encode_raw(node: int, seq: int, time: float, X: np.ndarray) -> bytes:
    c, d = X.shape
    return _RAW_HDR.pack(KIND_RAW, node, seq, time, c, d) + np.asarray(X, "<f4").tobytes()


def decode_raw(buf: bytes) -> tuple[int, int, float, np.ndarray]:
    _, node, seq, time, c, d = _RAW_HDR.unpack_from(buf, 0)
    X = np.frombuffer(buf, "<f4", c * d, _RAW_HDR.size).astype(np.float64).reshape(c, d)
    return node, seq, time, X

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


def decode_summary(buf: bytes) -> Summary:
    kind, _, node, seq, time, u, r, d = _SUMMARY_HDR.unpack_from(buf, 0)
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

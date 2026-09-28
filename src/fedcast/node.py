"""Edge node: local micro-clustering plus exact tracking of what the server holds.

The node is transport-agnostic: it builds Summary messages and is told when
they were delivered. The same class runs inside the discrete-event simulator
and inside the real Kafka runtime.
"""
from __future__ import annotations

import numpy as np

from .codec import (KAFKA_RECORD_OVERHEAD, KIND_DELTA, KIND_FULL, GlobalModel, Summary, summary_size,
                    summary_size_q)
from .microcluster import MCParams, MicroClusterModel, decay_factor


class EdgeNode:
    def __init__(self, node_id: int, dim: int, params: MCParams, policy):
        self.id = node_id
        self.dim = dim
        self.params = params
        self.mc = MicroClusterModel(dim, params)
        self.policy = policy
        # server view: id -> (n, LS, SS, t) as last sent, and the slot version sent
        self.sent: dict[int, tuple[float, np.ndarray, float, float]] = {}
        self.sent_mod: dict[int, int] = {}
        self.seq = 0
        self.global_model: GlobalModel | None = None
        # link-price estimate (EWMA of wire/payload bytes and of delivery latency)
        self.price_overhead = 1.0
        self.price_latency = 0.0
        self.latency_ref = 0.1
        # accounting
        self.bytes_up = 0
        self.wire_up = 0
        self.msgs_up = 0
        self.bytes_down = 0
        self.quant = False   # quantised wire format (set by the runtime)

    # ------------------------------------------------------------------ data
    def ingest(self, x: np.ndarray, now: float) -> None:
        self.mc.insert(x, now)

    def on_global(self, g: GlobalModel, nbytes: int) -> None:
        if self.global_model is None or g.version > self.global_model.version:
            self.global_model = g
        self.bytes_down += nbytes

    # ------------------------------------------------------------------ views
    def dirty(self) -> tuple[list[int], list[int]]:
        """(slots whose CF differs from the server copy, ids the server must delete)."""
        mc = self.mc
        act = np.flatnonzero(mc.active)
        slots = [int(s) for s in act
                 if self.sent_mod.get(int(mc.ids[s]), -1) < mc.mod[s]]
        live = {int(i) for i in mc.ids[act]}
        dels = [i for i in self.sent_mod if i not in live]
        return slots, dels

    def current_arrays(self, now: float):
        mc = self.mc
        act = np.flatnonzero(mc.active)
        f = decay_factor(now, mc.t[act], self.params.half_life)
        return mc.ids[act].copy(), mc.n[act] * f, mc.LS[act] * f[:, None], mc.SS[act] * f

    def sent_arrays(self, now: float):
        if not self.sent:
            return (np.zeros(0, np.int64), np.zeros(0), np.zeros((0, self.dim)), np.zeros(0))
        ids = np.fromiter(self.sent.keys(), np.int64, len(self.sent))
        vals = list(self.sent.values())
        n = np.array([v[0] for v in vals])
        LS = np.array([v[1] for v in vals])
        SS = np.array([v[2] for v in vals])
        t = np.array([v[3] for v in vals])
        f = decay_factor(now, t, self.params.half_life)
        return ids, n * f, LS * f[:, None], SS * f

    def summary_bytes(self, n_up: int, n_del: int) -> int:
        f = summary_size_q if self.quant else summary_size
        return f(n_up, n_del, self.dim) + KAFKA_RECORD_OVERHEAD

    def pending_bytes(self, full: bool = False) -> int:
        if full:
            return self.summary_bytes(int(self.mc.active.sum()), 0)
        slots, dels = self.dirty()
        if not slots and not dels:
            return 0
        return self.summary_bytes(len(slots), len(dels))

    def full_bytes_max(self) -> int:
        return self.summary_bytes(self.params.max_mc, 0)

    def link_price(self) -> float:
        return self.price_overhead * (1.0 + self.price_latency / self.latency_ref)

    # ------------------------------------------------------------------ messages
    def build(self, now: float, full: bool = False, only_slots: list[int] | None = None) -> Summary:
        mc = self.mc
        if full:
            slots, dels = [int(s) for s in np.flatnonzero(mc.active)], []
        else:
            slots, dels = self.dirty()
            if only_slots is not None:
                slots = only_slots
        s = np.array(slots, dtype=np.int64)
        summ = Summary(
            kind=KIND_FULL if full else KIND_DELTA, node=self.id, seq=self.seq, time=now,
            ids=mc.ids[s].copy(), n=mc.n[s].copy(), LS=mc.LS[s].copy(), SS=mc.SS[s].copy(),
            t=mc.t[s].copy(), deleted=np.array(dels, dtype=np.int64))
        self._pending_mods = {int(mc.ids[x]): int(mc.mod[x]) for x in slots}
        return summ

    def commit(self, summ: Summary, payload_bytes: int) -> None:
        """Record that summ was handed to the transport (server state will match)."""
        if summ.kind == KIND_FULL:
            self.sent.clear()
            self.sent_mod.clear()
        for j, i in enumerate(summ.ids):
            i = int(i)
            self.sent[i] = (float(summ.n[j]), summ.LS[j].copy(), float(summ.SS[j]), float(summ.t[j]))
            self.sent_mod[i] = self._pending_mods.get(i, 0)
        for i in summ.deleted:
            self.sent.pop(int(i), None)
            self.sent_mod.pop(int(i), None)
        self.seq += 1
        self.bytes_up += payload_bytes
        self.msgs_up += 1
        self.policy.on_sent(payload_bytes)

    def on_delivery(self, payload_bytes: int, wire_bytes: int, latency: float, alpha: float = 0.2) -> None:
        self.wire_up += wire_bytes
        self.price_overhead = (1 - alpha) * self.price_overhead + alpha * wire_bytes / max(payload_bytes, 1)
        self.price_latency = (1 - alpha) * self.price_latency + alpha * latency

    def build_kfed(self, now: float, k_local: int, rng: np.random.Generator) -> Summary:
        """k-FED message: k' local centres, each as the exact CF sum of its MCs."""
        from .macro import weighted_kmeans
        _, n, LS, SS = self.current_arrays(now)
        mu = LS / np.maximum(n, 1e-12)[:, None]
        k = min(k_local, len(n))
        _, lab, _ = weighted_kmeans(mu, n, k, rng)
        cn = np.bincount(lab, weights=n, minlength=k)
        cL = np.zeros((k, self.dim))
        np.add.at(cL, lab, LS)
        cS = np.bincount(lab, weights=SS, minlength=k)
        keep = cn > 0
        self._pending_mods = {}
        return Summary(kind=KIND_FULL, node=self.id, seq=self.seq, time=now,
                       ids=np.arange(int(keep.sum()), dtype=np.int64), n=cn[keep], LS=cL[keep],
                       SS=cS[keep], t=np.full(int(keep.sum()), now), deleted=np.zeros(0, np.int64))

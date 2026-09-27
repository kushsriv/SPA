"""In-application network emulator (portable replacement for Linux `tc netem`).

A Link models one node's uplink/downlink: propagation delay with jitter, a
bandwidth cap (serialisation delay with a FIFO queue), random loss with
retransmission after an RTO (as Kafka's producer retries do), and scheduled
outages during which nothing leaves the node (the producer buffers).

Everything is driven by a seeded RNG, so experiments are exactly repeatable
on any OS and cost no extra memory.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class LinkProfile:
    name: str = "lan"
    delay: float = 0.005          # one-way seconds
    jitter: float = 0.001
    loss: float = 0.0             # per-attempt loss probability
    bandwidth: float = 100e6      # bits per second
    rto: float = 0.3              # retransmission timeout (s)
    outages: list[tuple[float, float]] = field(default_factory=list)


PROFILES = {
    "lan": LinkProfile("lan", 0.005, 0.001, 0.0, 100e6),
    "wan": LinkProfile("wan", 0.08, 0.02, 0.01, 5e6),
    "cellular": LinkProfile("cellular", 0.15, 0.05, 0.03, 1e6),
    "poor": LinkProfile("poor", 0.30, 0.10, 0.08, 128e3),
}


class Link:
    def __init__(self, profile: LinkProfile, rng: np.random.Generator):
        self.p = profile
        self.rng = rng
        self.busy_until = 0.0

    def in_outage(self, t: float) -> float | None:
        for a, b in self.p.outages:
            if a <= t < b:
                return b
        return None

    def send(self, t: float, nbytes: int) -> tuple[float, int, int]:
        """Returns (arrival_time, wire_bytes, attempts) for a message sent at t."""
        start = max(t, self.busy_until)
        end = self.in_outage(start)
        if end is not None:
            start = end
        ser = nbytes * 8.0 / self.p.bandwidth
        attempts = 1
        while self.rng.random() < self.p.loss and attempts < 10:
            attempts += 1
        self.busy_until = start + ser * attempts
        delay = max(0.0, self.rng.normal(self.p.delay, self.p.jitter))
        arrival = start + ser * attempts + (attempts - 1) * self.p.rto + delay
        return arrival, nbytes * attempts, attempts

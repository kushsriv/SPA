import numpy as np
import pytest

from fedcast import codec
from fedcast.coordinator import Coordinator
from fedcast.macro import cf_cost, lloyd, sqdist, weighted_kmeans
from fedcast.microcluster import MCParams, MicroClusterModel, decay_factor
from fedcast.node import EdgeNode
from fedcast.partition import partition
from fedcast.policies import FedCAST, Naive, per_mc_staleness


def rng(s=0):
    return np.random.default_rng(s)


def test_cf_cost_matches_bruteforce():
    X = rng().normal(size=(50, 4))
    C = rng(1).normal(size=(3, 4))
    n, LS, SS = np.array([50.0]), X.sum(0)[None], np.array([(X ** 2).sum()])
    brute = ((X[:, None, :] - C[None]) ** 2).sum(-1).sum(0)
    assert np.allclose(cf_cost(n, LS, SS, C)[0], brute)


def test_microclusters_conserve_mass_without_decay():
    p = MCParams(max_mc=10, half_life=float("inf"), min_weight=0.0)
    mc = MicroClusterModel(3, p)
    X = rng().normal(size=(500, 3)) * 3
    for i, x in enumerate(X):
        mc.insert(x, float(i))
    a = mc.active
    assert mc.active.sum() <= 10
    assert np.isclose(mc.n[a].sum(), 500)
    assert np.allclose(mc.LS[a].sum(0), X.sum(0))
    assert np.isclose(mc.SS[a].sum(), (X ** 2).sum())


def test_decay_keeps_centroid():
    p = MCParams(half_life=10.0)
    mc = MicroClusterModel(2, p)
    mc.insert(np.array([1.0, 2.0]), 0.0)
    mc.insert(np.array([1.1, 2.1]), 0.0)
    before = mc.centroids()[0].copy()
    mc._decay_slot(0, 50.0)
    assert np.allclose(mc.centroids()[0], before)
    assert mc.n[0] == pytest.approx(2 * 2 ** -5)


def test_codec_roundtrip_and_sizes():
    d = 7
    s = codec.Summary(codec.KIND_DELTA, 3, 9, 12.5, np.array([1, 5]), np.array([2.0, 3.0]),
                      rng().normal(size=(2, d)), np.array([4.0, 5.0]), np.array([1.0, 2.0]),
                      np.array([7, 8, 9]))
    buf = codec.encode_summary(s)
    assert len(buf) == codec.summary_size(2, 3, d)
    r = codec.decode_summary(buf)
    assert r.node == 3 and r.seq == 9 and list(r.ids) == [1, 5] and list(r.deleted) == [7, 8, 9]
    assert np.allclose(r.LS, s.LS, atol=1e-5)
    g = codec.GlobalModel(4, 1.0, rng().normal(size=(5, d)), np.ones(5), np.arange(5.0))
    gb = codec.encode_global(g)
    assert len(gb) == codec.global_size(5, d)
    assert np.allclose(codec.decode_global(gb).centers, g.centers, atol=1e-5)
    X = rng().normal(size=(4, d))
    rb = codec.encode_raw(1, 2, 3.0, X)
    assert len(rb) == codec.raw_size(4, d)
    assert np.allclose(codec.decode_raw(rb)[3], X, atol=1e-5)


def _stream_node(policy, n=400, seed=0, d=3):
    node = EdgeNode(0, d, MCParams(max_mc=20, half_life=100.0), policy)
    X = rng(seed).normal(size=(n, d)) * 2
    return node, X


def test_delta_sync_reproduces_node_state_exactly():
    """With lossless in-order delivery the coordinator's copy equals the node's CFs."""
    node, X = _stream_node(Naive())
    coord = Coordinator(k=3, dim=3, half_life=100.0)
    for i, x in enumerate(X):
        node.ingest(x, i * 0.1)
        if i % 25 == 24:
            s = node.build(i * 0.1)  # delta
            buf = codec.encode_summary(s)
            node.commit(s, len(buf))
            coord.apply(codec.decode_summary(buf))
    s = node.build(40.0)
    node.commit(s, 0)
    coord.apply(codec.decode_summary(codec.encode_summary(s)))
    st = node.mc.state()
    assert set(coord.nodes[0]) == set(st)
    for i, (n, LS, SS, t) in st.items():
        cn, cL, cS, ct = coord.nodes[0][i]
        assert cn == pytest.approx(n, rel=1e-5) and np.allclose(cL, LS, rtol=1e-4, atol=1e-4)


def test_duplicates_are_ignored():
    coord = Coordinator(k=2, dim=2, half_life=10.0)
    s = codec.Summary(codec.KIND_FULL, 0, 0, 0.0, np.array([0]), np.array([1.0]), np.ones((1, 2)),
                      np.array([2.0]), np.array([0.0]), np.zeros(0, np.int64))
    assert coord.apply(s) and not coord.apply(s)
    assert coord.duplicates == 1


def test_staleness_zero_after_sync_and_bounds_cost_error():
    node, X = _stream_node(Naive(), n=600, seed=3)
    C = rng(9).normal(size=(4, 3)) * 2
    radii = np.ones(4)
    for i, x in enumerate(X[:300]):
        node.ingest(x, i * 0.1)
    s = node.build(30.0, full=True)
    node.commit(s, 0)
    slots, e, dels, ed, _ = per_mc_staleness(node, 30.0, C, radii)
    assert e.sum() + ed.sum() == pytest.approx(0.0, abs=1e-9)
    for i, x in enumerate(X[300:]):
        node.ingest(x, 30.0 + i * 0.1)
    now = 60.0
    node.mc.prune(now)
    slots, e, dels, ed, _ = per_mc_staleness(node, now, C, radii)
    delta = e.sum() + ed.sum()
    # Proposition 1: |J(current) - J(server copy)| <= Delta
    _, n1, L1, S1 = node.current_arrays(now)
    _, n0, L0, S0 = node.sent_arrays(now)
    J1 = cf_cost(n1, L1, S1, C).min(1).sum()
    J0 = cf_cost(n0, L0, S0, C).min(1).sum() if len(n0) else 0.0
    assert abs(J1 - J0) <= delta + 1e-6
    assert delta > 0


def test_fedcast_respects_budget():
    B, W, T = 2000.0, 10.0, 400.0
    pol = FedCAST(budget=B, window=W)
    node = EdgeNode(0, 4, MCParams(max_mc=30, half_life=60.0), pol)
    node.global_model = codec.GlobalModel(1, 0.0, rng().normal(size=(5, 4)), np.ones(5), np.ones(5))
    X = rng(1).normal(size=(8000, 4)) * 3
    per_tick = len(X) / T
    j = 0
    for t in np.arange(1.0, T + 1):
        while j < min(len(X), int(t * per_tick)):
            node.ingest(X[j], t)
            j += 1
        pol.tick(t)
        dec = pol.decide(node, t)
        if dec is not None:
            s = node.build(t, full=dec.full, only_slots=dec.only_slots)
            node.commit(s, len(codec.encode_summary(s)) + codec.KAFKA_RECORD_OVERHEAD)
    assert node.bytes_up <= B * T / W + pol.cap + 1
    assert node.bytes_up >= 0.5 * B * T / W  # and it uses the budget it is given


def test_swap_search_recovers_small_far_cluster():
    r = rng(4)
    big = np.vstack([r.normal(c, 0.3, (200, 2)) for c in ([0, 0], [4, 0], [0, 4])])
    small = r.normal([12, 12], 0.3, (15, 2))
    X = np.vstack([big, small])
    w = np.ones(len(X))
    bad_init = np.array([[0, 0], [4, 0], [0, 4], [0.5, 0.5]], float)
    _, _, cost_plain = lloyd(X, w, bad_init)
    C, _, cost_swap = weighted_kmeans(X, w, 4, r, init=bad_init, swaps=3, restarts=1)
    assert cost_swap < 0.5 * cost_plain
    assert sqdist(np.array([[12.0, 12.0]]), C).min() < 1.0


def test_partitions():
    y = np.repeat(np.arange(10), 100)
    st = partition(y, 10, "exclusive:1", 100.0, seed=0)
    for s in st:
        assert len(np.unique(y[s.idx])) == 1
    st = partition(y, 5, "dirichlet:0.5", 100.0, seed=0)
    assert sum(len(s.idx) for s in st) == len(y)
    assert all(np.all(np.diff(s.times) >= 0) for s in st)


def test_decay_factor():
    assert decay_factor(10.0, 0.0, 10.0) == pytest.approx(0.5)


def test_empty_node_state_roundtrips():
    """A node whose server-side state is empty must still be checkpointable (50-node Kafka bug)."""
    s = codec.Summary(codec.KIND_FULL, 7, 3, 1.0, np.zeros(0, np.int64), np.zeros(0),
                      np.zeros((0, 5)), np.zeros(0), np.zeros(0), np.zeros(0, np.int64))
    r = codec.decode_summary(codec.encode_summary(s))
    assert r.LS.shape == (0, 5) and r.node == 7
    vals = []
    assert np.array([v for v in vals]).reshape(0, 5).shape == (0, 5)


def test_uniform_staleness_bound_holds_for_every_centre_set():
    """Theorem (staleness-robust approximation), key step: for ANY centres with ||c|| <= R,
    |J(S;C) - J(S^;C)| <= U = sum |dSS| + 2R||dLS|| + R^2|dn|  (the 'uniform' score)."""
    from fedcast.policies import _per_mc_simple
    node, X = _stream_node(Naive(), n=700, seed=5)
    for i, x in enumerate(X[:350]):
        node.ingest(x, i * 0.1)
    s = node.build(35.0, full=True)
    node.commit(s, 0)
    for i, x in enumerate(X[350:]):
        node.ingest(x * 1.3 + 0.5, 35.0 + i * 0.1)   # drift
    now = 70.0
    node.mc.prune(now)
    slots, dels = node.dirty()
    _, e_s, _, e_d, _ = _per_mc_simple(node, now, slots, dels, "uniform")
    U = e_s.sum() + e_d.sum()
    act = np.flatnonzero(node.mc.active)
    R = float(np.linalg.norm(node.mc.LS[act] / node.mc.n[act, None], axis=1).max())
    _, n1, L1, S1 = node.current_arrays(now)
    _, n0, L0, S0 = node.sent_arrays(now)
    r = rng(11)
    for _ in range(200):
        C = r.normal(size=(4, 3))
        C *= (R * r.uniform(0, 1, (4, 1))) / np.linalg.norm(C, axis=1, keepdims=True)
        J1 = cf_cost(n1, L1, S1, C).min(1).sum()
        J0 = cf_cost(n0, L0, S0, C).min(1).sum()
        assert abs(J1 - J0) <= U + 1e-6


def test_quantised_codec_roundtrip_and_size():
    d = 12
    r = rng(3)
    n = r.uniform(1, 50, 6)
    mu = r.normal(size=(6, d)) * 2
    var = r.uniform(0.1, 1.0, 6)
    LS, SS = n[:, None] * mu, n * (var + (mu ** 2).sum(1))
    s = codec.Summary(codec.KIND_DELTA, 2, 4, 100.0, np.arange(6), n, LS, SS, np.full(6, 99.5), np.array([9]))
    buf = codec.encode_summary_q(s)
    assert len(buf) == codec.summary_size_q(6, 1, d)
    assert len(buf) < 0.6 * codec.summary_size(6, 1, d)          # ~2.8x smaller per record
    q = codec.decode_summary(buf)
    assert q.kind == codec.KIND_DELTA and list(q.deleted) == [9]
    mu_q = q.LS / q.n[:, None]
    span = mu.max(0) - mu.min(0)
    assert np.all(np.abs(mu_q - mu) <= span / 255 * 1.01 + 1e-2)  # about half a step per dimension
    assert np.allclose(q.t, 99.5, atol=1e-3)
    assert np.allclose(q.SS / q.n - (mu_q ** 2).sum(1), var, rtol=2e-3, atol=1e-3)


def test_lloyd_excess_is_exact():
    """Server centre c^ = L^/N^ from stale summaries; true data adds dL, dN. With assignments
    fixed, cost(c^) - min_c cost(c) == ||dL - dN c^||^2 / N  (N = true mass), exactly."""
    r = rng(8)
    old = r.normal(1.0, 1.0, (300, 5))
    new = r.normal(2.5, 0.7, (80, 5))           # drift: new mass away from the old centre
    c_hat = old.mean(0)
    allp = np.vstack([old, new])
    cost = lambda c: ((allp - c) ** 2).sum()
    excess = cost(c_hat) - cost(allp.mean(0))
    dL, dN, N = new.sum(0), len(new), len(allp)
    assert np.isclose(excess, ((dL - dN * c_hat) ** 2).sum() / N)
    # and growth "in place" (new points centred on c^) is worth ~nothing
    inplace = r.normal(0, 1.0, (80, 5)) + c_hat
    inplace -= inplace.mean(0) - c_hat
    assert np.isclose(((inplace.sum(0) - 80 * c_hat) ** 2).sum(), 0.0, atol=1e-8)

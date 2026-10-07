#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Unit tests for the CPU/RAM dynamic scheduler (no xdist workers involved)."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from test import HOST_ID
from test.pylib import sched_dir
from test.pylib.db.model import HostInfo
from test.pylib.db.writer import DEFAULT_DB_NAME, HOST_INFO_TABLE, SQLiteWriter
from test.pylib.dynamic_scheduler import (
    HELD_RESERVE_SECONDS,
    MEM_RESERVE,
    MEM_STALL_LIMIT,
    STATIC_MEM_CPP,
    STATIC_MEM_GDB,
    GB,
    DynamicScheduling,
    CostModel,
    parse_seastar_args,
    profile_key,
    take_eviction,
)

NO_CGROUP = Path("/nonexistent/cgroup")


class FakeNode:
    def __init__(self, wid: str):
        self.gateway = SimpleNamespace(id=wid)
        self.sent: list[int] = []
        self._shutdown_sent = False
        self._down = False

    @property
    def shutting_down(self) -> bool:
        return self._down or self._shutdown_sent

    def send_runtest_some(self, indices):
        assert not self._shutdown_sent, "sent tests after shutdown"
        self.sent.extend(indices)

    def shutdown(self):
        self._shutdown_sent = True


class FakeConfig:
    def __init__(self, tmp: Path, nodes: int, **overrides):
        self.opts = {
            "--mode": ["release"],

            "--tmpdir": str(tmp),
            "--gather-metrics": False,
            "tx": ["popen"] * nodes,
        }
        self.opts.update(overrides)
        self.pluginmanager = SimpleNamespace(getplugin=lambda name: None)
        self.hook = SimpleNamespace(pytest_collectreport=lambda report: None)

    def getoption(self, name):
        return self.opts[name]

    def getvalue(self, name):
        return self.opts[name]


# What the tests run the scheduler with unless they ask otherwise: the pressure guards and
# the ramp off, so that admission depends on the costs alone.
TEST_SETTINGS = dict(cpu_target=0.9, psi_cpu_limit=1e9, psi_mem_limit=1e9, burst=0.0, k_sigma=0.0,
                     max_workers=0,   # the pool stays as it starts unless a test grows it
                     refresh=0.0)     # every pass looks at the measurements, as the tests expect


def new_sched(config, **kw) -> DynamicScheduling:
    return DynamicScheduling(config, **{**TEST_SETTINGS, **kw})


@pytest.fixture(autouse=True)
def no_sched_dir(monkeypatch):
    """No run scratch unless a test asks for one.

    The test.py conftest points sched_dir at the real run's, and with
    it the scheduler can evict workers the test did not expect to lose.
    """
    monkeypatch.setattr(sched_dir, "_root", None)


def make_model(tmp: Path, ncpus: int, entries: dict[str, tuple[float, float, float]]) -> CostModel:
    """entries: key -> (cores, mem_bytes, wall)."""
    model = CostModel(tmp / "profile.json", ncpus, k_sigma=0.0)
    for key, (cores, mem, wall) in entries.items():
        model.tests[key] = {"cores": cores, "mem": mem, "wall": wall, "var_cores": 0.0, "n": 3, "n_unc": 3}
    return model


def make_sched(tmp: Path, collection: list[str], costs: dict[str, tuple[float, float, float]],
               nodes: int = 2, ncpus: int = 4, mem_total: float = 20 * GB, **settings) -> tuple[DynamicScheduling, list[FakeNode]]:
    model = make_model(tmp, ncpus, {profile_key(nid): c for nid, c in costs.items()})
    sched = new_sched(FakeConfig(tmp, nodes), model=model, ncpus=ncpus, mem_total=mem_total,
                      cgroup_tests=NO_CGROUP, available_fn=lambda: mem_total, **settings)
    fake_nodes = [FakeNode(f"gw{i}") for i in range(nodes)]
    for n in fake_nodes:
        sched.add_node(n)
        sched.add_node_collection(n, collection)
    sched.schedule()
    return sched, fake_nodes


def committed(sched: DynamicScheduling) -> set[int]:
    return set(sched.committed_at)


# ---------------------------------------------------------------- helpers ----

def test_profile_key_strips_mode_and_run_id():
    assert profile_key("cqlpy/test_x.py::test_y[1.5].dev.1") == "dev|cqlpy/test_x.py::test_y[1.5]"
    assert profile_key("boost/x_test.cc::case.2.release.7") == "release|boost/x_test.cc::case.2"


def test_parse_seastar_args():
    assert parse_seastar_args("-c2 -m2G") == (2.0, 2e9)
    assert parse_seastar_args("-c1 -m256M --logger-log-level x=trace") == (1.0, 256e6)
    assert parse_seastar_args("--smp 3 --memory 1G") == (3.0, 1e9)
    assert parse_seastar_args("-c1") == (1.0, None)


def test_static_cost_reads_boost_custom_args(tmp_path):
    """Shards come from -c.  Memory does not come from -m: see the test below."""
    model = CostModel(tmp_path / "p.json", ncpus=16)
    c = model.cost("boost/sstable_test.cc::some_case.dev.1")            # custom_args: -c1 -m2G
    assert c.cores == 1.0 and c.source == "static-cpp"
    c = model.cost("boost/multishard_query_test.cc::some_case.dev.1")   # custom_args: -c2 -m3G
    assert c.cores == 2.0
    c = model.cost("boost/unknown_test.cc::case.dev.1")
    assert c.cores == 2.0
    c = model.cost("cqlpy/test_x.py::test_y.dev.1")
    assert c.source == "static-python" and c.cores == 1.0


def test_static_memory_comes_from_measurement_not_from_the_seastar_flag(tmp_path):
    """-m is what a test may reserve, not what it takes: it is not the memory estimate."""
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="dev")        # no per-mode guesses: the base ones
    big = model.cost("boost/multishard_query_test.cc::some_case.dev.1")   # -c2 -m3G
    assert big.mem == pytest.approx(STATIC_MEM_CPP), "a roomy -m must not inflate the guess"
    plain = model.cost("boost/unknown_test.cc::case.dev.1")               # no custom args
    assert plain.mem == pytest.approx(STATIC_MEM_CPP)
    # ... but a small -m is a real ceiling, and does lower it
    small = model.cost("boost/reader_concurrency_semaphore_test.cc::case.dev.1")   # -c1 -m256M
    assert small.mem == pytest.approx(0.256e9 + 0.1e9)
    assert small.mem < STATIC_MEM_CPP


def test_the_one_suite_that_really_is_hungry_is_not_guessed_at_a_gigabyte(tmp_path):
    """scylla_gdb loads the whole binary's debug info: 2.4 GB median, 16 GB at the top.

    Guessing 1 GB for it, like every other Python suite, is what drove the first run into
    swap: a handful admitted together promise 4 GB and take forty.
    """
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="dev")        # no per-mode guesses: the base ones
    gdb = model.cost("scylla_gdb/test_basic_commands.py::test_scylla_commands[task-queues].dev.1")
    assert gdb.source == "static-gdb"
    assert gdb.mem == pytest.approx(STATIC_MEM_GDB)
    assert gdb.mem > model.cost("cqlpy/test_x.py::test_y.dev.1").mem * 5
    # and it is ordered as a long test, not a one-second one
    assert gdb.wall >= 15.0
    c = model.cost("cluster/test_x.py::test_y.dev.1")
    assert c.source == "static-cluster"


def test_learn_ema_and_family_fallback(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 4.0, "memory_peak": 1e9})
    assert model.tests["dev|f.py::a"]["cores"] == 2.0
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 0.5e9})
    e = model.tests["dev|f.py::a"]
    assert e["cores"] == pytest.approx(0.7 * 2.0 + 0.3 * 1.0)
    assert e["mem"] == pytest.approx(1e9)            # a lower peak does not lower the estimate at once
    assert e["n"] == 2
    # asymmetric memory: three runs far below the estimate bring it down slowly
    for _ in range(3):
        model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 0.2e9})
    assert e["mem"] == pytest.approx(0.8e9)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 3e9})
    assert e["mem"] == pytest.approx(3e9)            # a higher peak is taken immediately
    fam = model.cost("f.py::b.dev.1")                # unseen sibling -> family estimate
    assert fam.source == "family" and fam.cores == pytest.approx(e["cores"])
    # first-in-file samples of a known test feed the per-file setup cost, not the test
    model.learn({"key": "dev|f.py::a", "wall": 5.0, "usage_sec": 10.0, "memory_peak": 1e9, "first_in_file": True})
    assert model.tests["dev|f.py::a"]["n"] == 6
    model.save()
    reloaded = CostModel(tmp_path / "p.json", ncpus=8)
    assert reloaded.tests["dev|f.py::a"]["n"] == 6 and "dev|f.py" in reloaded.files


# --------------------------------------------------------------- scheduler ----

def test_measurements_are_looked_at_every_refresh_seconds_but_a_finished_test_frees_its_room_at_once(tmp_path):
    """A pass runs on every finished test: it reads the machine at most every
    REFRESH_SECONDS, while the room a finished test held goes to the next at once."""
    clock = {"t": 1000.0}
    col = [f"a{i}.py::t.dev.1" for i in range(6)]
    sched, nodes = make_sched(tmp_path, col, {n: (3.0, 1e8, 1.0) for n in col}, nodes=2, ncpus=4,
                              now=lambda: clock["t"], refresh=0.5)
    reads = []
    sched._refresh_measurement = lambda: reads.append(clock["t"])
    running = next(iter(committed(sched)))
    clock["t"] += 0.1
    sched.mark_test_complete(next(n for n in nodes if running in sched.node2pending[n]), running)
    assert len(committed(sched)) == 1 and running not in committed(sched), "the next one started at once"
    assert reads == [], "without reading the machine again"
    clock["t"] += 0.5
    sched.check_schedule()
    assert reads == [clock["t"]]
    assert sched.stats["controller_ms"] >= 0


def test_sending_a_successor_is_what_starts_a_test(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    # 3 cores each on a 4-cpu box with target 3.6 -> strictly one at a time
    sched, nodes = make_sched(tmp_path, col, {n: (3.0, 1e9, 1.0) for n in col})
    # one worker runs a test and holds the next, which its successor would start; the
    # other is not given a test it could not start
    assert len(committed(sched)) == 1
    runner = next(n for n in nodes if len(n.sent) == 2)
    waiter = next(n for n in nodes if n is not runner)
    assert not waiter.sent
    running = runner.sent[0]
    assert committed(sched) == {running}
    # finishing the running test frees the budget: exactly one more starts
    sched.mark_test_complete(runner, running)
    assert len(committed(sched)) == 1
    assert (sched.stats["rejected_cpu_band"] + sched.stats["rejected_cpu_ceiling"]
            + sched.stats["rejected_no_headroom"]) >= 1
    assert waiter.sent or runner.sent  # somebody got the successor


def test_never_more_than_budget_and_all_tests_run(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(12)]
    costs = {n: (1.5, 1e9, 1.0) for n in col}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=4, ncpus=4)   # target 3.6 -> 2 at a time
    done: set[int] = set()
    guard = 0
    while len(done) < len(col):
        guard += 1
        assert guard < 200
        assert len(committed(sched)) <= 2
        # complete one committed test (the oldest)
        idx = min(committed(sched), key=lambda i: sched.committed_at[i])
        node = next(n for n in nodes if idx in sched.node2pending[n])
        sched.mark_test_complete(node, idx)
        done.add(idx)
    assert done == set(range(12))
    assert sched.tests_finished
    assert all(n._shutdown_sent for n in nodes)


def test_progress_rule_runs_oversized_test(tmp_path):
    """A test asking for more RAM than the budget can never fit, but must still run (alone)."""
    col = ["a.py::big.dev.1", "a.py::big2.dev.1"]
    sched, nodes = make_sched(tmp_path, col, {n: (1.0, 100 * GB, 1.0) for n in col}, nodes=2, ncpus=4)
    assert len(committed(sched)) == 1
    assert sched.stats["forced"] == 1 and sched.stats["rejected_mem"] >= 1


def test_memory_gate(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    # tiny cpu, 12 GB each on a 20 GB box (target 15 GB) -> one at a time by memory
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 12 * GB, 1.0) for n in col}, nodes=3, ncpus=16)
    assert len(committed(sched)) == 1
    assert sched.stats["rejected_mem"] >= 1


def make_mem_sched(tmp: Path, n: int, mem: float, avail: dict, clock: dict, live: dict | None = None):
    """n workers on 16 cores, n tests of `mem` each and almost no CPU; available and time are the caller's."""
    col = [f"a.py::t{i}.dev.1" for i in range(n)]
    model = make_model(tmp, 16, {profile_key(c): (0.1, mem, 60.0) for c in col})
    sched = new_sched(FakeConfig(tmp, n), model=model, ncpus=16, mem_total=20 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    if live is not None:
        sched.live.memory = lambda wid: live.get(wid, 0.0)
    nodes = [FakeNode(f"gw{i}") for i in range(n)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    return sched, nodes


def test_headroom_counts_the_tests_admitted_earlier_in_the_same_pass(tmp_path):
    """Every test in a pass sees the memory the previous ones were just given.

    Checking each test against the same MemAvailable let 35 tests in within one pass on the
    full release suite, each seeing 4.7 GB free for its 0.6 GB.  Here the budget (18 GB free
    at start) would hold six 3 GB tests on paper; the 512 MiB reserve leaves room for five.
    """
    sched, _ = make_mem_sched(tmp_path, 8, 3 * GB, {"v": 18 * GB}, {"t": 0.0})
    assert sched.mem_reserve == MEM_RESERVE == 512 * 2**20
    assert len(committed(sched)) == 5
    assert sched.stats["rejected_mem"] >= 1


def test_memory_nobody_reserved_stops_admission(tmp_path):
    """Memory outside every reservation -- grown workers, fixtures, containers -- still counts.

    Reservations say 12 of 18 GB; the machine says 8 GB is left, of which the four tests that
    just started have not taken their 12 yet.  Admission must believe the machine.
    """
    avail = {"v": 16 * GB}
    sched, nodes = make_mem_sched(tmp_path, 8, 3 * GB, avail, {"t": 0.0})
    assert len(committed(sched)) == 5
    first = min(committed(sched))
    avail["v"] = 8 * GB
    sched.mark_test_complete(next(n for n in nodes if first in sched.node2pending[n]), first)
    assert len(committed(sched)) == 4, "reservations had room, the machine did not"


def test_a_started_test_stops_counting_what_it_holds_and_fades_once_settled(tmp_path):
    """What a test has allocated is in MemAvailable already; its forecast counts only the rest.

    And a test that has settled below its forecast releases the rest over time: holding
    every settled test at its peak for all its life starves the run.
    """
    avail, clock, live = {"v": 9 * GB + MEM_RESERVE}, {"t": 0.0}, {}
    sched, nodes = make_mem_sched(tmp_path, 4, 3 * GB, avail, clock, live)
    assert len(committed(sched)) == 3                   # 9 GB past the reserve: three 3 GB tests
    assert sched._mem_headroom() == pytest.approx(0.0)
    running = [n for n in nodes if any(i in sched.committed_at for i in sched.node2pending[n])]
    # each running test's worker has grown by its whole forecast, and the machine shows it
    for node in running:
        live[node.gateway.id] = 3 * GB
    avail["v"] = MEM_RESERVE
    sched._refresh_forecasts()
    assert sched._mem_headroom() == pytest.approx(0.0), "the grown part must not count twice"
    # settled at 1 GB instead: 2 GB each still to come while they may be growing ...
    for node in running:
        live[node.gateway.id] = 1 * GB
    avail["v"] = 6 * GB + MEM_RESERVE
    sched._refresh_forecasts()
    assert sched._mem_headroom() == pytest.approx(0.0)
    # ... and after five plateau half-lives only a thirty-second of it
    clock["t"] = 30.0 + 5 * 30.0
    sched._refresh_forecasts()
    assert sched._mem_headroom() == pytest.approx(6 * GB - 6 * GB / 32)


def test_a_test_growing_past_its_kind_raises_its_own_forecast(tmp_path):
    """A test that has reached 1.5 GB is not a typical dtest; count the growth still ahead of it.

    Pricing each unknown dtest at 0.9 GB and counting nothing once its worker had grown past
    that let twenty of the heaviest dtests start together and swap the machine.
    """
    avail, clock, live = {"v": 40 * GB}, {"t": 0.0}, {}
    col = [f"cluster/dtest/f{i}_test.py::test_a.release.1" for i in range(6)]
    model = CostModel(tmp_path / "p.json", ncpus=16, k_sigma=0.0, mode="release")
    sched = new_sched(FakeConfig(tmp_path, 6), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    nodes = [FakeNode(f"gw{i}") for i in range(6)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    assert len(committed(sched)) == 6, "six files, six scouts"
    sched._refresh_forecasts()
    to_come_at_start = sched._fc_mean
    for node in nodes:
        live[node.gateway.id] = 1.5 * GB
    sched._refresh_forecasts()
    grew = [i for n in nodes for i in sched.node2pending[n] if i in sched.committed_at]
    for idx in grew:
        mean = sched._forecast(idx, 1.5 * GB)
        assert mean > 2.0 * GB, "a dtest holding 1.5 GB is expected to go on growing"
    # still to come after taking 9 GB between them is more than was expected of them at the start
    assert sched._fc_mean > to_come_at_start


def test_an_unmeasured_file_sends_one_scout_first(tmp_path):
    """Siblings of an unmeasured dtest file wait for its scout, then start on what it shows."""
    clock = {"t": 0.0}
    col = [f"cluster/dtest/heavy_test.py::test_{i}.release.1" for i in range(4)]
    model = CostModel(tmp_path / "p.json", ncpus=16, k_sigma=0.0, mode="release")
    sched = new_sched(FakeConfig(tmp_path, 4), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: 60 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(4)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    assert len(committed(sched)) == 1, "only the scout may start"
    assert sched.stats["scouts"] == 1
    clock["t"] = 10.0
    sched.check_schedule()
    assert len(committed(sched)) == 1, "a young scout still holds its siblings"
    clock["t"] = 25.0
    sched.check_schedule()
    assert len(committed(sched)) > 1, "past SCOUT_SECONDS, and quiet, the siblings go"


def test_a_growing_scout_holds_its_siblings_until_it_settles(tmp_path):
    """A heavy test takes a minute to grow into its peak; its siblings wait to see how far it goes."""
    clock, live = {"t": 0.0}, {}
    col = [f"cluster/dtest/heavy_test.py::test_{i}.release.1" for i in range(4)]
    model = CostModel(tmp_path / "p.json", ncpus=16, k_sigma=0.0, mode="release")
    sched = new_sched(FakeConfig(tmp_path, 4), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: 60 * GB)
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    nodes = [FakeNode(f"gw{i}") for i in range(4)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    scout_node = next(n for n in nodes if any(i in sched.committed_at for i in sched.node2pending[n]))
    for t, held in ((10, 0.5), (20, 1.0), (30, 1.8), (40, 2.5)):
        clock["t"] = t
        live[scout_node.gateway.id] = held * GB
        sched.check_schedule()
        assert len(committed(sched)) == 1, f"the scout is still growing at {t} s"
    clock["t"] = 52.0                                    # no growth since 40 s
    sched.check_schedule()
    assert len(committed(sched)) > 1
    sibling = next(i for i in sched.committed_at if i != min(sched.committed_at, key=sched.committed_at.get))
    mean = sched._forecast(sibling, 0.0, sched._file_held[sched._file_of(sibling)])
    assert mean >= 2.5 * GB, "siblings are forecast on what the scout holds"


def test_cheap_uniform_files_do_not_scout(tmp_path):
    col = [f"cqlpy/test_x.py::test_{i}.release.1" for i in range(4)]
    model = CostModel(tmp_path / "p.json", ncpus=16, k_sigma=0.0, mode="release")
    sched = new_sched(FakeConfig(tmp_path, 4), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             available_fn=lambda: 60 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(4)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    assert sched.stats["scouts"] == 0 and len(committed(sched)) > 1


def test_conditional_peak_of_the_release_dtest_distribution(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="release")
    dist = model.kind_dist("dtest")
    mean0 = dist.conditional(0.0)
    mean1 = dist.conditional(1.0 * GB)
    mean2 = dist.conditional(2.0 * GB)
    assert 0.5 * GB < mean0 < 0.8 * GB
    assert mean0 < mean1 < mean2
    assert 1.6 * GB < mean1 < 3.2 * GB
    assert dist.conditional(20 * GB) is None
    assert model.kind_dist("gdb") is not None
    assert CostModel(tmp_path / "d.json", ncpus=16, mode="dev").kind_dist("dtest") is None


def test_release_guesses_price_dtests_as_the_heavy_tail_of_the_cluster_suite(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="release")
    dtest = model.cost("cluster/dtest/manager_backup_tests.py::TestX::test_y.release.1")
    cluster = model.cost("cluster/test_x.py::test_y.release.1")
    assert dtest.source == "static-dtest" and cluster.source == "static-cluster"
    assert dtest.mem == pytest.approx(0.9e9) and cluster.mem == pytest.approx(0.62e9)
    assert model.cost("scylla_gdb/test_basic_commands.py::test_x.release.1").mem == pytest.approx(6.1e9)
    # debug has no dtest guess of its own and keeps its cluster one
    debug = CostModel(tmp_path / "d.json", ncpus=16, mode="debug")
    assert debug.cost("cluster/dtest/x_test.py::test_y.debug.1").mem == pytest.approx(8.5e9)


def make_pool_sched(tmp: Path, clock: dict, avail: dict, n_start: int = 2, max_workers: int = 6, n_tests: int = 40,
                    cores: float = 0.1, mem: float = 0.5 * GB, one_file: bool = True):
    col = [f"a.py::t{i}.dev.1" if one_file else f"f{i}.py::t.dev.1" for i in range(n_tests)]
    model = make_model(tmp, 16, {profile_key(c): (cores, mem, 60.0) for c in col})
    spawned: list[int] = []
    sched = new_sched(FakeConfig(tmp, n_start), max_workers=max_workers, model=model, ncpus=16,
                             mem_total=64 * GB, cgroup_tests=NO_CGROUP, now=lambda: clock["t"],
                             available_fn=lambda: avail["v"],
                             spawn_worker=lambda: spawned.append(1) or f"gw{n_start + len(spawned) - 1}")
    nodes = [FakeNode(f"gw{i}") for i in range(n_start)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    return sched, nodes, col, spawned


def arrive(sched: DynamicScheduling, nodes: list[FakeNode], col: list[str]) -> FakeNode:
    """A spawned worker comes up and reports its collection, as xdist would."""
    node = FakeNode(f"gw{len(nodes)}")
    nodes.append(node)
    sched.add_node(node)
    sched.add_node_collection(node, col)
    sched.schedule()
    return node


def test_the_pool_grows_one_worker_at_a_time_while_every_worker_is_busy(tmp_path):
    """Start at the CPU count, add a worker only when all are busy, and only after the last one has collected."""
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail)
    assert len(committed(sched)) == 2 and len(spawned) == 1, "both workers busy: grow by one"
    clock["t"] = 30.0
    sched.check_schedule()
    assert len(spawned) == 1, "the new worker has not collected yet"
    new = arrive(sched, nodes, col)
    assert any(i in sched.committed_at for i in sched.node2pending[new]), "the new worker gets work at once"
    sched.check_schedule()
    assert len(spawned) == 2, "collected and past the cooldown: the next one"
    clock["t"] = 31.0
    arrive(sched, nodes, col)
    assert len(spawned) == 2, "within POOL_GROW_SECONDS of the last spawn"
    for _ in range(10):
        clock["t"] += 11.0
        if sched._spawning:
            arrive(sched, nodes, col)
        sched.check_schedule()
    assert len(sched._live_workers()) + sched._spawning <= 6, "never past max_workers"
    assert sched.stats["pool_grown"] == 4


def test_the_pool_does_not_grow_without_memory_for_a_worker_and_a_test(tmp_path):
    clock, avail = {"t": 0.0}, {"v": 1.3 * GB + MEM_RESERVE}   # two 0.5 GB tests fit, a worker and a third do not
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail)
    assert len(committed(sched)) == 2
    assert spawned == [], "no room for a worker and the test it would run"


def test_swap_is_not_counted_either_way(tmp_path):
    """The kernel pages cold memory out as ordinary reclaim, with gigabytes free.

    So admission reads MemAvailable and nothing else.
    """
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, max_workers=0)
    assert sched._available() == 40 * GB
    assert not hasattr(sched, "tests_swap")


def test_the_tests_stalling_on_memory_stops_admission_until_it_falls_back(tmp_path):
    """memory.pressure "full" of the tests' own cgroup: all of them stalled on memory at once."""
    cg = tmp_path / "tests"; cg.mkdir()
    (cg / "cpu.stat").write_text("usage_usec 0\n")
    pressure = cg / "memory.pressure"
    pressure.write_text("some avg10=20.00 avg60=5.00 avg300=1.00 total=1\n"
                        "full avg10=4.60 avg60=1.00 avg300=0.50 total=1\n")
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    model = make_model(tmp_path, 8, {profile_key(c): (0.5, GB, 5.0) for c in col})
    sched = new_sched(FakeConfig(tmp_path, 2), model=model, ncpus=8, mem_total=64 * GB,
                             cgroup_tests=cg, available_fn=lambda: 30 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(2)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    assert sched.mem_stall == pytest.approx(4.6), "full, not some"
    held = next(i for n in nodes if (i := sched._held(n)) is not None and i not in sched.committed_at)
    node = next(n for n in nodes if sched._held(n) == held)
    assert sched._fits(held, node, pressure=False), "4.6% is a busy machine, not a short one"
    pressure.write_text("some avg10=40.00 avg60=5.00 avg300=1.00 total=1\n"
                        f"full avg10={MEM_STALL_LIMIT:.2f} avg60=1.00 avg300=0.50 total=1\n")
    sched._refresh_measurement()
    assert not sched._fits(held, node, pressure=False)
    assert sched.stats["rejected_mem_stall"] >= 1 and sched.stats["mem_stalls"] == 1
    pressure.write_text("some avg10=3.00 avg60=5.00 avg300=1.00 total=1\n"
                        "full avg10=2.00 avg60=1.00 avg300=0.50 total=1\n")
    sched._refresh_measurement()
    assert sched._fits(held, node, pressure=False), "fallen back: admit again"


def test_the_pool_does_not_grow_while_the_tests_stall_on_memory(tmp_path):
    """A test waiting for its pages burns no CPU; low load then is not spare capacity."""
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail)
    arrive(sched, nodes, col)
    before = len(spawned)
    sched.mem_stall = MEM_STALL_LIMIT
    clock["t"] = 30.0
    sched._maybe_grow_pool(pressure=False)
    assert len(spawned) == before, "all busy, but stalled on memory"
    sched.mem_stall = MEM_STALL_LIMIT - 1.0
    sched._maybe_grow_pool(pressure=False)
    assert len(spawned) == before + 1, "below the limit it grows as before"


def test_a_worker_that_is_starting_is_charged_to_the_forecast(tmp_path):
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail)
    assert sched._spawning == 1
    sched._refresh_forecasts()
    starting = sched._mem_headroom()
    arrive(sched, nodes, col)
    sched._refresh_forecasts()
    assert sched._spawning == 0
    assert sched._mem_headroom() != starting


def test_the_pool_floor_is_a_quarter_of_its_start(tmp_path):
    """Idle workers hold their heaps; the pool may drain to a quarter of its start, never under two."""
    for start, floor in ((32, 8), (8, 2), (4, 2)):
        (tmp_path / str(start)).mkdir()
        sched, *_ = make_pool_sched(tmp_path / str(start), {"t": 0.0}, {"v": 40 * GB}, n_start=start, n_tests=4 * start)
        assert sched._pool_floor == floor, start


def test_the_pool_shrinks_back_when_workers_sit_idle(tmp_path):
    """Workers with nothing running for POOL_IDLE_SECONDS are drained, one per cooldown, down to the start size.

    One file per test, so a worker that waits holds the first test of a module it
    has not started (see test_a_worker_in_the_middle_of_a_module_is_not_drained)."""
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=2, max_workers=6, n_tests=60,
                                                 one_file=False)
    for _ in range(20):                                  # grow to 6 workers, each arriving as it is spawned
        if len(sched._live_workers()) == 6:
            break
        clock["t"] += 11.0
        if sched._spawning:
            arrive(sched, nodes, col)
        sched.check_schedule()
    assert len(sched._live_workers()) == 6 and len(spawned) == 4
    # memory runs short: the running tests finish and nothing more fits, so the workers idle
    avail["v"] = 0.3 * GB + MEM_RESERVE
    for node in nodes:
        for idx in [i for i in sched.node2pending[node] if i in sched.committed_at]:
            sched.mark_test_complete(node, idx)
    assert len(committed(sched)) <= 1, "only the progress rule's one test keeps running"
    clock["t"] += 30.0
    sched.check_schedule()
    assert sched.stats["pool_drained"] == 0, "not idle long enough yet"
    for _ in range(10):
        clock["t"] += 61.0
        sched.check_schedule()
    assert len(sched._draining) + sum(n._shutdown_sent for n in nodes) == 4, "down to the two it started with"
    # memory comes back: a draining worker runs its held test and exits
    avail["v"] = 40 * GB
    sched.check_schedule()
    draining_done = [n for n in nodes if n._shutdown_sent]
    assert draining_done and all(any(i in sched.committed_at for i in sched.node2pending[n]) for n in draining_done)


def test_a_drained_worker_gives_its_held_test_back(tmp_path, monkeypatch):
    """A worker drained for sitting idle is told to skip the test it holds, which goes
    back to the queue: run on the way out, it would start past admission, and it needs
    a fresh setup wherever it runs."""
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "sched")
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=2, max_workers=4, n_tests=40,
                                                 one_file=False)
    for _ in range(20):
        if len(sched._live_workers()) == 4:
            break
        clock["t"] += 11.0
        if sched._spawning:
            arrive(sched, nodes, col)
        sched.check_schedule()
    avail["v"] = 0.3 * GB + MEM_RESERVE                  # nothing more fits: the workers idle
    for node in nodes:
        for idx in [i for i in sched.node2pending[node] if i in sched.committed_at]:
            sched.mark_test_complete(node, idx)
    for _ in range(5):
        clock["t"] += 61.0
        sched.check_schedule()
    evicted = [n for n in nodes if n in sched._evicted]
    assert evicted and sched.stats["evicted"] == len(evicted)
    for node in evicted:
        held = sched._evicted[node]
        assert node._shutdown_sent and held not in sched.committed_at, "sent home without running it"
        assert (tmp_path / "sched" / "evictions" / node.gateway.id).read_text() == col[held]


def test_a_test_held_too_long_gets_room_kept_instead_of_being_forced(tmp_path):
    """Small tests refilling every freed core can starve a big one held on a worker.
    After HELD_RESERVE_SECONDS room is kept for it: the small ones only use what is
    left beside it, and it starts once enough has finished, never over the budget."""
    clock = {"t": 1000.0}
    col = ["a.py::big.dev.1"] + [f"b{i}.py::s.dev.1" for i in range(60)]
    costs = {"a.py::big.dev.1": (3.0, 1e8, 15.0)}                 # too short to be the critical path
    costs.update({f"b{i}.py::s.dev.1": (1.0, 1e8, 15.0) for i in range(60)})
    model = make_model(tmp_path, 4, {profile_key(n): c for n, c in costs.items()})
    sched = new_sched(FakeConfig(tmp_path, 5), model=model, ncpus=4, mem_total=20 * GB, cpu_target=1.0,
                      cpu_overcommit=1.0, cgroup_tests=NO_CGROUP, now=lambda: clock["t"],
                      available_fn=lambda: 20 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(5)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.collection = col
    for i in range(len(col)):
        sched._add_pending(i)
    big = 0
    sched._take(sched._file_of(big), big)              # gw0 holds it; the other four run small ones
    sched._send(nodes[0], big)
    for node, small in zip(nodes[1:], (1, 2, 3, 4)):
        sched._take(sched._file_of(small), small)
        sched._send(node, small)
        sched._commit(small, node)
    sched.check_schedule()
    assert big not in committed(sched)
    started = clock["t"]
    while big not in committed(sched) and clock["t"] - started < 10 * HELD_RESERVE_SECONDS:
        clock["t"] += 4.0
        idx = min(committed(sched), key=lambda i: sched.committed_at[i])
        sched.mark_test_complete(next(n for n in nodes if idx in sched.node2pending[n]), idx)
        if clock["t"] - started < HELD_RESERVE_SECONDS:
            assert big not in committed(sched) and len(committed(sched)) == 4, "the small ones refill the machine"
        assert sum(sched.res_cpu.values()) <= 4.0, "never started over the budget"
    assert sched.stats["reservations"] == 1
    assert big in committed(sched) and clock["t"] - started <= HELD_RESERVE_SECONDS + 4 * 4.0, \
        "it starts once the small ones running when room was kept for it are done"


def test_a_worker_in_the_middle_of_a_module_is_not_drained(tmp_path):
    """However long admission keeps the next test of its module waiting: draining
    would start that test regardless and send the rest of the module elsewhere."""
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=2, max_workers=6, n_tests=10)
    sched._pool_floor = 0
    node = nodes[0]
    # its last test, of a.py, is over and admission keeps a.py's next test waiting on it
    sched.committed_at.clear()
    sched.node2pending[node] = [5]
    sched._last_file_done[node] = sched._file_of(4)
    assert sched._continues_module(node)
    for _ in range(10):
        clock["t"] += 61.0
        sched._maybe_shrink_pool()
    assert node not in sched._draining and not node._shutdown_sent


def test_a_bloated_worker_is_recycled(tmp_path):
    """A worker that holds 3 GB when its tests start, against 0.3 GB for its peers, is sent home."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=4, max_workers=4, n_tests=20,
                                                 one_file=False)
    sched._pool_floor = 4                              # no draining in the way: this is about recycling
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    for i, node in enumerate(nodes):
        live[node.gateway.id] = 3 * GB if i == 0 else 0.3 * GB
    # every worker finishes its test; the next ones start with the leftovers measured
    clock["t"] = 100.0
    for node in nodes:
        running = next(i for i in sched.node2pending[node] if i in sched.committed_at)
        sched.mark_test_complete(node, running)
    assert sched.worker_cost() == pytest.approx(0.3 * GB)
    assert sched.stats["recycled"] == 1
    assert nodes[0]._shutdown_sent and not any(n._shutdown_sent for n in nodes[1:])


def test_a_fixed_pool_never_recycles(tmp_path):
    """With -j the pool cannot grow back, so a recycled worker would be lost for the rest of the run."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=4, max_workers=0, n_tests=20,
                                                 one_file=False)
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    for i, node in enumerate(nodes):
        live[node.gateway.id] = 3 * GB if i == 0 else 0.3 * GB
    clock["t"] = 100.0
    for node in nodes:
        sched.mark_test_complete(node, next(i for i in sched.node2pending[node] if i in sched.committed_at))
    assert sched.stats["recycled"] == 0


def test_a_module_cluster_kept_for_the_next_test_is_not_worker_overhead(tmp_path):
    """After a test of the same file the worker still holds that module's cluster, which the next test reuses."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=4, max_workers=8, n_tests=20)
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    for node in nodes:
        live[node.gateway.id] = 3 * GB
    clock["t"] = 100.0
    for node in nodes:
        sched.mark_test_complete(node, next(i for i in sched.node2pending[node] if i in sched.committed_at))
    assert sched._idle_mem == {}, "one file: every worker's 3 GB is its module's cluster"
    assert sched.stats["recycled"] == 0


def test_a_worker_holding_its_modules_cluster_adds_only_what_the_test_takes_beyond_it(tmp_path):
    """A peak is learned as the whole worker cgroup; what the worker holds already is not new memory."""
    avail, clock, live = {"v": 10 * GB}, {"t": 0.0}, {}
    col = [f"a.py::t{i}.dev.1" for i in range(8)]
    model = make_model(tmp_path, 16, {profile_key(c): (0.1, 2.2 * GB, 60.0) for c in col})
    sched = new_sched(FakeConfig(tmp_path, 8), model=model, ncpus=16, mem_total=20 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    nodes = [FakeNode(f"gw{i}") for i in range(8)]
    for node in nodes:
        live[node.gateway.id] = 2 * GB                   # each worker keeps its module's 2 GB node
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    # 10 GB available less a 1 GB reserve: at 2.2 GB each, four would fit; at 0.2 GB each, all eight do
    assert len(committed(sched)) == 8


def test_a_recycled_worker_is_replaced_to_keep_the_pool_at_its_floor(tmp_path):
    """The pool never stays below its floor: a recycled worker is refilled at once."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail, n_start=4, max_workers=8, n_tests=40,
                                                 cores=4.0, one_file=False)   # 4 x 4 cores: the CPU is full, no growth
    sched._pool_floor = 4                              # a floor of four, so that recycling one drops below it
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    assert spawned == []
    for i, node in enumerate(nodes):
        live[node.gateway.id] = 3 * GB if i == 0 else 0.3 * GB
    clock["t"] = 100.0
    for node in nodes:
        running = next(i for i in sched.node2pending[node] if i in sched.committed_at)
        sched.mark_test_complete(node, running)
    assert sched.stats["recycled"] == 1
    clock["t"] = 200.0
    sched.check_schedule()
    assert len(spawned) == 1, "below the floor: refilled even though the CPU is busy"


def test_a_merged_samples_file_is_not_merged_again(tmp_path):
    """With a fixed SCYLLA_TEST_HOST_ID a left-behind samples file would be learned by every later run."""
    from test.pylib.dynamic_scheduler import append_sample, merge_run_into_profile, samples_path
    append_sample(tmp_path, {"key": "dev|f.py::t", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 1e9})
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 1
    assert not samples_path(tmp_path).exists()
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 0
    assert CostModel(tmp_path / "profile.json", 8).tests["dev|f.py::t"]["n"] == 1


def test_learning_drops_only_the_learned_files_cached_costs(tmp_path):
    col = ["a.py::t1.dev.1", "a.py::t2.dev.1", "b.py::t1.dev.1"]
    sched, nodes = make_sched(tmp_path, col, {n: (0.5, 1e9, 1.0) for n in col}, nodes=1)
    for idx in range(3):
        sched._costs_for(idx)
    pending = [i for i in range(3) if i not in sched.committed_at]
    sched.learn({"key": "dev|a.py::t1", "wall": 1.0, "usage_sec": 0.5, "memory_peak": 1e9})
    for idx in pending:
        assert (idx in sched._costs) == (sched._profile_file_of(idx) != "dev|a.py"), idx


def _scout_and_held_sched(tmp_path, clock, avail, live, ready_running=False):
    """A started scout on gw0, its waiting sibling held on gw1, a ready cqlpy test held on gw2."""
    col = ["cluster/dtest/heavy_test.py::test_0.release.1", "cluster/dtest/heavy_test.py::test_1.release.1",
           "cqlpy/test_x.py::test_a.release.1", "cqlpy/test_x.py::test_b.release.1"]
    model = CostModel(tmp_path / "p.json", ncpus=16, k_sigma=0.0, mode="release")
    for name in ("test_a", "test_b"):
        model.tests[f"release|cqlpy/test_x.py::{name}"] = {"cores": 0.5, "mem": 0.3 * GB, "wall": 1.0,
                                                          "var_cores": 0.0, "n": 3, "n_unc": 3}
    sched = new_sched(FakeConfig(tmp_path, 3), max_workers=8, model=model, ncpus=16,
                             mem_total=20 * GB, cgroup_tests=NO_CGROUP, now=lambda: clock["t"],
                             available_fn=lambda: avail["v"])
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    nodes = [FakeNode(f"gw{i}") for i in range(3)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.collection = col
    for i in range(len(col)):
        sched._add_pending(i)
    sched._pool_floor = 1
    for idx, node in ((0, nodes[0]), (1, nodes[1]), (2, nodes[2])):
        sched._take(sched._file_of(idx), idx)
        sched._send(node, idx)
    sched._commit(0, nodes[0])                         # the scout runs
    if ready_running:                                  # gw2 runs its test, with a successor held behind it
        sched._take(sched._file_of(3), 3)
        sched._commit(2, nodes[2])
        sched._send(nodes[2], 3)
    assert sched._waits_for_first_run(1), "the sibling waits for its scout"
    return sched, nodes


def test_retiring_a_worker_never_starts_its_held_test(tmp_path):
    """Without a way to tell it to skip its test, a worker holding one is never sent home:
    the shutdown would start the test, waiting or not."""
    clock, avail = {"t": 5.0}, {"v": 0.2 * GB}          # short: nothing held fits
    live = {"gw1": 2 * GB, "gw2": 1 * GB}
    sched, nodes = _scout_and_held_sched(tmp_path, clock, avail, live)
    sched._ram_guard()
    assert sched.stats["retired"] == 0
    assert not any(n._shutdown_sent for n in nodes) and 1 not in sched.committed_at and 2 not in sched.committed_at


def test_a_stalled_run_sends_home_an_idle_worker_that_holds_nothing(tmp_path):
    """Its heap and leftovers are memory no test can use, and nothing starts when it goes."""
    clock, avail = {"t": 5.0}, {"v": 0.2 * GB}
    live = {"gw1": 2 * GB, "gw2": 1 * GB, "gw3": 1.5 * GB}
    sched, nodes = _scout_and_held_sched(tmp_path, clock, avail, live)
    spare = FakeNode("gw3")
    sched.add_node(spare)
    sched._ram_guard()
    assert sched.stats["retired"] == 1 and spare._shutdown_sent
    assert not any(n._shutdown_sent for n in nodes)


def _evicting_sched(tmp_path, monkeypatch):
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "sched")
    evictions = tmp_path / "sched" / "evictions"
    clock, avail = {"t": 5.0}, {"v": 0.2 * GB}          # short: nothing held fits
    live = {"gw1": 2 * GB, "gw2": 1 * GB}
    sched, nodes = _scout_and_held_sched(tmp_path, clock, avail, live)
    sched._ram_guard()
    return sched, nodes, evictions


def test_a_stalled_run_evicts_the_idle_worker_holding_most_without_starting_its_test(tmp_path, monkeypatch, caplog):
    """A held test cannot be recalled, so its worker is told to skip it, then sent home."""
    caplog.set_level(logging.DEBUG, logger="test.pylib.dynamic_scheduler")
    sched, nodes, evictions = _evicting_sched(tmp_path, monkeypatch)
    assert sched.stats["evicted"] == 1 and sched.stats["retired"] == 0
    assert nodes[1]._shutdown_sent and not nodes[0]._shutdown_sent and not nodes[2]._shutdown_sent
    assert (evictions / "gw1").read_text() == sched.collection[1]
    assert 1 not in sched.committed_at and 1 not in sched.pending_set
    assert not sched.tests_finished, "the evicted test is still to be run"
    assert sched._committed(nodes[1]) == [], "a test it skips is not running on it"
    text = caplog.text
    assert "evicting gw1" in text and "on purpose" in text and "cannot start" in text


def test_an_evicted_test_is_queued_again_once_its_worker_skipped_it(tmp_path, monkeypatch):
    sched, nodes, evictions = _evicting_sched(tmp_path, monkeypatch)
    assert take_eviction("gw1", sched.collection[1]), "what the worker's runner sees"
    completed = sched.stats["completed"]
    sched.mark_test_complete(nodes[1], 1)
    assert sched.stats["completed"] == completed, "skipped, not completed"
    assert sched.stats["evicted_requeued"] == 1 and 1 in sched.pending_set
    assert sched.remove_node(nodes[1]) is None, "xdist asserts a clean exit leaves no crashed test"
    assert sched.collection[1] in [sched.collection[i] for i in sched.pending_set]


def test_an_evicted_worker_that_dies_first_crashes_nothing(tmp_path, monkeypatch):
    sched, nodes, _ = _evicting_sched(tmp_path, monkeypatch)
    assert sched.remove_node(nodes[1]) is None
    assert 1 in sched.pending_set and not sched._evicted


def test_a_worker_skips_only_the_test_it_was_told_to(tmp_path, monkeypatch):
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "sched")
    (tmp_path / "sched" / "evictions").mkdir(parents=True)
    (tmp_path / "sched" / "evictions" / "gw3").write_text("a.py::t1")
    assert not take_eviction("gw3", "a.py::t2")
    assert not take_eviction("gw4", "a.py::t1")
    assert not take_eviction(None, "a.py::t1")
    assert take_eviction("gw3", "a.py::t1")
    assert not take_eviction("gw3", "a.py::t1"), "the flag is consumed"
    monkeypatch.setattr(sched_dir, "_root", None)
    assert not take_eviction("gw3", "a.py::t1")


def test_recycling_skips_a_worker_whose_test_waits_for_its_scout(tmp_path):
    clock, avail = {"t": 5.0}, {"v": 10 * GB}
    sched, nodes = _scout_and_held_sched(tmp_path, clock, avail, {})
    # gw1 and gw2 both bloated against typical workers at 0.3 GB
    sched._idle_mem = {"gw0": 0.3 * GB, "gwA": 0.3 * GB, "gwB": 0.3 * GB, "gw1": 3 * GB, "gw2": 3 * GB}
    sched._recycle_bloated_worker()
    assert sched.stats["recycled"] == 1
    assert nodes[2]._shutdown_sent and not nodes[1]._shutdown_sent
    assert 1 not in sched.committed_at


def test_a_first_in_file_sample_still_teaches_the_peak(tmp_path):
    """A small file is first-in-file on every worker; its tests' peaks must still be learned."""
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 1e9})
    model.learn({"key": "dev|f.py::a", "wall": 9.0, "usage_sec": 18.0, "memory_peak": 4e9, "first_in_file": True})
    e = model.tests["dev|f.py::a"]
    assert e["mem"] == pytest.approx(4e9), "a higher peak is taken at once, first-in-file or not"
    assert e["wall"] == pytest.approx(2.0) and e["n"] == 1, "its setup time stays out of the test's entry"
    assert model.files["dev|f.py"]["n"] == 1


def test_each_test_is_priced_for_its_own_build_mode(tmp_path):
    """--mode dev --mode debug: a debug cluster test gets debug's first-run price, not dev's."""
    model = CostModel(tmp_path / "p.json", ncpus=8, mode="dev")
    assert model.cost("cluster/test_x.py::test_y.debug.1").mem == pytest.approx(8.5e9)
    assert model.cost("cluster/test_x.py::test_y.dev.1").mem == pytest.approx(0.6e9)
    assert model.kind_dist("dtest", model.mode_of("debug|cluster/dtest/x.py::t")) is not None
    assert model.kind_dist("dtest", model.mode_of("dev|cluster/dtest/x.py::t")) is None


def test_a_crash_clone_does_not_finish_a_spawn(tmp_path):
    """xdist replaces a crashed worker on its own; only the worker we spawned ends our spawn."""
    clock, avail = {"t": 0.0}, {"v": 40 * GB}
    sched, nodes, col, spawned = make_pool_sched(tmp_path, clock, avail)
    assert sched._spawning == 1                       # gw2 is on its way
    clone = FakeNode("gw9")
    sched.add_node(clone)
    sched.add_node_collection(clone, col)
    assert sched._spawning == 1, "gw9 is xdist's clone, not the worker we started"
    arrive(sched, nodes, col)
    assert sched._spawning == 0


def test_a_test_committed_behind_a_running_one_starts_when_it_ends(tmp_path):
    """Its forecast must not fade while it waits: its clocks start when it really starts."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    model = make_model(tmp_path, 16, {profile_key(c): (0.1, 5 * GB, 300.0) for c in col})
    sched = new_sched(FakeConfig(tmp_path, 1), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    node = FakeNode("gw0")
    sched.add_node(node); sched.add_node_collection(node, col)
    sched.schedule()
    running, behind = sched.node2pending[node][0], sched.node2pending[node][1]
    sched._commit(behind, node)                        # as a retirement or the end of a run does
    assert running in sched._started_at and behind not in sched._started_at
    clock["t"] = 600.0                                 # ten minutes: long past any plateau
    sched._refresh_forecasts()
    assert sched._growth_weight(behind, clock["t"]) == 1.0, "it has not started: its forecast counts in full"
    sched.mark_test_complete(node, running)
    assert sched._started_at[behind] == 600.0


def test_remove_node_distinguishes_held_from_running(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    sched, nodes = make_sched(tmp_path, col, {n: (3.0, 1e9, 1.0) for n in col})
    runner = next(n for n in nodes if len(n.sent) == 2)
    waiter = next(n for n in nodes if n is not runner)
    held_only = min(sched.pending_set)                # handed to it, never started
    sched._take(sched._file_of(held_only), held_only)
    sched._send(waiter, held_only)
    waiter._down = True
    assert sched.remove_node(waiter) is None          # never started -> not a crash
    assert held_only in sched.pending_set or any(held_only in sched.node2pending[n] for n in sched.node2pending)
    running = runner.sent[0]
    runner._down = True
    assert sched.remove_node(runner) == col[running]  # was running -> crash item
    assert running not in sched.committed_at


def _node_of(sched: DynamicScheduling, idx: int) -> FakeNode:
    return next(n for n in sched.node2pending if idx in sched.node2pending[n])


def test_repeats_of_an_unknown_test_start_on_what_the_first_copy_measured(tmp_path):
    """--repeat of a test the profile does not know: one copy measures, the rest follow.

    Starting every copy at once on the static guess would admit them all against a number
    that is not the test's, which is exactly what the first copy is there to replace.
    """
    col = [f"cluster/test_r.py::t.dev.{i}" for i in range(1, 5)]
    sched, nodes = make_sched(tmp_path, col, {}, nodes=4, ncpus=16)
    assert len(committed(sched)) == 1
    first = next(iter(committed(sched)))
    assert sched._costs_for(first).source == "static-cluster"
    # the other copies wait for it, and no idle worker is handed one meanwhile
    assert all(sched._waits_for_first_run(i) for i in range(4) if i != first)
    assert all(not n.sent for n in nodes if first not in n.sent)
    sched.learn({"key": profile_key(col[first]), "wall": 3.0, "usage_sec": 1.5, "memory_peak": 3 * GB})
    sched.mark_test_complete(_node_of(sched, first), first)
    rest = committed(sched)
    assert rest == set(range(4)) - {first}
    for i in rest:
        c = sched._costs_for(i)
        assert c.source == "profile"
        assert c.mem == pytest.approx(3 * GB)
        assert sched.res_mem[i] == pytest.approx(3 * GB)
    for i in rest:
        sched.mark_test_complete(_node_of(sched, i), i)
    assert sched.tests_finished


def test_repeats_of_a_profiled_test_do_not_wait(tmp_path):
    col = [f"cluster/test_r.py::t.dev.{i}" for i in range(1, 5)]
    sched, nodes = make_sched(tmp_path, col, {col[0]: (1.0, 1 * GB, 3.0)}, nodes=4, ncpus=16)
    assert committed(sched) == set(range(4))
    assert sched.stats["held_first_run"] == 0


def test_a_first_copy_that_reports_nothing_still_releases_the_rest(tmp_path):
    col = [f"cqlpy/test_r.py::t.dev.{i}" for i in range(1, 4)]
    sched, nodes = make_sched(tmp_path, col, {}, nodes=3, ncpus=16)
    first = next(iter(committed(sched)))
    sched.mark_test_complete(_node_of(sched, first), first)    # no sample, e.g. it crashed in setup
    assert committed(sched) == set(range(3)) - {first}


def test_a_parked_copy_takes_over_when_the_first_copy_crashes(tmp_path):
    col = [f"cluster/test_r.py::t.dev.{i}" for i in range(1, 5)]
    sched, nodes = make_sched(tmp_path, col, {}, nodes=4, ncpus=16)
    first = next(iter(committed(sched)))
    node = _node_of(sched, first)
    node._down = True
    assert sched.remove_node(node) == col[first]
    # exactly one of the parked copies starts, and measures for the others
    assert len(committed(sched)) == 1
    successor = next(iter(committed(sched)))
    assert sched._first_run[profile_key(col[successor])] == successor
    assert all(sched._waits_for_first_run(i) for n in sched.node2pending
               for i in sched.node2pending[n] if i != successor)


def test_other_tests_run_while_repeats_wait(tmp_path):
    """Waiting copies are the last resort: a worker takes any other test first."""
    reps = [f"cluster/test_r.py::t.dev.{i}" for i in range(1, 4)]
    others = [f"cqlpy/test_o.py::o{i}.dev.1" for i in range(3)]
    col = reps + others
    sched, nodes = make_sched(tmp_path, col, {n: (0.5, 0.5 * GB, 1.0) for n in others}, nodes=4, ncpus=16)
    started = {col[i] for i in committed(sched)}
    assert len(started & set(reps)) == 1
    assert set(others) <= started | {col[i] for n in nodes for i in n.sent}


def test_file_affinity_and_longest_first(tmp_path):
    col = ["a.py::a1.dev.1", "a.py::a2.dev.1", "a.py::a3.dev.1", "b.py::b1.dev.1", "b.py::b2.dev.1"]
    costs = {"a.py::a1.dev.1": (0.1, 1e8, 5.0), "a.py::a2.dev.1": (0.1, 1e8, 1.0), "a.py::a3.dev.1": (0.1, 1e8, 1.0),
             "b.py::b1.dev.1": (0.1, 1e8, 10.0), "b.py::b2.dev.1": (0.1, 1e8, 1.0)}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=2, ncpus=16)
    first = [n.sent[0] for n in nodes]
    # b has the most remaining wall (11 s) so its longest test goes out first, then a's longest
    assert col[first[0]] == "b.py::b1.dev.1" and col[first[1]] == "a.py::a1.dev.1"
    # successors stay in the same file
    assert col[nodes[0].sent[1]].startswith("b.py")
    assert col[nodes[1].sent[1]].startswith("a.py")


def test_the_target_follows_pressure_as_a_pi_controller(tmp_path, monkeypatch):
    """The CPU target is the full one plus TARGET_KP of the CPUs per point of
    pressure below the limit, plus that error integrated at TARGET_KI: a lasting
    overload cuts it more and more, down to its floor and no further, and once
    the pressure is gone it climbs back to the full target and stops there."""
    import test.pylib.dynamic_scheduler as bs
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    clock = {"t": 1000.0}
    psi = {"cpu": 30.0}
    monkeypatch.setattr(bs, "read_psi", lambda kind, path=None, line_kind="some": psi["cpu"] if kind == "cpu" else 0.0)
    model = make_model(tmp_path, 4, {profile_key(n): (0.5, 1e8, 1.0) for n in col})
    sched = new_sched(FakeConfig(tmp_path, 1), psi_cpu_limit=20.0, model=model, ncpus=8,
                      mem_total=20 * GB, cgroup_tests=NO_CGROUP, now=lambda: clock["t"])
    full = sched.cpu_target
    assert sched._pressure_guard()
    assert sched.cpu_target == pytest.approx(full - bs.TARGET_KP * 8 * 10), "the first reading: proportional only"
    clock["t"] += 4
    sched._pressure_guard()
    assert sched.cpu_target == pytest.approx(full - bs.TARGET_KP * 8 * 10 - bs.TARGET_KI * 8 * 10 * 4)
    assert sched.stats["pressure_cuts"] == 1
    for _ in range(600):
        clock["t"] += 1
        sched._pressure_guard()
    assert sched.cpu_target == pytest.approx(sched.cpu_target_floor), "never below the floor"
    psi["cpu"] = 20.0
    clock["t"] += 1
    sched._pressure_guard()
    assert sched.cpu_target == pytest.approx(sched.cpu_target_floor), "the integral did not wind up past the floor"
    psi["cpu"] = 0.0
    for _ in range(600):
        clock["t"] += 1
        assert not sched._pressure_guard()
    assert sched.cpu_target == pytest.approx(full), "back to the full target, not past it"

def test_a_test_is_predicted_at_its_average_parallelism(tmp_path):
    """One number per test: CPU-seconds over wall time, and nothing once it is well past its wall."""
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::t", "wall": 6.0, "usage_sec": 12.0, "memory_peak": 1.2e9})
    assert model.predict_at("f.py::t.dev.1", 0.5) == pytest.approx(2.0)
    assert model.predict_at("f.py::t.dev.1", 5.0) == pytest.approx(2.0)
    assert model.predict_at("f.py::t.dev.1", 30.0, future=True) == 0.0           # long past its wall


def test_a_just_started_test_counts_at_its_forecast_not_what_it_holds(tmp_path):
    """Two 11 GB tests on a 20 GB machine: the second waits even though nothing is using RAM yet."""
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 11 * GB, 5.0) for n in col}, nodes=3, ncpus=16)
    assert len(committed(sched)) == 1
    assert sum(sched.res_mem.values()) == pytest.approx(11 * GB)   # request = the peak, no margin
    assert sched.stats["rejected_mem"] >= 1
    # completion releases the reservation and the next one is admitted
    running = next(iter(committed(sched)))
    node = next(n for n in nodes if running in sched.node2pending[n])
    sched.mark_test_complete(node, running)
    assert running not in sched.res_mem and len(committed(sched)) == 1


def test_backfill_past_unfittable_head(tmp_path):
    """Head of the queue needs 32 GB on a 20 GB box; the small tests behind it must run."""
    col = ["a.py::big.dev.1", "b.py::small1.dev.1", "b.py::small2.dev.1"]
    costs = {"a.py::big.dev.1": (4.0, 32 * GB, 100.0), "b.py::small1.dev.1": (2.0, 4 * GB, 1.0),
             "b.py::small2.dev.1": (4.0, 8 * GB, 1.0)}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=25 * GB)   # budget 20 GB
    sched.check_schedule()      # the second backfilled test is held until the next tick commits it
    names = sorted(col[i] for i in committed(sched))
    assert "a.py::big.dev.1" not in names and len(names) == 2      # both small ones run
    assert sched.stats["backfilled"] >= 1


def test_longest_test_starts_first(tmp_path):
    """The long pole goes in when the machine is still empty, which is when it fits.

    Ordering by a file's total remaining work buried a 120 s test that lives in a small
    file behind whole directories of short ones, so it started once everything else had
    drained and then ran alone. Ordering by the longest single test puts it first.
    """
    col = ["b.py::s0.dev.1", "b.py::s1.dev.1", "b.py::s2.dev.1", "b.py::s3.dev.1", "a.py::hog.dev.1"]
    costs = {"a.py::hog.dev.1": (1.0, 8 * GB, 120.0)}
    costs.update({f"b.py::s{i}.dev.1": (1.0, 2 * GB, 30.0) for i in range(4)})
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=40 * GB)
    hog = col.index("a.py::hog.dev.1")
    assert hog in committed(sched), "the longest test must be among the first admitted"
    # and when it cannot fit, its capacity is held rather than lost to the tail
    col2 = ["a.py::hog.dev.1"] + [f"b.py::s{i}.dev.1" for i in range(20)]
    costs2 = {"a.py::hog.dev.1": (1.0, 9 * GB, 120.0)}
    costs2.update({f"b.py::s{i}.dev.1": (1.0, 8 * GB, 10.0) for i in range(20)})
    sub = tmp_path / "b"; sub.mkdir(exist_ok=True)
    sched2, nodes2 = make_sched(sub, col2, costs2, nodes=4, ncpus=2, mem_total=30 * GB)
    for _ in range(20):
        if 0 in committed(sched2) or sched2.hold_for == 0:
            break
        if not committed(sched2):
            break
        idx = sorted(committed(sched2))[0]
        node = next(n for n in sched2.node2pending if idx in sched2.node2pending[n])
        sched2.mark_test_complete(node, idx)
    assert 0 in committed(sched2) or sched2.hold_for == 0


def test_selection_counts_what_other_workers_already_hold(tmp_path):
    """Two workers must not both choose a test that only one of them can run.

    A test sent to a worker cannot be recalled, so if selection ignored the tests its
    colleagues are holding, every worker would pick something heavy at the same moment
    and the losers would sit blocked until the memory freed up.
    """
    col = [f"a.py::big{i}.dev.1" for i in range(2)] + [f"b.py::s{i}.dev.1" for i in range(6)]
    costs = {f"a.py::big{i}.dev.1": (1.0, 12 * GB, 60.0) for i in range(2)}
    costs.update({f"b.py::s{i}.dev.1": (1.0, 1 * GB, 5.0) for i in range(6)})
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=25 * GB)   # budget 20 GB
    chosen = [q[-1] for q in sched.node2pending.values() if q]
    big = {i for i, n in enumerate(col) if "big" in n}
    assert len(set(chosen) & big) <= 1, "only one worker may hold a 12 GB test on a 20 GB budget"
    assert len(chosen) == 3, "the other workers still get work, just lighter work"


def test_a_blocked_worker_is_never_unblocked_past_memory(tmp_path):
    """A worker stuck on a test admission keeps refusing is freed, but not by swapping.

    CPU is compressible: one test over that budget makes everything slightly slower.
    Memory is not: forcing a test the machine has no room for costs more in reclaim
    than the test was ever going to use.
    """
    clock, free = {"t": 1000.0}, {"gb": 40.0}
    col = ["a.py::hog.dev.1", "b.py::s0.dev.1"]
    costs = {"a.py::hog.dev.1": (1.0, 12 * GB, 60.0), "b.py::s0.dev.1": (1.0, 4 * GB, 5.0)}
    model = make_model(tmp_path, 4, {profile_key(n): c for n, c in costs.items()})
    sched = new_sched(FakeConfig(tmp_path, 2), model=model, ncpus=4, mem_total=40 * GB,
                             cgroup_tests=NO_CGROUP, now=lambda: clock["t"],
                             available_fn=lambda: free["gb"] * GB)
    nodes = [FakeNode("gw0"), FakeNode("gw1")]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.collection = col
    for i in range(len(col)):
        sched._add_pending(i)
    hog, small = 0, 1
    sched._take(sched._file_of(hog), hog)          # gw0 is holding it, un-started
    sched._send(nodes[0], hog)
    sched._take(sched._file_of(small), small)      # gw1 is running something, so the
    sched._send(nodes[1], small)                   # dead-lock rule does not apply
    sched._commit(small, nodes[1])

    free["gb"] = 8.0                               # something else takes the memory
    clock["t"] += 10 * HELD_RESERVE_SECONDS          # long past the escape hatch
    sched.check_schedule()
    assert hog not in committed(sched), "must not be forced onto a machine with no room"
    assert sched.stats["forced_held"] == 0

    free["gb"] = 40.0                              # the memory comes back
    for _ in range(4):
        clock["t"] += 1.0
        sched.check_schedule()
        if hog in committed(sched):
            break
    assert hog in committed(sched), "once there is room it must start"


def test_the_critical_hold_stays_until_the_test_starts(tmp_path):
    """Handed to a worker is not started: the room is kept until it really goes in."""
    col = ["a.py::big.dev.1"] + [f"b.py::s{i}.dev.1" for i in range(9)]
    costs = {"a.py::big.dev.1": (1.0, 12 * GB, 200.0)}
    costs.update({f"b.py::s{i}.dev.1": (1.0, 4 * GB, 10.0) for i in range(9)})
    model = make_model(tmp_path, 16, {profile_key(n): c for n, c in costs.items()})
    # 24 GB available: the 3.2 GB reserve, plus room for the 12 GB test once one small test
    # is gone -- the two still running have just started and count at their full 8 GB.
    sched = new_sched(FakeConfig(tmp_path, 4), model=model, ncpus=16, mem_total=64 * GB,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: 24 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(4)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.collection = col
    for i in range(len(col)):
        sched._add_pending(i)
    big = 0
    for node, small in zip(nodes[1:], (1, 2, 3)):     # 12 GB of small tests running
        sched._take(sched._file_of(small), small)
        sched._send(node, small)
        sched._commit(small, node)
    sched.hold_for = big                               # chosen as critical ...
    sched._take(sched._file_of(big), big)              # ... and handed to gw0, where it waits
    sched._send(nodes[0], big)

    sched.check_schedule()
    assert sched.hold_for == big, "the hold must not move on while the test waits on a worker"
    assert committed(sched) == {1, 2, 3}, "no small test may take the room being kept for it"

    sched.mark_test_complete(nodes[1], 1)              # 8 GB running: now 12 GB fit
    assert big in committed(sched)
    assert sched.hold_for != big


def test_admission_ramps_instead_of_bursting(tmp_path):
    """Reservations may only grow at the burst rate, so a cold start cannot commit everything at once.

    Completions do not lower the ceiling, so refilling a freed slot is immediate: the
    ramp limits growth of the running set, not churn within it.
    """
    clock = {"t": 1000.0}
    col = [f"a.py::t{i}.dev.1" for i in range(8)]
    model = make_model(tmp_path, 4, {profile_key(n): (1.0, 1e8, 10.0) for n in col})
    sched = new_sched(FakeConfig(tmp_path, 8), burst=0.25, model=model, ncpus=4,
                             mem_total=20 * GB, cgroup_tests=NO_CGROUP, now=lambda: clock["t"],
                             available_fn=lambda: 20 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(8)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    assert len(committed(sched)) == 2              # 2 cores of ceiling on the first pass
    clock["t"] += 2.0                              # + 2 s x 1 core/s
    sched.check_schedule()
    assert len(committed(sched)) == 4
    clock["t"] += 10.0                             # ramp no longer binds; the CPU band does
    sched.live.cores = lambda wid: 0.5             # the four running tests burn their core each: no slack
    sched.measured_load = 4.0
    sched.check_schedule()
    assert len(committed(sched)) == 4              # 4 cores on 4 CPUs, the over-commit band needs slack
    assert sched.stats["rejected_burst"] > 0
    # a completion frees a slot and the replacement starts at once, with no new ramp
    idx = min(committed(sched), key=lambda i: sched.committed_at[i])
    node = next(n for n in sched.node2pending if idx in sched.node2pending[n])
    sched.live.cores = lambda wid: 3 / 8           # three tests still burning their core
    sched.measured_load = 3.0
    sched.mark_test_complete(node, idx)
    assert len(committed(sched)) == 4


def test_passed_over_test_still_runs(tmp_path):
    """Backfill may pass over a big test, but not for good.

    A 100-second test on a run with a few seconds of other work left is the critical
    path: room is kept for it, so the small tests stop refilling the machine and it
    starts as soon as enough of them have finished.  What must never happen is that
    it is dropped or dead-locks.
    """
    clock = {"t": 1000.0}
    col = ["a.py::big.dev.1"] + [f"b.py::s{i}.dev.1" for i in range(12)]
    costs = {"a.py::big.dev.1": (3.5, 1e8, 100.0)}                # 3.5 of 4 cores, head of the order
    costs.update({f"b.py::s{i}.dev.1": (1.0, 1e8, 1.0) for i in range(12)})
    model = make_model(tmp_path, 4, {profile_key(n): c for n, c in costs.items()})
    sched = new_sched(FakeConfig(tmp_path, 3), model=model, ncpus=4, mem_total=20 * GB,
                             cgroup_tests=NO_CGROUP, now=lambda: clock["t"], available_fn=lambda: 20 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(3)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    big = 0
    assert committed(sched) == {big}                 # at a fresh start the head runs first, as it should
    # its worker dies: big is requeued at the front while the other two workers fill up with smalls
    victim = next(n for n in nodes if big in sched.node2pending[n])
    victim._down = True
    assert sched.remove_node(victim) == col[big]      # reported as crashed by xdist ...
    sched.mark_test_pending(col[big])                   # ... and re-queued, as a rerun plugin would
    spare = FakeNode("gw3")
    sched.add_node(spare); sched.add_node_collection(spare, col); sched.schedule()
    assert big not in committed(sched) and len(committed(sched)) >= 2

    def complete_one():
        idx = min(committed(sched), key=lambda i: sched.committed_at[i])
        node = next(n for n in sched.node2pending if idx in sched.node2pending[n])
        sched.mark_test_complete(node, idx)

    busy = []
    for _ in range(20):
        clock["t"] += 1.0
        complete_one()
        busy.append(len(committed(sched)))
        if big in committed(sched):
            break
    assert big in committed(sched), "it runs"
    assert len(busy) <= 3, "the room kept for it appears after a few small tests"


def test_contention_gates_parallelism_learning(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::t", "wall": 25.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.01})
    e = model.tests["dev|f.py::t"]
    assert e["cores"] == pytest.approx(4.0) and e["n_unc"] == 1
    # a contended run (same work, twice the wall) must not lower the parallelism estimate
    model.learn({"key": "dev|f.py::t", "wall": 50.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.4})
    assert e["cores"] == pytest.approx(4.0) and e["wall"] == pytest.approx(25.0)
    assert e["n"] == 2 and e["n_unc"] == 1
    # an uncontended one does
    model.learn({"key": "dev|f.py::t", "wall": 20.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.0})
    assert e["cores"] == pytest.approx(0.7 * 4.0 + 0.3 * 5.0) and e["n_unc"] == 2


def _settle_sched(tmp_path, clock, n_tests=24, per_test=3 * GB, nodes=12, mem_total=35 * GB):
    col = [f"a.py::t{i}.dev.1" for i in range(n_tests)]
    model = make_model(tmp_path, 8, {profile_key(n): (0.2, per_test, 300.0) for n in col})
    sched = new_sched(FakeConfig(tmp_path, nodes), model=model, ncpus=8, mem_total=mem_total,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: mem_total,
                             now=lambda: clock["t"])
    fake = [FakeNode(f"gw{i}") for i in range(nodes)]
    for n in fake:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    return sched, fake


def test_settled_tests_release_the_memory_they_never_took(tmp_path):
    """A test's forecast peak held for its whole run starves the queue: peaks rarely coincide.

    Tests that have stopped growing below their forecast fade out of it, and the queue moves.
    """
    clock = {"t": 1000.0}
    sched, _ = _settle_sched(tmp_path, clock)
    at_start = len(committed(sched))
    assert at_start >= 2
    assert sched.stats["rejected_mem"] >= 1, "the forecast must be what stopped it"
    clock["t"] += 2.0
    sched.check_schedule()
    assert len(committed(sched)) == at_start, "seconds after admission they still count in full"
    clock["t"] += 120.0
    sched.check_schedule()
    assert len(committed(sched)) > at_start, "settled tests must let the queue move again"


def test_a_target_above_the_core_count_really_over_subscribes(tmp_path):
    """Asking for more than the machine has is a request to keep work runnable, not idle.

    Below 100 % the machine itself is the limit and the target only bites inside the
    over-commit band; above it, the target has to move the hard gate too or the setting
    does nothing at all.
    """
    col = [f"a.py::t{i}.dev.1" for i in range(12)]
    costs = {n: (1.0, GB, 30.0) for n in col}
    at100, _ = make_sched(tmp_path, col, costs, nodes=12, ncpus=4, mem_total=60 * GB,
                          cpu_target=1.0)
    sub = tmp_path / "over"; sub.mkdir()
    at150, _ = make_sched(sub, col, costs, nodes=12, ncpus=4, mem_total=60 * GB,
                          cpu_target=1.5)
    assert len(committed(at150)) > len(committed(at100)), (
        f"1.5 admitted {len(committed(at150))}, 1.0 admitted {len(committed(at100))}")


def test_the_budget_is_measured_once_the_workers_are_up(tmp_path):
    """Whatever the workers cost is already spent by then, so measure it rather than guess.

    The guess it replaces was a per-worker constant, and at two workers per CPU a constant
    wrong by 50 MB is wrong by gigabytes.
    """
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    model = make_model(tmp_path, 8, {profile_key(n): (0.5, GB, 5.0) for n in col})
    free = {"now": 60 * GB}
    sched = new_sched(FakeConfig(tmp_path, 8), model=model, ncpus=8, mem_total=64 * GB,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: free["now"])
    for i in range(8):
        n = FakeNode(f"gw{i}")
        sched.add_node(n); sched.add_node_collection(n, col)
    free["now"] = 42 * GB          # the workers have started and taken their share
    sched.schedule()
    assert sched.mem_target == pytest.approx(42 * GB), (
        "the budget must follow what is really left, not a per-worker estimate")


def test_no_memory_is_held_back(tmp_path):
    """The budget is what is free, all of it: no share of the machine is kept aside.

    12% of RAM held back and a 70% ceiling left 17 GB of a 22 GB free machine to debug
    cluster tests that take 7-11 GB each.  Admission's live RAM check guards the machine.
    """
    col = ["a.py::t0.dev.1"]
    model = make_model(tmp_path, 8, {profile_key(col[0]): (0.5, GB, 5.0)})
    for free in (30 * GB, 63 * GB):
        sched = new_sched(FakeConfig(tmp_path, 2), model=model, ncpus=8, mem_total=64 * GB,
                                 cgroup_tests=NO_CGROUP, available_fn=lambda: free)
        for i in range(2):
            n = FakeNode(f"gw{i}")
            sched.add_node(n); sched.add_node_collection(n, col)
        sched.schedule()
        assert sched.mem_target == pytest.approx(free)


def test_the_budget_follows_ram_not_swap(tmp_path):
    """Pages the kernel has already moved out of the tests are not RAM, and not a signal.

    Only what the machine has available, and what the tests hold in RAM, decide.
    """
    col = ["a.py::t0.dev.1", "a.py::t1.dev.1"]
    model = make_model(tmp_path, 8, {profile_key(n): (0.5, GB, 5.0) for n in col})
    sched = new_sched(FakeConfig(tmp_path, 2), model=model, ncpus=8, mem_total=64 * GB,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: 30 * GB)
    assert not hasattr(sched, "measured_swap")
    before = sched.mem_target
    for n in (FakeNode("gw0"), FakeNode("gw1")):
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    assert sched.mem_target == before == 30 * GB
    assert committed(sched) == {0, 1}


def test_a_short_machine_sends_idle_workers_home(tmp_path):
    """Idle workers hold their last module's cluster; when short, the pool is the lever.

    When no held test fits, an idle worker that holds no test goes, one per cooldown,
    never below the floor -- and with a fixed pool, never below its start.
    """
    col = [f"a.py::t{i}.dev.1" for i in range(12)]
    model = make_model(tmp_path, 8, {profile_key(n): (0.2, 10 * GB, 300.0) for n in col})
    clock, avail = {"t": 1000.0}, {"b": 64 * GB}
    sched = new_sched(FakeConfig(tmp_path, 4), max_workers=12, model=model, ncpus=8,
                             mem_total=64 * GB, cgroup_tests=NO_CGROUP, available_fn=lambda: avail["b"],
                             now=lambda: clock["t"])
    fake = [FakeNode(f"gw{i}") for i in range(4)]
    for n in fake:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    # two workers that have just come up and hold nothing yet
    empty = [FakeNode("gw4"), FakeNode("gw5")]
    for n in empty:
        sched.add_node(n)
    fake += empty
    assert all(sched.node2pending[n] for n in fake[:4])
    sched._ram_guard()
    assert sched.stats["retired"] == 0, "a held test fits: it will be admitted, nobody goes"
    avail["b"] = 5 * GB                                # no held 10 GB test can start any more
    sched._ram_guard()
    assert sched.stats["retired"] == 1
    gone = [n for n in fake if n._shutdown_sent]
    assert len(gone) == 1 and gone[0] in empty, "only a worker holding nothing goes"
    # a fixed pool cannot grow back: it keeps what it started with (4), however low its floor
    sched.max_workers = 0
    clock["t"] += 60
    sched._ram_guard()
    assert sched.stats["retired"] == 2                 # 5 workers -> 4
    clock["t"] += 60
    sched._ram_guard()
    assert sched.stats["retired"] == 2, "never below the four it started with"


def test_debug_mode_guesses_more_memory_than_release(tmp_path):
    """A first run in debug must not price tests as if they were release builds.

    The same test carries the sanitizers and no optimisation, and admission counts a
    just-started test at its predicted peak, so a release-sized guess lets the scheduler
    admit several times more work than the machine can hold.
    """
    release = CostModel(tmp_path / "r.json", ncpus=16, mode="release")
    debug = CostModel(tmp_path / "d.json", ncpus=16, mode="debug")
    for nodeid in ("alternator/test_x.py::test_y", "cluster/test_z.py::test_w",
                   "boost/some_test.cc::case"):
        assert debug.cost(nodeid).mem > 2 * release.cost(nodeid).mem, nodeid
    # sanitize and coverage are debug builds by another name
    assert CostModel(tmp_path / "s.json", ncpus=16, mode="sanitize").cost(
        "cluster/test_z.py::test_w").mem == debug.cost("cluster/test_z.py::test_w").mem


def test_a_worker_that_goes_down_mid_run_has_its_gateway_closed(tmp_path, monkeypatch):
    """xdist only tears worker processes down at the end of the session.

    So a worker the pool drains, recycles or retires mid-run has its gateway closed
    as soon as it goes down.
    """
    import test.pylib.dynamic_scheduler as bs
    dsession = SimpleNamespace(shuttingdown=False)
    sched = SimpleNamespace(config=SimpleNamespace(pluginmanager=SimpleNamespace(getplugin=lambda name: dsession)),
                            stats={"gateways_closed": 0})
    monkeypatch.setattr(bs, "_scheduler", sched)
    closed = []
    node = SimpleNamespace(ensure_teardown=lambda: closed.append(1))
    bs.pytest_testnodedown(node=node, error=None)
    assert closed == [1] and sched.stats["gateways_closed"] == 1
    dsession.shuttingdown = True                       # the end of the run: xdist does it itself
    bs.pytest_testnodedown(node=node, error=None)
    assert closed == [1]


def test_gather_metrics_records_what_admission_saw(tmp_path):
    """--gather-metrics samples the scheduler into the run's metrics database."""
    SQLiteWriter(tmp_path / DEFAULT_DB_NAME).write_row(HostInfo(HOST_ID, "cpu", 4, 20 * GB), HOST_INFO_TABLE)
    clock = {"t": 0.0}
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    model = make_model(tmp_path, 4, {profile_key(n): (1.0, 1 * GB, 10.0) for n in col})
    sched = new_sched(FakeConfig(tmp_path, 2, **{"--gather-metrics": True}), model=model, ncpus=4,
                      mem_total=20 * GB, cgroup_tests=NO_CGROUP, available_fn=lambda: 20 * GB,
                      now=lambda: clock["t"])
    nodes = [FakeNode("gw0"), FakeNode("gw1")]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    sched.check_schedule()                              # within METRICS_SECONDS: not sampled again
    clock["t"] += 5.0
    sched.check_schedule()

    rows = sqlite3.connect(tmp_path / DEFAULT_DB_NAME).execute(
        "select ncpus, booked_cores, running, workers, stats from scheduler_metrics").fetchall()
    assert len(rows) == 2
    ncpus, booked, running, workers, stats = rows[-1]
    assert (ncpus, workers) == (4, 2)
    assert running == len(sched.committed_at) > 0 and booked == sum(sched.res_cpu.values())
    assert json.loads(stats)["admitted"] == sched.stats["admitted"]


def test_each_run_of_a_file_is_a_module_of_its_own():
    col = ["a.py::t1.dev.1", "a.py::t1.dev.2", "a.py::t2.dev.1"]
    sched, _ = make_sched(Path("/nonexistent"), col, {n: (0.5, 1e9, 1.0) for n in col}, nodes=1)
    assert sched._file_of(0) == sched._file_of(2) != sched._file_of(1)
    assert sched._profile_file_of(0) == sched._profile_file_of(1) == "dev|a.py"


def test_a_worker_stays_in_its_module_and_others_take_untouched_ones(tmp_path):
    """Moving between modules sets fixtures up again, so a worker keeps to
    its module while it has tests, and an idle worker takes a module no
    one is in: no module is split here."""
    col = ["a.py::a1.dev.1", "a.py::a2.dev.1", "a.py::a3.dev.1",
           "b.py::b1.dev.1", "b.py::b2.dev.1", "c.py::c1.dev.1"]
    walls = {"a.py::a1.dev.1": 50.0, "a.py::a2.dev.1": 5.0, "a.py::a3.dev.1": 4.0,
             "b.py::b1.dev.1": 40.0, "b.py::b2.dev.1": 30.0, "c.py::c1.dev.1": 1.0}
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 1e8, w) for n, w in walls.items()}, nodes=2, ncpus=8)
    files = {n.gateway.id: {sched._file_of(i) for i in n.sent} for n in nodes}
    assert files["gw0"] != files["gw1"], "two idle workers start in two different modules"
    for node in nodes:
        for idx in list(node.sent):
            if idx in sched.committed_at:
                sched.mark_test_complete(node, idx)
    for node in nodes:
        for idx in list(node.sent):
            if idx in sched.committed_at:
                sched.mark_test_complete(node, idx)
    on = {}
    for node in nodes:
        for i in node.sent:
            on.setdefault(sched._file_of(i), set()).add(node.gateway.id)
    assert all(len(workers) == 1 for workers in on.values()), on



def test_a_modules_tests_run_in_collection_order(tmp_path):
    """Tests in a file may depend on their order, as plain xdist keeps it:
    a longer test later in the module still waits for the ones before it."""
    col = ["a.py::first.dev.1", "a.py::second.dev.1", "a.py::longest.dev.1"]
    walls = {"a.py::first.dev.1": 1.0, "a.py::second.dev.1": 2.0, "a.py::longest.dev.1": 90.0}
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 1e8, w) for n, w in walls.items()}, nodes=1, ncpus=8)
    for _ in range(len(col)):
        for idx in [i for i in nodes[0].sent if i in sched.committed_at]:
            sched.mark_test_complete(nodes[0], idx)
    assert [col[i] for i in nodes[0].sent] == col


def test_the_load_estimate_counts_each_test_for_the_part_the_measurement_missed(tmp_path):
    """The measurement averages a window: a test started in it is partly in it,
    one started after it not at all, and one that ended in it is in it although
    it no longer runs."""
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    sched, nodes = make_sched(tmp_path, col, {n: (2.0, 1e8, 60.0) for n in col}, nodes=1, ncpus=16,
                              max_workers=0)
    sched.live._window[f"path:{NO_CGROUP}"] = (10.0, 12.0)
    sched._measured_raw, sched._services_raw = 4.0, 0.0
    sched.committed_at = {0: 11.0, 1: 13.0}
    sched._started_at = {0: 11.0, 1: 13.0}            # half of its run in the window, and none of it
    sched._recent_done = [(9.5, 11.0, 2.0),           # half of the window, and over
                          (8.0, 9.0, 2.0)]            # before the window: in nothing
    assert sched._estimate_now(13.5) == pytest.approx(4.0 + 1.0 + 2.0 - 1.0)
    assert len(sched._recent_done) == 1, "what ended before the window is forgotten"


def test_a_worker_holds_one_more_test_when_its_tests_are_short(tmp_path):
    short = [f"a.py::s{i}.dev.1" for i in range(6)]
    long_ = [f"b.py::l{i}.dev.1" for i in range(6)]
    for col, wall, held in ((short, 0.2, 3), (long_, 5.0, 2)):
        sched, nodes = make_sched(tmp_path / str(wall), col, {n: (0.1, 1e8, wall) for n in col}, nodes=1, ncpus=8)
        assert len(nodes[0].sent) == held, (wall, nodes[0].sent)


def test_pressure_cuts_lower_the_limit_below_the_core_count_too(tmp_path):
    """With the target cut by pressure, a test that fits the core count but
    not the cut limit waits, even with bookings well under the core count."""
    col = ["a.py::running.dev.1", "a.py::next.dev.1"]
    sched, nodes = make_sched(tmp_path, col, {n: (2.0, 1e8, 60.0) for n in col}, nodes=1, ncpus=16)
    full = sched.cpu_target
    sched.measured_load = 7.0
    assert sched._fits(1, nodes[0], pressure=False), "7 + 2 cores fit 16"
    sched.cpu_target = full / 2                          # cut to its floor: the limit is 8
    assert not sched._fits(1, nodes[0], pressure=False), "7 + 2 cores do not fit 8"
    assert sched.stats["rejected_no_headroom"] == 1


def test_room_goes_to_a_module_already_set_up_then_to_the_longest_chain(tmp_path):
    """No test goes past the pressure cut.  When room frees up, a worker going on with
    its module gets it first (no new setup), then the held test with the longest chain
    of work behind it; a test that would start a short new module waits."""
    col = (["a.py::first.dev.1", "a.py::next.dev.1"] + [f"b.py::b{i}.dev.1" for i in range(6)]
           + ["c.py::c0.dev.1"])
    # 10-second tests: none is long enough to be the critical path and have room kept for it
    sched, nodes = make_sched(tmp_path, col, {n: (2.0, 1e8, 10.0) for n in col}, nodes=3, ncpus=16)
    for n in nodes:                                       # start from three idle workers holding nothing
        for i in sched.node2pending[n]:
            sched.committed_at.pop(i, None); sched.res_cpu.pop(i, None); sched.res_mem.pop(i, None)
            sched._add_pending(i, front=True)
        sched.node2pending[n] = []
    a, b, c = nodes
    for node, idx in ((a, 1), (b, 2), (c, 8)):            # a goes on with a.py; b and c start modules
        sched._take(sched._file_of(idx), idx)
        sched._send(node, idx)
    sched._last_file_done[a] = sched._file_of(0)
    assert sched._continues_module(a) and not sched._continues_module(b)
    sched._pressure_guard = lambda: True                   # under pressure, the target cut to its
    sched.cpu_target = sched.cpu_target_frac * 16 / 2      # floor: the limit is 8 cores
    sched._refresh_measurement = lambda: None
    sched.measured_load = 4.5                              # room for one 2-core test, then none
    sched.check_schedule()
    assert 1 in committed(sched), "a.py goes on first"
    assert not {2, 8} & committed(sched), "pressure holds the new modules back"
    sched.measured_load = 2.5                              # room for one more
    sched.check_schedule()
    assert 2 in committed(sched) and 8 not in committed(sched), "b.py's six tests before c.py's one"


def _chain_sched(tmp_path, first_wall=None, rest_wall=None):
    """One long module (8 x 100 s) worked by gw0, plus a short one; gw1 comes free.

    Two CPUs: what is left of the run needs 300 s per CPU, so no single test is
    on the critical path (that rule would hand it to any worker), but the
    module, 600 s on one worker, is."""
    col = [f"s.py::t{i}.dev.1" for i in range(8)] + ["q.py::short.dev.1"]
    costs = {n: (0.5, 1e8, 100.0) for n in col[:8]}
    costs["q.py::short.dev.1"] = (0.5, 1e8, 1.0)
    sched, nodes = make_sched(tmp_path, col, costs, nodes=1, ncpus=2)
    if first_wall is not None:
        sched.learn({"key": "dev|s.py::t0", "wall": first_wall, "usage_sec": 1.0, "memory_peak": 1e8, "first_in_file": True})
        sched.learn({"key": "dev|s.py::t1", "wall": rest_wall, "usage_sec": 1.0, "memory_peak": 1e8, "first_in_file": False})
    late = FakeNode("gw1")
    sched.add_node(late); sched.add_node_collection(late, col)
    sched.check_schedule()
    return sched, nodes[0], late, col


def test_a_long_module_with_cheap_setup_is_shared(tmp_path):
    """A chain longer than the rest of the run, whose setup is measured and small, takes a second worker."""
    sched, first, late, col = _chain_sched(tmp_path, first_wall=104.0, rest_wall=100.0)
    assert late.sent and all(col[i].startswith("s.py") for i in late.sent), [col[i] for i in late.sent]
    assert sched.stats["chain_joins_offered"] >= 1


def test_a_long_module_is_not_shared_before_its_setup_is_measured(tmp_path):
    sched, first, late, col = _chain_sched(tmp_path)
    assert [col[i] for i in late.sent][:1] == ["q.py::short.dev.1"], "it takes the untouched module instead"


def test_a_long_module_with_expensive_setup_is_not_shared(tmp_path):
    sched, first, late, col = _chain_sched(tmp_path, first_wall=150.0, rest_wall=100.0)
    assert [col[i] for i in late.sent][:1] == ["q.py::short.dev.1"]


def test_time_left_follows_the_measured_work_rate(tmp_path):
    """Once enough tests have finished, the rest of the run is timed at the rate
    predicted work actually got done, not at the allowed concurrency."""
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    clock = {"t": 1000.0}
    sched, nodes = make_sched(tmp_path, col, {n: (0.5, 1e8, 10.0) for n in col}, nodes=1, ncpus=8,
                              now=lambda: clock["t"])
    sched.total_remaining = 800.0
    assert sched._time_left() == pytest.approx(800.0 / 8), "too few finished tests: the concurrency"
    # 20 tests of 10 predicted seconds each, run 30 s each, ending 900-995: the first started at
    # 870, so 200 predicted seconds got done in 130 s
    for i in range(20):
        sched._done_log.add(1000.0 - 100 + 5 * i, 10.0, 30.0)
    assert sched._time_left() == pytest.approx(800.0 / (200.0 / 130.0))
    assert sched._run_slowdown() == pytest.approx(3.0)


def test_a_module_that_runs_slower_than_predicted_becomes_a_chain(tmp_path):
    col = [f"s.py::t{i}.dev.1" for i in range(4)]
    sched, nodes = make_sched(tmp_path, col, {n: (0.5, 1e8, 100.0) for n in col}, nodes=1, ncpus=2)
    module = sched._file_of(2)
    sched._first_walls["dev|s.py"] = [101.0]
    sched._rest_walls["dev|s.py"] = [100.0]                # setup 1 s: cheap
    remaining = sum(sched._costs_for(i).wall for i in sched.files[module])
    budget = remaining * 1.5                                 # predicted, it is not the longest chain
    assert not sched._joinable(module, 1, budget, 1.0)
    sched._file_walls["dev|s.py"] = [600.0, 200.0]           # its tests took 3x their prediction
    assert sched._joinable(module, 1, budget, 1.0)

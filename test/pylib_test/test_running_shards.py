#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for max_running_shards: the marker, and the claim it makes true."""

import ast
import asyncio
import logging
import shlex
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from test.pylib.cpp.base import DEFAULT_CUSTOM_ARGS, DEFAULT_SCYLLA_ARGS
from test.pylib.db import writer
from test.pylib.db.writer import (
    CLUSTER_METRICS_TABLE,
    SQLiteWriter,
    add_column,
    add_missing_columns,
)
from test.pylib.host_registry import Host
from test.pylib.internal_types import ServerNum
from test.pylib.marker_index import build_index
from test.pylib.running_shards import (
    MARKER,
    RunningShards,
    RunningShardsExceeded,
    claimed_shards,
)
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib.scylla_server import (
    SCYLLA_CMDLINE_OPTIONS,
    ScyllaServer,
    merge_cmdline_options,
    shards_of,
    specifies_shards,
)
from test.pylib.update_max_running_shards_markers import (
    main,
    measured_shards,
    parse_nodeid,
    update_file,
)


# --- the shard count a command line asks for --------------------------------

@pytest.mark.parametrize("options,expected", [
    (["--smp", "2"], 2),
    (["--smp=8"], 8),
    (["--overprovisioned", "--smp", "1", "--abort-on-ebadf", "1"], 1),
    # Seastar's short spelling, as the C++ test cases pass it.
    (["-c2", "-m2G"], 2),
    (["-c", "4"], 4),
    (["-c=4"], 4),
    # A long option merely starting with -c is not one.
    (["--collectd", "0", "--smp", "3"], 3),
    # The last spelling wins, whichever it is.
    (["--smp", "1", "--smp", "4"], 4),
    (["--smp", "2", "-c1"], 1),
])
def test_shards_of(options, expected):
    assert shards_of(options) == expected


def test_shards_of_default_cmdline():
    """The default every server starts from claims two shards."""
    assert shards_of(SCYLLA_CMDLINE_OPTIONS) == 2


@pytest.mark.parametrize("override", [["--smp", "3"], ["--smp=3"]])
def test_shards_of_merged_override(override):
    """A test overriding --smp changes the server's shard count, either spelling."""
    assert shards_of(merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, override)) == 3


def test_shards_of_without_smp_counts_cores(caplog):
    """No --smp means a shard per core, and says so -- no test means to ask for that."""
    import os

    assert shards_of(["--overprovisioned"]) == (os.cpu_count() or 1)
    assert "no --smp" in caplog.text


@pytest.mark.parametrize("base,override,expected", [
    (["--smp", "1"], ["--smp", "2"], ["--smp", "2"]),
    (["--smp", "1"], ["--smp"], ["--smp"]),
    (["--smp", "1"], ["--smp", "__missing__"], ["--smp"]),
    (["--smp", "1"], ["--smp", "__remove__"], []),
    (["--smp=1"], ["--smp=2"], ["--smp", "2"]),
    (["--smp=1"], ["--smp=__remove__"], []),
    (["--overprovisioned", "--smp=1", "--abort-on-ebadf"], ["--smp=2"],
     ["--overprovisioned", "--smp", "2", "--abort-on-ebadf"]),
])
def test_merge_cmdline_options_documented_cases(base, override, expected):
    """The cases merge_cmdline_options' own comment promises.

    It had no test of its own, and shards_of() now shares its option parser.
    """
    assert merge_cmdline_options(base, override) == expected


@pytest.mark.parametrize("options,expected", [
    ([], False),
    (["--overprovisioned", "-m2G"], False),
    (["--collectd", "0"], False),
    (["--smp", "2"], True),
    (["-c2"], True),
])
def test_specifies_shards(options, expected):
    assert specifies_shards(options) is expected


# --- reading the marker -----------------------------------------------------

def node(*args, **kwargs):
    """An item carrying one max_running_shards marker, or none at all."""
    mark = SimpleNamespace(args=args, kwargs=kwargs) if (args or kwargs) else None
    return SimpleNamespace(nodeid="a_test.py::test_x",
                           get_closest_marker=lambda name: mark if name == MARKER else None)


def test_claimed_shards_absent():
    assert claimed_shards(SimpleNamespace(get_closest_marker=lambda name: None)) is None


@pytest.mark.parametrize("mark", [(6,), {"amount": 6}])
def test_claimed_shards_either_spelling(mark):
    item = node(*mark) if isinstance(mark, tuple) else node(**mark)
    assert claimed_shards(item) == 6


@pytest.mark.parametrize("args,kwargs", [
    ((6,), {"amount": 6}),   # both spellings at once
    ((1, 2), {}),            # two counts
    ((), {"shards": 6}),     # unknown keyword
    (("6",), {}),            # not a number
    ((True,), {}),           # bool is an int, but not a shard count
    ((0,), {}),
    ((-1,), {}),
])
def test_claimed_shards_rejects(args, kwargs):
    with pytest.raises(ValueError, match=MARKER):
        claimed_shards(node(*args, **kwargs))


# --- the claim and the high-water mark --------------------------------------

def test_unclaimed_cluster_runs_unrestricted():
    """Which is what lets a new test run before anyone has measured it."""
    shards = RunningShards()                    # claims nothing by default
    shards.reserve(running=6, adding=100, what="a greedy server")
    assert shards.high_water_mark == 106


def test_high_water_mark_is_the_peak_not_the_last_value():
    shards = RunningShards()
    shards.reserve(running=0, adding=6, what="three servers")
    shards.reserve(running=2, adding=2, what="one more, after two stopped")
    assert shards.high_water_mark == 6


def test_claim_respected():
    shards = RunningShards()
    shards.claim = 6
    shards.reserve(running=0, adding=2, what="a server")
    shards.reserve(running=2, adding=4, what="two more servers")
    assert shards.high_water_mark == 6


def test_claim_exceeded_is_refused_before_anything_starts():
    shards = RunningShards()
    shards.claim = 4
    shards.reserve(running=0, adding=2, what="a server")

    with pytest.raises(RunningShardsExceeded, match=f"{MARKER}=4"):
        shards.reserve(running=2, adding=4, what="two more servers")

    # The mark did not move: those shards were never put on the machine.
    assert shards.high_water_mark == 2


# --- the backfill -----------------------------------------------------------

@pytest.mark.parametrize("nodeid,expected", [
    ("cluster/test_x.py::test_y", (Path("cluster/test_x.py"), None, "test_y")),
    ("cluster/test_x.py::TestC::test_y", (Path("cluster/test_x.py"), "TestC", "test_y")),
    ("cluster/test_x.py::test_y[3-True]", (Path("cluster/test_x.py"), None, "test_y")),
])
def test_parse_nodeid(nodeid, expected):
    assert parse_nodeid(nodeid) == expected


@pytest.mark.parametrize("nodeid", ["cluster/test_x.py", "a.py::A::B::test_y"])
def test_parse_nodeid_rejects(nodeid):
    with pytest.raises(ValueError):
        parse_nodeid(nodeid)


def make_db(tmp_path: Path, rows: list[tuple[str, int]], *,
            status: str = "passed", claim: int | None = None) -> Path:
    """A measurements DB holding `rows` as (nodeid, shards), one row each.

    `claim` is what was in force while they were measured; None -- the default --
    is a --measure-running-shards run, the only kind the backfill trusts.
    """
    db_path = tmp_path / "measurements.db"
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(f"CREATE TABLE {CLUSTER_METRICS_TABLE} "
                     "(id INTEGER PRIMARY KEY, test_id INT, nodeid TEXT, "
                     "max_running_shards INTEGER, status VARCHAR(15), claim INTEGER)")
        for test_id, (nodeid, shards) in enumerate(rows):
            conn.execute(f"INSERT INTO {CLUSTER_METRICS_TABLE} "
                         "(test_id, nodeid, max_running_shards, status, claim) VALUES (?, ?, ?, ?, ?)",
                         (test_id, nodeid, shards, status, claim))
    conn.close()
    return db_path


def test_measured_shards_folds_repeats_modes_and_params(tmp_path):
    """One function carries one marker, so everything folding into it takes the max."""
    db_path = make_db(tmp_path, [
        ("cluster/test_x.py::test_y[1]", 2),
        ("cluster/test_x.py::test_y[2]", 6),   # the heaviest parameter wins
        ("cluster/test_x.py::test_y[2]", 4),   # a --repeat copy of the same one
        ("cluster/test_x.py::TestC::test_y", 8),
    ])
    assert measured_shards([db_path], headroom=0) == {
        Path("cluster/test_x.py"): {None: {"test_y": 6}, "TestC": {"test_y": 8}},
    }


def test_measured_shards_ignores_tests_that_started_nothing(tmp_path):
    """A test that leased a cluster but ran no server has nothing to claim.

    A marker of 0 would be a claim it breaks the first time it does start one,
    and claimed_shards() rejects it anyway.
    """
    db_path = make_db(tmp_path, [("cluster/test_x.py::test_y", 0)])
    assert measured_shards([db_path], headroom=0) == {}


def test_measured_shards_ignores_runs_held_to_a_claim(tmp_path):
    """A claim caps the peak recorded under it, so such a row proves nothing new."""
    db_path = make_db(tmp_path, [("cluster/test_x.py::test_y", 6)], claim=6)
    assert measured_shards([db_path], headroom=0) == {}


def test_measured_shards_ignores_failed_runs(tmp_path):
    """A test that failed part-way never reached its peak, so it makes no claim."""
    db_path = make_db(tmp_path, [("cluster/test_x.py::test_y", 2)], status="failed")
    assert measured_shards([db_path], headroom=0) == {}


def test_measured_shards_does_not_let_one_run_vouch_for_another(tmp_path):
    """Two runs of one test share a tests.id, so the outcome has to be per row.

    Here the run that measured 2 failed, and the run that passed was held to a
    claim.  Neither says what the test uses, and joining them would say 2.
    """
    db_path = tmp_path / "two_runs.db"
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(f"CREATE TABLE {CLUSTER_METRICS_TABLE} "
                     "(id INTEGER PRIMARY KEY, test_id INT, nodeid TEXT, "
                     "max_running_shards INTEGER, status VARCHAR(15), claim INTEGER)")
        for shards, status, claim in [(2, "failed", None), (6, "passed", 6)]:
            conn.execute(f"INSERT INTO {CLUSTER_METRICS_TABLE} "
                         "(test_id, nodeid, max_running_shards, status, claim) VALUES (1, ?, ?, ?, ?)",
                         ("cluster/test_x.py::test_y", shards, status, claim))
    conn.close()
    assert measured_shards([db_path], headroom=0) == {}


@pytest.mark.parametrize("status", ["skipped", "xfailed"])
def test_measured_shards_ignores_runs_that_stopped_early(tmp_path, status):
    """These are recorded as successes, but the test did not run to the end.

    A mid-test skip or an xfail leaves the peak short of what the test does
    when it runs through, and a claim written from that fails the test later.
    """
    db_path = make_db(tmp_path, [("cluster/test_x.py::test_y", 2)], status=status)
    assert measured_shards([db_path], headroom=0) == {}


def test_negative_headroom_is_refused(monkeypatch, tmp_path, capsys):
    """It would write a claim below the measured peak, and at enough below it a
    zero or negative one that claimed_shards() rejects."""
    db = make_db(tmp_path, [("cluster/test_x.py::test_y", 6)])
    monkeypatch.setattr(sys, "argv", ["update_max_running_shards_markers",
                                      str(db), "--headroom-shards", "-1"])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code != 0
    assert "cannot be negative" in capsys.readouterr().err


def test_measured_shards_headroom(tmp_path):
    db_path = make_db(tmp_path, [("cluster/test_x.py::test_y", 6)])
    assert measured_shards([db_path], headroom=2)[Path("cluster/test_x.py")][None]["test_y"] == 8


SOURCE = textwrap.dedent(f'''\
    import pytest


    async def test_plain(manager):
        pass


    @pytest.mark.asyncio
    async def test_decorated(manager):
        pass


    @pytest.mark.{MARKER}(2)
    async def test_claims_too_little(manager):
        pass


    @pytest.mark.{MARKER}(100)
    async def test_claims_plenty(manager):
        pass


    class TestC:
        async def test_method(self, manager):
            pass
    ''')


def write_source(tmp_path: Path, source: str = SOURCE) -> Path:
    path = tmp_path / "test_x.py"
    path.write_text(source)
    return path


TESTS = {
    None: {
        "test_plain": 4,
        "test_decorated": 4,
        "test_claims_too_little": 6,
        "test_claims_plenty": 6,
    },
    "TestC": {"test_method": 8},
}


def test_update_file_writes_and_raises_claims(tmp_path):
    path = write_source(tmp_path)
    assert update_file(path, TESTS)
    updated = path.read_text()

    assert f"@pytest.mark.{MARKER}(4)\nasync def test_plain" in updated
    # Above the whole decorator stack, not between it and the function.
    assert f"@pytest.mark.{MARKER}(4)\n@pytest.mark.asyncio\nasync def test_decorated" in updated
    # Too low, so raised in place; already generous, so left alone.
    assert f"@pytest.mark.{MARKER}(6)\nasync def test_claims_too_little" in updated
    assert f"@pytest.mark.{MARKER}(100)\nasync def test_claims_plenty" in updated
    # Methods keep their indentation.
    assert f"    @pytest.mark.{MARKER}(8)\n    async def test_method" in updated


def test_update_file_is_idempotent(tmp_path):
    path = write_source(tmp_path)
    assert update_file(path, TESTS)
    once = path.read_text()

    assert not update_file(path, TESTS), "a second run should find nothing to change"
    assert path.read_text() == once


def test_update_file_adds_the_pytest_import(tmp_path):
    path = write_source(tmp_path, textwrap.dedent('''\
        from test.pylib.manager_client import ManagerClient


        async def test_plain(manager):
            pass
        '''))
    assert update_file(path, {None: {"test_plain": 4}})
    updated = path.read_text()
    assert "import pytest" in updated
    assert updated.index("import pytest") < updated.index(f"@pytest.mark.{MARKER}(4)")


@pytest.mark.parametrize("source", [
    "import pytest as pt\n\n\n@pt.mark.asyncio\nasync def test_plain(manager):\n    pass\n",
    "def helper():\n    import pytest\n\n\nasync def test_plain(manager):\n    pass\n",
    "async def test_plain(manager):\n    pass\n",
], ids=["aliased-import", "import-inside-a-helper", "no-imports-at-all"])
def test_update_file_adds_the_pytest_import_when_the_name_is_not_bound(tmp_path, source):
    """The marker needs `pytest` bound at module level, above it."""
    path = write_source(tmp_path, source)
    assert update_file(path, {None: {"test_plain": 4}})
    updated = path.read_text().splitlines()
    assert "import pytest" in updated, "the marker would fail with NameError without it"
    assert updated.index("import pytest") < next(i for i, line in enumerate(updated) if MARKER in line)
    ast.parse("\n".join(updated))


def test_update_file_adds_the_pytest_import_above_a_class(tmp_path):
    """An import cannot be indented into the class body the method lives in."""
    path = write_source(tmp_path, "class TestX:\n    async def test_plain(self, manager):\n        pass\n")
    assert update_file(path, {"TestX": {"test_plain": 4}})
    updated = path.read_text()
    assert updated.index("import pytest") < updated.index("class TestX")
    ast.parse(updated)


def test_update_file_leaves_a_marker_it_cannot_rewrite(tmp_path):
    """A second marker would sit above the first, and the first is the one that counts."""
    source = "import pytest as pt\n\n\n@pt.mark.max_running_shards(2)\nasync def test_plain(manager):\n    pass\n"
    path = write_source(tmp_path, source)
    assert not update_file(path, {None: {"test_plain": 6}})
    assert path.read_text() == source


@pytest.mark.parametrize("trailing", ["\n", ""], ids=["final-newline", "no-final-newline"])
def test_update_file_changes_only_the_markers(tmp_path, trailing):
    """Whatever else the file has, only the marker lines may move.

    The rewrite has to round-trip the source exactly: a file that ended without
    a newline must not gain one, and a form feed must not become a line break.
    """
    source = ("import pytest\n"
              "\n"
              "PAGE = 'a\fb'  # a form feed, which splitlines() would break on\n"
              "\n"
              "\n"
              "async def test_plain(manager):\n"
              "    pass" + trailing)
    path = write_source(tmp_path, source)
    assert update_file(path, {None: {"test_plain": 4}})

    updated = path.read_text()
    assert updated == source.replace("async def test_plain",
                                     f"@pytest.mark.{MARKER}(4)\nasync def test_plain")


def test_update_file_ignores_a_function_nested_in_a_test(tmp_path):
    """Only a test gets a marker, not a helper that happens to share its name."""
    source = textwrap.dedent("""\
        import pytest


        async def test_plain(manager):
            def test_plain():   # a helper, not a test
                pass
            test_plain()
        """)
    path = write_source(tmp_path, source)
    assert update_file(path, {None: {"test_plain": 4}})
    updated = path.read_text()
    assert updated.count(f"@pytest.mark.{MARKER}(4)") == 1
    assert updated.startswith("import pytest\n\n\n"
                              f"@pytest.mark.{MARKER}(4)\nasync def test_plain(manager):")


def test_update_file_raises_a_multi_line_claim_without_moving_the_rest(tmp_path):
    """Replacing a marker spelled across several lines shortens the file.

    Every edit position comes from the original AST, so if that replacement ran
    before an insert below it, the insert would land at a stale index -- the
    marker for the second test ended up after it, as invalid Python.
    """
    source = textwrap.dedent(f"""\
        import pytest


        @pytest.mark.{MARKER}(
            2
        )
        async def test_first(manager):
            pass


        async def test_second(manager):
            pass
        """)
    path = write_source(tmp_path, source)
    assert update_file(path, {None: {"test_first": 6, "test_second": 4}})

    updated = path.read_text()
    assert f"@pytest.mark.{MARKER}(6)\nasync def test_first" in updated
    assert f"@pytest.mark.{MARKER}(4)\nasync def test_second" in updated
    ast.parse(updated)   # and it is still Python


def test_update_file_leaves_a_computed_claim_alone(tmp_path):
    """A claim this script cannot read is a human's to change."""
    source = textwrap.dedent(f'''\
        import pytest

        SHARDS = 2


        @pytest.mark.{MARKER}(SHARDS)
        async def test_plain(manager):
            pass
        ''')
    path = write_source(tmp_path, source)
    assert not update_file(path, {None: {"test_plain": 6}})
    assert path.read_text() == source


# --- the cluster's side of the claim ----------------------------------------
#
# No Scylla process is involved: a refused reservation happens before anything
# is started, which is the property these check.

def make_cluster(tmp_path: Path) -> ScyllaCluster:
    return ScyllaCluster(
        logger=logging.getLogger(__name__),
        vardir=tmp_path,
        mode="dev",
        cmdline_options=[],
        cmdline_options_override=[],
        config_options={},
        append_env={},
        scylla_exe="/nonexistent/scylla",
    )


def test_running_shards_counts_servers_on_their_way_up(tmp_path):
    """A concurrent servers_add() must see the shards its siblings are taking."""
    cluster = make_cluster(tmp_path)
    cluster.running[ServerNum(1)] = SimpleNamespace(shards=2)
    cluster.running[ServerNum(2)] = SimpleNamespace(shards=1)
    cluster.starting[ServerNum(3)] = SimpleNamespace(shards=8)
    assert cluster.running_shards == 11


async def test_add_server_refuses_to_break_the_claim(tmp_path, monkeypatch):
    """Refused before a ScyllaServer exists, so there is nothing to unwind."""
    cluster = make_cluster(tmp_path)
    cluster.shard_usage.claim = 4

    leased = set[str]()

    async def lease_host() -> str:
        leased.add("127.0.0.99")
        return "127.0.0.99"

    async def release_host(host: Host) -> None:
        leased.remove(host)

    monkeypatch.setattr(cluster.host_registry, "lease_host", lease_host)
    monkeypatch.setattr(cluster.host_registry, "release_host", release_host)

    with pytest.raises(RunningShardsExceeded, match="adding a server"):
        await cluster.add_server(cmdline=["--smp", "8"])

    assert not leased and not cluster.leased_ips, "the address leased for the server leaked"
    assert not cluster.starting and not cluster.stopped and not cluster.running


async def test_add_server_that_will_not_start_takes_no_shards(tmp_path, monkeypatch):
    """start=False only installs the server and parks it in self.stopped.

    It runs nothing, so it must not be counted against the claim -- a test
    already at its claim is still allowed to park one.  server_start() reserves
    for it if the test ever starts it.
    """
    cluster = make_cluster(tmp_path)
    cluster.shard_usage.claim = 2
    # Already at the claim.  ip_addr because _seeds() reads it off running servers.
    cluster.running[ServerNum(1)] = SimpleNamespace(shards=2, ip_addr="127.0.0.98")

    async def lease_host() -> str:
        return "127.0.0.99"

    monkeypatch.setattr(cluster.host_registry, "lease_host", lease_host)
    monkeypatch.setattr(ScyllaServer, "install", lambda self: asyncio.sleep(0))

    before = cluster.shard_usage.high_water_mark
    await cluster.add_server(start=False, cmdline=["--smp", "8"])

    assert len(cluster.stopped) == 1, "the server should be parked, not refused"
    assert cluster.shard_usage.high_water_mark == before, "a parked server runs nothing"


async def test_server_start_refuses_to_break_the_claim(tmp_path):
    """The server stays stopped, rather than being lost between the two dicts."""
    cluster = make_cluster(tmp_path)
    cluster.shard_usage.claim = 4
    cluster.stopped[ServerNum(1)] = SimpleNamespace(shards=8, server_id=ServerNum(1))

    with pytest.raises(RunningShardsExceeded, match="starting server 1"):
        await cluster.server_start(ServerNum(1))

    assert ServerNum(1) in cluster.stopped and not cluster.running


async def park_server(cluster: ScyllaCluster, monkeypatch, cmdline: list[str]) -> ServerNum:
    """Install a real ScyllaServer into `cluster` and leave it stopped.

    A real one, because ScyllaServer.shards is what these check.  Nothing is
    started: the process, the config file and the address are all stubbed out.
    """
    async def lease_host() -> str:
        return "127.0.0.99"

    monkeypatch.setattr(cluster.host_registry, "lease_host", lease_host)
    monkeypatch.setattr(ScyllaServer, "install", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(ScyllaServer, "_write_config_file", lambda self: None)
    monkeypatch.setattr(ScyllaServer, "start", lambda self, **kwargs: asyncio.sleep(0))
    await cluster.add_server(start=False, cmdline=cmdline)
    return next(iter(cluster.stopped))


async def test_server_start_counts_a_cmdline_override(tmp_path, monkeypatch):
    """cmdline_options_override replaces the whole command line, --smp included."""
    cluster = make_cluster(tmp_path)
    server_id = await park_server(cluster, monkeypatch, ["--smp", "2"])
    cluster.shard_usage.claim = 4

    with pytest.raises(RunningShardsExceeded, match=f"starting server {server_id}"):
        await cluster.server_start(server_id, cmdline_options_override=["--smp", "8"])


async def test_an_override_keeps_counting_while_the_server_runs(tmp_path, monkeypatch):
    """Reserving for the start is not enough.

    Every later reservation counts the running servers, and this one no longer
    runs the command line it was installed with.
    """
    cluster = make_cluster(tmp_path)
    server_id = await park_server(cluster, monkeypatch, ["--smp", "2"])
    cluster.shard_usage.claim = 8

    await cluster.server_start(server_id, cmdline_options_override=["--smp", "6"])
    assert cluster.running_shards == 6, "the server is the size the override made it"

    with pytest.raises(RunningShardsExceeded, match="adding a server"):
        await cluster.add_server(cmdline=["--smp", "4"])       # 6 + 4 is over 8


async def test_a_restart_without_an_override_counts_the_installed_options(tmp_path, monkeypatch):
    """The override applied to one start, so it must not outlive it."""
    cluster = make_cluster(tmp_path)
    server_id = await park_server(cluster, monkeypatch, ["--smp", "2"])
    cluster.shard_usage.claim = 8

    await cluster.server_start(server_id, cmdline_options_override=["--smp", "6"])
    cluster.stopped[server_id] = cluster.running.pop(server_id)   # as server_stop() does
    await cluster.server_start(server_id)

    assert cluster.running_shards == 2, "back to the command line it was installed with"


def test_a_stopped_server_stops_counting(tmp_path):
    """Nothing decrements: the count is derived, so a stop just is not in it."""
    cluster = make_cluster(tmp_path)
    server = SimpleNamespace(shards=2, server_id=ServerNum(1))
    cluster.running[ServerNum(1)] = server
    assert cluster.running_shards == 2

    del cluster.running[ServerNum(1)]           # as server_stop() does
    cluster.stopped[ServerNum(1)] = server
    assert cluster.running_shards == 0


def test_a_start_stop_cycle_does_not_accumulate(tmp_path):
    """The claim is the peak at once, not the total over the test's life.

    A test that starts a node, stops it, and does that a hundred times peaks at
    one node -- so it claims one node's worth, not a hundred.  test_commitlog.py
    is the real thing: one server, two stop/start cycles, measured claim 2.
    """
    cluster = make_cluster(tmp_path)
    cluster.shard_usage.claim = 2               # one server's worth, and no more
    server = SimpleNamespace(shards=2, server_id=ServerNum(1))

    for _ in range(100):
        cluster._reserve_shards(server.shards, "starting server 1")   # raises if it accumulated
        cluster.running[ServerNum(1)] = server
        del cluster.running[ServerNum(1)]       # the test stops it again

    assert cluster.shard_usage.high_water_mark == 2

# --- the marker index -------------------------------------------------------

def test_build_index_ignores_the_callers_pytest_options(tmp_path, monkeypatch):
    """A -k in PYTEST_ADDOPTS would silently narrow what the index covers."""
    captured: dict[str, str] = {}

    def fake_run(argv, **kwargs):
        captured.update(kwargs["env"])
        (tmp_path / "marker_index.json").write_text('{"tests": []}')
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setenv("PYTEST_ADDOPTS", "-k nothing_matches")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    monkeypatch.setattr(subprocess, "run", fake_run)

    assert build_index(["test/cluster"], tmpdir=tmp_path) == []
    assert "PYTEST_ADDOPTS" not in captured
    assert "PYTEST_XDIST_WORKER" not in captured


# --- the metrics database outlives a run ------------------------------------

def test_writer_adds_a_column_missing_from_an_older_database(tmp_path):
    """The database is not wiped between runs, so an older one has to be updated.

    prepare_dirs() clears *.log from the tmpdir but not sqlite_*.db, and a host
    with SCYLLA_TEST_HOST_ID set keeps the same filename -- so a file written
    before a column existed has to gain it, or every insert into it fails.
    """
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(f"CREATE TABLE {CLUSTER_METRICS_TABLE} ("
                     "id INTEGER PRIMARY KEY, test_id INT NOT NULL, host_id VARCHAR(5) NOT NULL, "
                     "nodeid TEXT NOT NULL, max_running_shards INTEGER NOT NULL)")
    conn.close()

    SQLiteWriter(db_path).close()

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({CLUSTER_METRICS_TABLE})")}
    conn.close()
    assert {"claim", "status"} <= columns, "every column add_column names has to be applied"

    SQLiteWriter(db_path).close()   # and opening it again is a no-op


class LostTheRace:
    """A cursor whose first look at the schema is one migration out of date.

    What a worker sees when another adds a column between its check and its
    ALTER: the check says missing, the ALTER says duplicate, and looking again
    says it is there.  Workers share one database file and build a writer per
    test, so nothing stops them arriving together.
    """

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.cursor = cursor
        self.stale = True
        self.last = ""

    def execute(self, sql: str, *args):
        self.last = sql
        return self.cursor.execute(sql, *args)

    def fetchall(self):
        rows = self.cursor.fetchall()
        if not self.stale or not self.last.startswith("PRAGMA table_info"):
            return rows
        self.stale = False
        added = {column for _, column, _ in add_column}
        return [row for row in rows if row[1] not in added]


def test_writer_survives_losing_the_migration_race(tmp_path):
    """The column being there already is the outcome the migration wanted."""
    db_path = tmp_path / "current.db"
    SQLiteWriter(db_path).close()               # a database that is already migrated

    conn = sqlite3.connect(db_path)
    with conn:
        add_missing_columns(LostTheRace(conn.cursor()))
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({CLUSTER_METRICS_TABLE})")}
    conn.close()
    assert {column for _, column, _ in add_column} <= columns


def test_writer_reports_a_migration_that_really_failed(tmp_path, monkeypatch):
    """A refused ALTER is only forgivable when the column ended up there anyway."""
    db_path = tmp_path / "current.db"
    SQLiteWriter(db_path).close()
    # SQLite refuses this one outright: ALTER TABLE cannot add an index.
    monkeypatch.setattr(writer, "add_column", [(CLUSTER_METRICS_TABLE, "late", "INTEGER UNIQUE")])

    conn = sqlite3.connect(db_path)
    with pytest.raises(sqlite3.OperationalError):
        add_missing_columns(conn.cursor())
    conn.close()


def test_shards_of_cpp_defaults():
    """The command line every C++ test case gets by default claims two shards."""
    assert shards_of([*DEFAULT_SCYLLA_ARGS, *shlex.split(DEFAULT_CUSTOM_ARGS[0])]) == 2

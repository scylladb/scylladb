#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for max_running_shards: the marker, and the claim it makes true."""

import asyncio
import logging
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from test.pylib.db import writer
from test.pylib.db.writer import (
    CLUSTER_METRICS_TABLE,
    SQLiteWriter,
    add_column,
    add_missing_columns,
)
from test.pylib.host_registry import Host
from test.pylib.internal_types import ServerNum
from test.pylib.running_shards import (
    MARKER,
    RunningShards,
    RunningShardsExceeded,
    claimed_shards,
)
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib.scylla_server import ScyllaServer
from test.pylib.scylla_server import (
    SCYLLA_CMDLINE_OPTIONS,
    merge_cmdline_options,
    shards_of,
    specifies_shards,
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

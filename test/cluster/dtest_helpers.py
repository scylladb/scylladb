#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Helpers for tests ported from scylla-dtest: the dtests' node configuration,
their keyspace, table and data helpers (create_ks, create_cf, insert_c1c2,
query_c1c2), and nodetool run as a subprocess, as ccm does.
"""
import asyncio
import logging
import re
import subprocess
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from test.cluster.util import strip_asan_warnings
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The node configuration of the dtests that matters for these tests: vnodes
# keyspaces with 256 tokens per node, auto_snapshot, and a failure detector
# quick to notice stopped nodes.
DTEST_CONFIG = {
    "tablets_mode_for_new_keyspaces": "disabled",
    "num_tokens": 256,
    "auto_snapshot": True,
    "phi_convict_threshold": 5,
}

# The dtests' default_config_options() with batch_commitlog=True: no hinted
# handoff, sstable key validation, and a batch commitlog.
NO_HINTS_BATCH_COMMITLOG = {"hinted_handoff_enabled": False, "enable_sstable_key_validation": True,
                            "commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 2}


def dtest_session(cql):
    """Makes the session's statements default to CL=ONE, like the sessions of
    the dtests (test.py sessions default to LOCAL_QUORUM)."""
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


def cql_one(manager: ScyllaClusterManager):
    """The manager's driver session, with statements defaulting to CL=ONE."""
    return dtest_session(manager.get_cql())


async def exclusive_one(manager: ScyllaClusterManager, server: ServerInfo):
    """A session to the server alone, like the dtests'
    patient_exclusive_cql_connection(), with statements defaulting to CL=ONE."""
    return dtest_session(await manager.get_cql_exclusive(server))


def topology(racks: dict[str, int], dc: str = "datacenter1") -> list[dict[str, str]]:
    """Property files of nodes in the given racks of a DC, like the dtests'
    cluster topologies ({"rack1": 2, "rack2": 1} is two nodes in rack1, then
    one in rack2)."""
    return [{"dc": dc, "rack": rack} for rack, n in racks.items() for _ in range(n)]


def create_ks(cql, name: str, rf: int | dict[str, int]) -> None:
    """The dtests' create_ks(), with NetworkTopologyStrategy."""
    if isinstance(rf, int):
        replication = f"'class':'NetworkTopologyStrategy', 'replication_factor':{rf}"
    else:
        replication = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':{n}" for dc, n in rf.items())
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{{replication}}}")


def create_cf(cql, name: str, columns: dict[str, str], read_repair: float | None = None,
              tombstone_gc: str | None = None) -> None:
    """The dtests' create_cf() of a table with a text key and the given
    columns; it checks that the table exists by reading it."""
    cols = "".join(f", {k} {v}" for k, v in columns.items())
    query = (f"CREATE COLUMNFAMILY {name} (key varchar PRIMARY KEY{cols}) WITH comment='test cf' "
             "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    if read_repair is not None:
        query += f" AND read_repair_chance={read_repair:f}"
    if tombstone_gc is not None:
        query += f" AND tombstone_gc = {{'mode': '{tombstone_gc}'}}"
    cql.execute(query)
    cql.execute(SimpleStatement(f"SELECT * FROM {name} LIMIT 1", consistency_level=ConsistencyLevel.ONE))


C1C2 = {"c1": "text", "c2": "text"}


def insert_c1c2(cql, keys, consistency=ConsistencyLevel.QUORUM, ks="ks", cf="cf", c1_values=None, c2_values=None,
                concurrency=20) -> None:
    """The dtests' insert_c1c2(): rows k<key> with c1, c2 = value1, value2,
    or the given values."""
    if c1_values is not None:
        stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
        args = [[f"k{k}", c1, c2] for k, c1, c2 in zip(keys, c1_values, c2_values)]
    else:
        stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        args = [[f"k{k}"] for k in keys]
    stmt.consistency_level = consistency
    execute_concurrent_with_args(cql, stmt, args, concurrency=concurrency, raise_on_first_error=True)


def query_c1c2(cql, key, consistency=ConsistencyLevel.QUORUM, ks="ks", cf="cf", must_be_missing=False) -> None:
    """The dtests' query_c1c2()."""
    rows = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM {ks}.{cf} WHERE key='k{key}'", consistency_level=consistency)))
    if must_be_missing:
        assert rows == []
    else:
        assert len(rows) == 1 and tuple(rows[0]) == ("value1", "value2"), rows


async def nodetool(manager: ScyllaClusterManager, server: ServerInfo, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Runs `scylla nodetool <args>` against the server, as ccm's node.nodetool() does."""
    exe = await manager.server_get_exe(server.server_id)
    cmd = [exe, "nodetool", "-h", server.ip_addr, *args]
    res = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True)
    res.stderr = strip_asan_warnings(res.stderr)
    logger.debug("%s: rc=%s stdout=%s stderr=%s", " ".join(cmd), res.returncode, res.stdout, res.stderr)
    if check and res.returncode != 0:
        raise RuntimeError(f"nodetool {' '.join(args)} failed: {res.stderr}")
    return res


STATUS_LINE = re.compile(r"^(?P<status>[UDNLJM]{2})\s+(?P<address>[\d.]+)\s+(?P<load>\?|\S+\s+\S+)\s+(?P<tokens>\S+)\s+"
                         r"(?P<owns>\S+)\s+(?P<host_id>\S+)\s+(?P<rack>\S+)\s*$", re.MULTILINE)


async def nodetool_status(manager: ScyllaClusterManager, server: ServerInfo) -> list[dict[str, str]]:
    """The nodes in `nodetool status` of the server, like the dtests' nodetool_status()."""
    out = (await nodetool(manager, server, "status")).stdout
    return [m.groupdict() for m in STATUS_LINE.finditer(out)]


async def wait_for_nodes_status(manager: ScyllaClusterManager, server: ServerInfo, statuses: list[str],
                                timeout: float = 120) -> None:
    """Waits until `nodetool status` of the server lists nodes with exactly
    the given statuses (in any order), like the dtests' wait_for_nodes_status()."""
    deadline = time.time() + timeout
    while True:
        nodes = await nodetool_status(manager, server)
        if sorted(n["status"] for n in nodes) == sorted(statuses):
            return
        assert time.time() < deadline, f"nodetool status: {nodes}, expected {statuses}"
        await asyncio.sleep(1)


async def flush_all(manager: ScyllaClusterManager, servers: list[ServerInfo]) -> None:
    """`nodetool flush` on each server, like the dtests' cluster.flush()."""
    for s in servers:
        await nodetool(manager, s, "flush")


async def check_rows_on_node(manager: ScyllaClusterManager, server: ServerInfo, rows: int, ks="ks", cf="cf",
                             restart: bool = True) -> None:
    """The dtests' check_rows_on_node(): with every other node stopped, the
    node alone returns all the rows at CL=ONE; then the others start again."""
    others = [s for s in await manager.running_servers() if s.server_id != server.server_id]
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in others))
    cql = await exclusive_one(manager, server)
    result = cql.execute(SimpleStatement(f"SELECT * FROM {ks}.{cf} LIMIT {rows * 2}", consistency_level=ConsistencyLevel.ONE),
                         timeout=300)
    assert sum(1 for _ in result) == rows
    if restart:
        for s in others:
            await manager.server_start(s.server_id)


async def start_node_wait_cql(manager: ScyllaClusterManager, server: ServerInfo, **kwargs) -> None:
    """Start a node without (re)connecting the manager's driver, and wait
    until it listens for CQL clients, like ccm's start(wait_for_binary_proto=True).
    manager.server_start(connect_driver=False) itself returns once the node
    knows its host ID, before CQL is served."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_start(server.server_id, connect_driver=False, **kwargs)
    await log.wait_for("Starting listening for CQL clients", from_mark=mark, timeout=300)

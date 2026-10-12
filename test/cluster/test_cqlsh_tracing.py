#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Traces of queries run with cqlsh's TRACING ON: the replicas' events in the
session cqlsh prints, and the sstable reads (partition ranges or keys, and
the bulk DMA reads of Data and Index files) of tables, materialized views
and replicas.

Ported from scylla-dtest. Like the dtests, cqlsh is the client, the nodes
use vnodes (256 per node), one rack per node (SimpleSnitch and one rack for
ccm populate(n) clusters), and are stopped gracefully at
the end of each test, as the dtests' teardown did.
"""

import asyncio
import glob
import logging
import os
import random
import re
import time
import uuid

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.util import get_table_dir, run_nodetool
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}


async def cqlsh(server, cmds: str) -> str:
    """Run cqlsh commands against the node, as the dtests' node.run_cqlsh()
    with --no-color."""
    res = await asyncio.to_thread(run_cqlsh_at, server.ip_addr, 9042, cmds, ["--no-color"])
    logger.debug("cqlsh %s: %s %s", cmds, res.stdout, res.stderr)
    return res.stdout


async def add_servers(manager: ScyllaClusterManager, nodes: int, config: dict | None = None, simple_snitch: bool = False):
    """ccm's populate(generate_cluster_topology(rack_num=nodes)): one rack per
    node in datacenter1; or, with simple_snitch, ccm's populate(nodes):
    SimpleSnitch, every node in datacenter1/rack1."""
    if simple_snitch:
        return await manager.servers_add(nodes, config={**CONFIG, "endpoint_snitch": "SimpleSnitch", **(config or {})},
                                         property_file={"dc": "datacenter1", "rack": "rack1"})
    return await manager.servers_add(nodes, config={**CONFIG, **(config or {})},
                                     property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(nodes)])


############################################################################
# Ported from cql_tracing_test.py.

# With TRACING ON in cqlsh, a SELECT and an INSERT at ALL print a tracing
# session, and a SELECT at ALL on three nodes (RF=3) shows events of the
# coordinator and the row; system_traces.events gets events of all three
# nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_simple(manager: ScyllaClusterManager):
    servers = await add_servers(manager, 3)
    cql = manager.get_cql()
    await cql.run_async("DROP KEYSPACE IF EXISTS ks")
    await cql.run_async("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    await cql.run_async("CREATE TABLE ks.users (userid uuid PRIMARY KEY, firstname text, lastname text, age int);")
    node1 = servers[0]
    out = await cqlsh(node1, "TRACING ON")
    assert "Tracing is enabled" in out, "Tracing has not been enabled"
    out = await cqlsh(node1, "TRACING ON; SELECT * from ks.users")
    assert "Tracing session: " in out and "Request complete " in out, out
    out = await cqlsh(node1, "CONSISTENCY ALL; TRACING ON; INSERT INTO ks.users (userid, firstname, lastname, age) "
                             "VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
    assert "Tracing session: " in out and "Request complete " in out, out
    out = await cqlsh(node1, "CONSISTENCY ALL; TRACING ON; SELECT firstname, lastname FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
    match = re.search(r"Tracing session: ([0-9a-f-]{36})", out)
    assert match, out
    assert f" {node1.ip_addr} " in out, f"no events of the coordinator {node1.ip_addr}: {out}"
    assert "Request complete " in out and " Frodo |  Baggins" in out, out
    # Each replica writes its trace events asynchronously, and cqlsh reads
    # the events only once, so its output can miss a replica's events. Read
    # the events table until all nodes are in it.
    await wait_for_trace_event_sources(cql, uuid.UUID(match.group(1)), {s.ip_addr for s in servers})


async def wait_for_trace_event_sources(cql, session_id, expected_sources, timeout=30):
    """Waits until system_traces.events has events of the tracing session
    from each of the addresses, like the dtests' wait_for_trace_event_sources()."""
    query = SimpleStatement("SELECT source FROM system_traces.events WHERE session_id = %s", consistency_level=ConsistencyLevel.ALL)
    deadline = time.time() + timeout
    while True:
        sources = {str(row.source) for row in await cql.run_async(query, (session_id,))}
        if expected_sources <= sources:
            return
        assert time.time() < deadline, f"trace events of session {session_id} only from {sorted(sources)}, expected {sorted(expected_sources)}"
        await asyncio.sleep(0.5)


############################################################################
# Ported from cql_tracing_addition_test.py.

async def prepare_tracing_table(manager: ScyllaClusterManager, nodes: int = 1, rf: int | None = None,
                                disable_cache: bool = True, compaction: str | None = None, create_index: bool = False,
                                create_mv: bool = False, simple_snitch: bool = False):
    """The dtests' prepare_cluster(): ks.cf (key text, name text, rate int)
    created as by create_cf (with the given compaction instead of its
    default one), with RF=rf (default: nodes), the cache disabled by
    default, and optionally an index on rate or a materialized view by rate."""
    servers = await add_servers(manager, nodes, {"enable_cache": "false"} if disable_cache else None, simple_snitch)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf or nodes}}}")
    compaction = compaction or "{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}"
    await cql.run_async("CREATE COLUMNFAMILY ks.cf (key text PRIMARY KEY, name text, rate int) WITH comment='test cf' "
                        f"AND compaction = {compaction} AND compression = {{}}")
    await cql.run_async("SELECT * FROM ks.cf LIMIT 1")
    if create_index:
        await cql.run_async("CREATE INDEX ON ks.cf (rate)")
    if create_mv:
        await cql.run_async("CREATE MATERIALIZED VIEW ks.cf_by_rate AS SELECT * FROM ks.cf WHERE rate IS NOT NULL AND key IS NOT NULL PRIMARY KEY (rate, key)")
    return servers, cql


def insert_statement(cql):
    # At CL=ONE, the dtests' session default.
    stmt = cql.prepare("INSERT INTO ks.cf (key, name, rate) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE
    return stmt


async def insert_rows(cql, rows: int) -> None:
    stmt = insert_statement(cql)
    await asyncio.gather(*[cql.run_async(stmt, [str(uuid.uuid4()), f"lastname_{random.randint(1, 1000)}", random.randint(1, 100)]) for _ in range(rows)])


async def verify_tracing_info(manager, output: str, server, table: str, element: str) -> None:
    """For each sstable of the table on the node, the trace shows it read
    (a partition range or a key) and its Data or Index file read by bulk
    DMA reads, as the dtests' _verify_tracing_info() matched."""
    addr = re.escape(server.ip_addr)
    for path in glob.glob(f"{await get_table_dir(manager, server, 'ks', table)}/*-Data.db"):
        sstable = os.path.basename(path)
        assert re.search(rf"Reading {element} .* from sstable .*{sstable}.*| {addr} |", output), output
        data_or_index = re.sub("Data", "(Data|Index)", sstable)
        assert re.search(rf"{data_or_index}: scheduling bulk DMA read of size [\d]* at offset [\d]*.*| {addr} |", output), output
        assert re.search(rf"{data_or_index}: finished bulk DMA read of size [\d]* at offset [\d]*, successfully read [\d]* bytes.*| {addr} |", output), output


# On three nodes (RF=3) with the cache enabled and trace probability 1,
# each node, restarted to empty its cache, traces a full scan reading its
# own sstables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_info_for_sstables_on_each_node_from_replica_with_cache_enabled(manager: ScyllaClusterManager):
    servers, cql = await prepare_tracing_table(manager, nodes=3, disable_cache=False)
    await insert_rows(cql, 50)
    for s in servers:
        await run_nodetool(manager, s, "settraceprobability", "1.0")
    for s in servers:
        await run_nodetool(manager, s, "flush")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_start(s.server_id)
        out = await cqlsh(s, "TRACING ON; SELECT * FROM ks.cf")
        await verify_tracing_info(manager, out, s, "cf", "partition range")


# Ported from scylla-dtest cql_tracing_addition_test.py::TestTracingReadAccess::test_tracing_info_sstables_locally_on_each_node_from_replica.
# On three nodes in three racks (RF=3) with trace probability 1, a traced
# full scan coordinated by each node shows the reads of its own sstables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_info_sstables_locally_on_each_node_from_replica(manager: ScyllaClusterManager):
    servers, cql = await prepare_tracing_table(manager, nodes=3, rf=3)
    await insert_rows(cql, 50)
    await asyncio.gather(*(manager.api.set_trace_probability(s.ip_addr, 1.0) for s in servers))
    for s in servers:
        await run_nodetool(manager, s, "flush")
    for s in servers:
        out = await cqlsh(s, "TRACING ON; SELECT * FROM ks.cf")
        await verify_tracing_info(manager, out, s, "cf", "partition range")


# Ported from scylla-dtest cql_tracing_addition_test.py::TestTracingReadAccess::test_tracing_info_from_remote_sstables.
# On two nodes with RF=1, a traced full scan coordinated by node2 shows the
# reads of node1's flushed sstables.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_info_from_remote_sstables(manager: ScyllaClusterManager):
    [node1, node2], cql = await prepare_tracing_table(manager, nodes=2, rf=1, simple_snitch=True)
    await insert_rows(cql, 10)
    await run_nodetool(manager, node1, "flush")
    out = await cqlsh(node2, "TRACING ON; SELECT * FROM ks.cf")
    await verify_tracing_info(manager, out, node1, "cf", "partition range")


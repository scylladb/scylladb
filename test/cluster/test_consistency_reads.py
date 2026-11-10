#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Reads at various consistency levels while replicas disagree or are down,
# ported from scylla-dtest consistency_test.py (TestConsistency): read repair
# fixes a replica that missed writes, QUORUM stays available with one of three
# replicas down, and a LIMIT 1 query at ALL over replicas with conflicting
# updates returns the newest row. Keyspaces use vnodes, cache-hit-rate read
# balancing is off and hinted handoff is off, as in the dtests.

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "cache_hit_rate_read_balancing": False}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def racks(n):
    return [{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, n + 1)]


def connect(manager: ScyllaClusterManager, server):
    """A session whose statements default to consistency level ONE, as dtest sessions do."""
    cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


def create_ks(cql, rf):
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")


def create_c1c2_table(cql, read_repair=None):
    """The dtests' create_c1c2_table(): create_cf() with columns c1 and c2."""
    query = ("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
             "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    if read_repair is not None:
        query += f" AND read_repair_chance={read_repair:f}"
    cql.execute(query)
    cql.execute("SELECT * FROM ks.cf LIMIT 1")


def insert_c1c2(cql, n, consistency):
    stmt = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    stmt.consistency_level = consistency
    execute_concurrent_with_args(cql, stmt, [[f"k{k}"] for k in range(n)], concurrency=20)


def query_c1c2(cql, key, consistency):
    rows = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM ks.cf WHERE key='k{key}'", consistency_level=consistency)))
    assert len(rows) == 1 and tuple(rows[0]) == ("value1", "value2"), rows


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_readrepair(manager: ScyllaClusterManager):
    """With read_repair_chance=1, QUORUM reads of every key repair the replica
    that was down during the writes: it then has every key by itself.
    The dtest's run measured for this port took its branch without balanced
    tokens, on nodes of 256 vnodes (ccm kept num_tokens: 256), so the second
    node bootstraps by streaming many ranges; the port's nodes do the same."""
    servers = []
    for rack in racks(2):
        servers.append(await manager.server_add(config={**CONFIG, "hinted_handoff_enabled": False, "num_tokens": 256},
                                                property_file=rack))
    node1, node2 = servers
    cql = connect(manager, node1)
    create_ks(cql, 2)
    create_c1c2_table(cql, read_repair=1.0)

    await manager.server_stop_gracefully(node2.server_id)
    insert_c1c2(cql, 10000, ConsistencyLevel.ONE)
    await manager.server_start(node2.server_id)

    # Read everything, to cause read repair.
    for key in range(10000):
        query_c1c2(cql, key, ConsistencyLevel.QUORUM)

    await manager.server_stop_gracefully(node1.server_id)
    cql2 = connect(manager, node2)
    for key in range(10000):
        query_c1c2(cql2, key, ConsistencyLevel.ONE)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_quorum_available_during_failure(manager: ScyllaClusterManager):
    """QUORUM reads with RF=3 succeed with one of the three nodes down.
    As for test_readrepair, the dtest's measured run took its branch without
    balanced tokens, on nodes of 256 vnodes, so the nodes bootstrap by
    streaming many ranges; the port's nodes do the same."""
    servers = []
    for rack in racks(3):
        servers.append(await manager.server_add(config={**CONFIG, "num_tokens": 256}, property_file=rack))
    node1, node2 = servers[:2]
    cql = connect(manager, node2)
    create_ks(cql, 3)
    create_c1c2_table(cql)
    insert_c1c2(cql, 100, ConsistencyLevel.QUORUM)

    await manager.server_stop_gracefully(node1.server_id)
    for key in range(100):
        query_c1c2(cql, key, ConsistencyLevel.QUORUM)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_incomplete_result_test_per_partition_row_limit(manager: ScyllaClusterManager):
    """A LIMIT 1 query at ALL, after one replica deleted the first row and the
    other updated the second, returns the second row with its new value."""
    servers = await manager.servers_add(2, config={**CONFIG, "hinted_handoff_enabled": False}, property_file=racks(2))
    node1, node2 = servers
    cql1 = connect(manager, node1)
    create_ks(cql1, 2)
    cql1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")
    cql1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '1', '0')", consistency_level=ConsistencyLevel.ALL))
    cql1.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '2', '1')", consistency_level=ConsistencyLevel.ALL))

    # Update node1.
    await manager.server_stop_gracefully(node2.server_id)
    cql1.execute(SimpleStatement("delete from ks.cf1 where p = 1 and c = '1'", consistency_level=ConsistencyLevel.ONE))

    # Update node2.
    await manager.server_start(node2.server_id)
    await manager.server_stop_gracefully(node1.server_id)
    cql2 = connect(manager, node2)
    cql2.execute(SimpleStatement("insert into ks.cf1 (p, c, r) values (1, '2', '2')", consistency_level=ConsistencyLevel.ONE))

    # Query the whole cluster.
    await manager.server_start(node1.server_id)
    res = list(cql2.execute(SimpleStatement("select c from ks.cf1 limit 1", consistency_level=ConsistencyLevel.ALL)))
    assert len(res) == 1, f"Expecting 1 row, got {len(res)} ({res})"
    assert len(res[0]) == 1, f"Expecting 1 cell, got {len(res[0])} ({res[0]})"
    assert res[0][0] == "2", f"Expecting value 2, got {res[0][0]}"

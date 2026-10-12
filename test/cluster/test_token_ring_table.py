#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests of the system.token_ring virtual table on multi-node clusters,
# ported from scylla-dtest system_tables_test.py. The single-node test is in
# test/cqlpy/test_virtual_tables.py.
# The keyspaces use vnodes, as on the dtests' nodes: with tablets,
# system.token_ring lists only tables, per tablet, and these tests are about
# the vnode token ranges.

from test.cluster.util import new_test_keyspace, vnodes_keyspace
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest


# The dtests' nodes: vnodes for new keyspaces.
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
# ccm's populate(n): every node in datacenter1/rack1 of the SimpleSnitch.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}
TEST_KEYSPACE = "test_keyspace"


async def token_ring(cql, ks, host=None):
    return list(await cql.run_async(f"SELECT * FROM system.token_ring WHERE keyspace_name = '{ks}'", host=host))


async def create_test_keyspace(cql, replication_factor: int | dict) -> None:
    """The dtests' create_ks(): a NetworkTopologyStrategy keyspace, with the
    cluster's default (vnodes), then USE of it."""
    if isinstance(replication_factor, int):
        replication = f"'class':'NetworkTopologyStrategy', 'replication_factor':{replication_factor}"
    else:
        replication = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':{rf}" for dc, rf in replication_factor.items())
    await cql.run_async(f"CREATE KEYSPACE {TEST_KEYSPACE} WITH replication={{{replication}}}")
    cql.execute(f"USE {TEST_KEYSPACE}")


async def table_name_restriction(cql) -> str:
    """The dtests' table_name_restriction(): with vnodes, the rows of a
    keyspace's token ranges have table_name '<ALL>'."""
    res = await cql.run_async("SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'system' and table_name = 'token_ring' and column_name = 'table_name'")
    return "AND table_name = '<ALL>'" if list(res) else ""


async def first_start_token(cql) -> str:
    query = f"select start_token from system.token_ring where keyspace_name = '{TEST_KEYSPACE}' limit 1;"
    return (await cql.run_async(query))[0].start_token


# Ported from scylla-dtest system_tables_test.py::TestTokenRingTable::test_content_increasing_cluster.
# A token range of an RF=3 keyspace is listed with both nodes of a 2-node
# cluster, and with 3 nodes once a 3rd and then a 4th node join.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_content_increasing_cluster(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**VNODES, **SIMPLE_SNITCH})
    cql = manager.get_cql()
    replication_factor = 3
    await create_test_keyspace(cql, replication_factor)
    start_token = await first_start_token(cql)
    query = (f"select * from system.token_ring where keyspace_name='{TEST_KEYSPACE}' {await table_name_restriction(cql)} "
             f"and start_token='{start_token}';")
    assert len(await cql.run_async(query)) == len(servers), \
        f"The token range starting from {start_token} should be assigned to {len(servers)} nodes."
    for _ in [3, 4]:
        servers.append(await manager.server_add(config={**VNODES, **SIMPLE_SNITCH}))
        assert len(await cql.run_async(query)) == replication_factor, \
            f"The token range starting from {start_token} should be assigned to {replication_factor} nodes."


# Ported from scylla-dtest system_tables_test.py::TestTokenRingTable::test_content_multi_dc_cluster.
# With replication {dc1: 2, dc2: 1}, a token range is listed with 3
# endpoints, from both DCs.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_content_multi_dc_cluster(manager: ScyllaClusterManager):
    await manager.servers_add(4, config=VNODES,
                              property_file=[{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"},
                                             {"dc": "dc2", "rack": "rack1"}, {"dc": "dc2", "rack": "rack1"}])
    cql = manager.get_cql()
    await create_test_keyspace(cql, {"dc1": 2, "dc2": 1})
    start_token = await first_start_token(cql)
    query = (f"select dc from system.token_ring where keyspace_name = '{TEST_KEYSPACE}' {await table_name_restriction(cql)} "
             f"and start_token = '{start_token}';")
    dc_list = [row.dc for row in await cql.run_async(query)]
    assert len(dc_list) == 3, f"The token range starting from {start_token} should be assigned to 3 nodes."
    assert "dc1" in dc_list and "dc2" in dc_list, \
        f"The token range starting from {start_token} should be assigned to nodes from 'dc1' and 'dc2'."


# Ported from scylla-dtest system_tables_test.py::TestTokenRingTable::test_content_match_on_all_nodes.
# On 3 nodes (rack1, and two in rack2), system.token_ring lists the same
# token ranges of an RF=2 keyspace on every node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_content_match_on_all_nodes(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=VNODES,
                                        property_file=[{"dc": "datacenter1", "rack": "rack1"},
                                                       {"dc": "datacenter1", "rack": "rack2"},
                                                       {"dc": "datacenter1", "rack": "rack2"}])
    query = f"select * from system.token_ring where keyspace_name = '{TEST_KEYSPACE}'"
    cql = await manager.get_cql_exclusive(servers[0])
    await create_test_keyspace(cql, 2)
    test_ks_token_set = list(await cql.run_async(query))
    for server in servers[1:]:
        cql = await manager.get_cql_exclusive(server)
        assert test_ks_token_set == list(await cql.run_async(query)), \
            f"The set of token ranges does not match on the nodes {servers[0].ip_addr} and {server.ip_addr}!"


# A node replacing another one takes over exactly the token ranges the
# replaced node had.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_token_ring_replace_node(manager: ScyllaClusterManager):
    # The dtest's cluster: datacenter1 with node1 in rack1, node2 and node3 in
    # rack2; node3 is replaced by a node in its rack.
    servers = await manager.servers_add(3, property_file=[{"dc": "datacenter1", "rack": "rack1"},
                                                          {"dc": "datacenter1", "rack": "rack2"},
                                                          {"dc": "datacenter1", "rack": "rack2"}])
    cql = manager.get_cql()
    async with new_test_keyspace(manager, vnodes_keyspace("'replication_factor': 2")) as ks:
        replaced = servers[2]
        replaced_ranges = sorted((row.start_token, row.end_token) for row in await token_ring(cql, ks)
                                 if row.endpoint == replaced.ip_addr)
        assert replaced_ranges
        await manager.server_stop_gracefully(replaced.server_id)
        replace_cfg = ReplaceConfig(replaced_id=replaced.server_id, reuse_ip_addr=False, use_host_id=True)
        replacing = await manager.server_add(replace_cfg=replace_cfg, property_file={"dc": "datacenter1", "rack": "rack2"})
        cql, hosts = await manager.get_ready_cql([servers[0], servers[1], replacing])
        for host in hosts:
            ring = await token_ring(cql, ks, host)
            assert sorted((row.start_token, row.end_token) for row in ring if row.endpoint == replacing.ip_addr) == replaced_ranges
            assert not [row for row in ring if row.endpoint == replaced.ip_addr]

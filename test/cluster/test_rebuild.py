#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Ported from scylla-dtest rebuild_test.py: "nodetool rebuild" of a node
# from another DC or its own, and with RF=1 data lost by a replace.

import asyncio
import logging

import pytest
from cassandra import ConsistencyLevel

from test import DEBUG_MODES
from test.cluster.node_helpers import create_cf, create_ks, exclusive_one, insert_c1c2, nodetool, query_c1c2, start_node_wait_cql
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The dtests' nodes: vnodes keyspaces with 256 tokens per node.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}


async def add_node(manager: ScyllaClusterManager, dc: str = "dc1"):
    """TestRebuild.add_node(): a node of the GossipingPropertyFileSnitch in
    the given DC; ccm's rack None is RAC1."""
    return await manager.server_add(config=CONFIG, property_file={"dc": dc, "rack": "RAC1"})


def use_ks(cql, name: str, rf) -> None:
    """The dtests' create_ks(): the keyspace, schema agreement and USE."""
    create_ks(cql, name, rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


def check_data(session, keyspaces, tables, keys, cl=ConsistencyLevel.ALL) -> None:
    """TestRebuild._check_data()."""
    total = 0
    errors = 0
    for ks in keyspaces:
        for cf in tables:
            for i in keys:
                total += 1
                try:
                    query_c1c2(session, i, ks=ks, cf=cf, consistency=cl)
                except AssertionError:
                    errors += 1
    assert errors == 0, f"Found {errors} errors out of {total} keys"


def replication_options(session, ks: str) -> dict:
    """The dtests' get_replication_options()."""
    row = session.execute(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'").one()
    return dict(row.replication)


async def populate_dc1(manager: ScyllaClusterManager, keys: int, vnodes_keyspace: bool = False):
    """node1 in dc1 with `keys` keys in ks.cf ({"dc1": 1}), checked at ALL.
    With vnodes_keyspace, the keyspace is created with tablets disabled
    explicitly, as the dtests' create_ks(..., tablets=0) does."""
    node1 = await add_node(manager, "dc1")
    session = await exclusive_one(manager, node1)
    if vnodes_keyspace:
        session.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'dc1':1} and tablets={'enabled':false}")
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        session.execute("USE ks")
    else:
        use_ks(session, "ks", {"dc1": 1})
    create_cf(session, "cf", {"c1": "text", "c2": "text"})
    insert_c1c2(session, keys=range(keys), consistency=ConsistencyLevel.ALL)
    for i in range(keys):
        query_c1c2(session, i, ConsistencyLevel.ALL)
    session.cluster.shutdown()
    return node1


# Ported from scylla-dtest rebuild_test.py::TestRebuild::test_simple_rebuild.
# A node of a new DC2, once the keyspace is altered to replicate to DC2,
# is rebuilt from DC1 and the data reads at CL=ALL through it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_simple_rebuild(manager: ScyllaClusterManager):
    keys = 25
    await populate_dc1(manager, keys)
    node2 = await add_node(manager, "dc2")
    session = await exclusive_one(manager, node2)
    repl = replication_options(session, "ks")
    session.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class':'NetworkTopologyStrategy', 'dc1':{repl['dc1']}, 'dc2':1 }};", timeout=300)
    session.execute("USE ks")
    # The dtest counts "rebuild is in progress" and other failures without checking them.
    res = await nodetool(manager, node2, "rebuild", "dc1", check=False)
    if res.returncode != 0:
        logger.info(f"rebuild failed: {res.stderr}")
    for i in range(keys):
        query_c1c2(session, i, ConsistencyLevel.ALL)


async def prepare_everywhere_cluster(manager: ScyllaClusterManager, keys: int):
    """TestRebuild._prepare_everywhere_cluster(): node1 in dc1 with the
    keys, node2 in dc2, and ks switched to EverywhereStrategy."""
    # EverywhereStrategy needs vnodes.
    node1 = await populate_dc1(manager, keys, vnodes_keyspace=True)
    node2 = await add_node(manager, "dc2")
    session = await exclusive_one(manager, node2)
    session.execute("USE ks")
    session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'EverywhereStrategy'};")
    session.cluster.shutdown()
    return node1, node2


# Ported from scylla-dtest rebuild_test.py::TestRebuild::test_rebuild_everywhere.
# An EverywhereStrategy keyspace is rebuilt on the DC2 node from DC1; then
# each node, started alone, has all the data.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rebuild_everywhere(manager: ScyllaClusterManager):
    keys = 1000
    _, node2 = await prepare_everywhere_cluster(manager, keys)
    await nodetool(manager, node2, "rebuild", "dc1")
    nodes = await manager.running_servers()
    await asyncio.gather(*(manager.server_stop_gracefully(n.server_id) for n in nodes))
    for n in nodes:
        await start_node_wait_cql(manager, n)
        session = await exclusive_one(manager, n)
        session.execute("USE ks")
        check_data(session, ["ks"], ["cf"], range(keys), cl=ConsistencyLevel.ONE)
        session.cluster.shutdown()
        await manager.server_stop_gracefully(n.server_id)


# Ported from scylla-dtest rebuild_test.py::TestRebuild::test_rebuild_many_tables.
# A second dc1 node, after RF 2, is rebuilt with 100 tables of 100 keys;
# with node1 killed it serves all of them at CL=ONE.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rebuild_many_tables(manager: ScyllaClusterManager):
    num_keys = 100
    num_tables = 100
    node1 = await add_node(manager)
    session = await exclusive_one(manager, node1)
    ks, dc = "ks", "dc1"
    use_ks(session, ks, {dc: 1})
    tables = [f"cf_{i:04d}" for i in range(num_tables)]
    for cf in tables:
        create_cf(session, cf, {"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(num_keys), ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)
    keys = list(range(num_keys))
    check_data(session, [ks], tables, keys)
    session.cluster.shutdown()

    node2 = await add_node(manager)
    session = await exclusive_one(manager, node2)
    session.execute(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{dc}':2 }};")
    await nodetool(manager, node2, "rebuild")
    await manager.server_stop(node1.server_id, convict=False)
    check_data(session, [ks], tables, keys, cl=ConsistencyLevel.ONE)


# Ported from scylla-dtest rebuild_test.py::TestRebuild::test_rebuild_keyspace_with_rf_1.
# Three nodes (SimpleSnitch) without repair-based node operations, 1000 keys
# with RF=1: node3 is stopped and replaced by node4 (auto_bootstrap off),
# which warns it cannot stream the RF=1 ranges; after "nodetool rebuild" on
# node4, keys of node3 are still lost.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rebuild_keyspace_with_rf_1(manager: ScyllaClusterManager):
    config = {**CONFIG, "endpoint_snitch": "SimpleSnitch", "enable_repair_based_node_ops": False}
    property_file = {"dc": "datacenter1", "rack": "rack1"}
    servers = [await manager.server_add(config=config, property_file=property_file) for _ in range(3)]
    node1, _, node3 = servers
    session = await exclusive_one(manager, node1)
    use_ks(session, "ks", 1)
    create_cf(session, "cf", {"c1": "text", "c2": "text"})
    insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)
    for i in range(1000):
        query_c1c2(session, i, ConsistencyLevel.ONE)

    await manager.server_stop_gracefully(node3.server_id)
    await manager.others_not_see_server(node3.ip_addr)
    replace_cfg = ReplaceConfig(replaced_id=node3.server_id, reuse_ip_addr=False, use_host_id=True)
    # ccm's seeds: the three populated nodes, the stopped node3 included
    # (test.py would give node4 the running nodes only).
    node4 = await manager.server_add(replace_cfg, config={**config, "auto_bootstrap": False}, property_file=property_file,
                                     seeds=[s.ip_addr for s in servers])
    log = await manager.server_open_log(node4.server_id)
    await log.wait_for(r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation",
                       timeout=60)
    await nodetool(manager, node4, "rebuild")
    with pytest.raises(AssertionError, match=r"Found .* errors out of 1000 keys"):
        check_data(session, keyspaces=["ks"], tables=["cf"], keys=list(range(1000)))

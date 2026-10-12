#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.cluster.dtest_helpers import start_node_wait_cql
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest


# Ported from scylla-dtest everywhere_strategy_test.py::TestEverywhereConsistency::test_bootstrap_second_dc.
# Bootstrapping the first node of a new DC streams it all the data of an
# EverywhereStrategy keyspace: with the old DC's node down, the new node
# alone returns every row.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_bootstrap_second_dc(manager: ScyllaClusterManager):
    node1 = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'EverywhereStrategy'} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        insert = cql.prepare(f"INSERT INTO {ks}.t (pk) VALUES (?)")
        insert.consistency_level = ConsistencyLevel.ALL
        for k in range(100):
            await cql.run_async(insert, [k])

        node2 = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"})
        await manager.server_stop_gracefully(node1.server_id)
        cql2 = await manager.get_cql_exclusive(node2)
        try:
            # Prepared, as in the dtest.
            select = cql2.prepare(f"select pk from {ks}.t")
            select.consistency_level = ConsistencyLevel.LOCAL_ONE
            rows = await cql2.run_async(select)
            assert {r.pk for r in rows} == set(range(100))
        finally:
            cql2.cluster.shutdown()
        await manager.server_start(node1.server_id)
        await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 60)


EVERYWHERE_KS = "distributed_everywhere"
# The dtests' nodes: vnodes with 256 tokens.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}


async def create_and_fill_user_table(manager: ScyllaClusterManager, node) -> None:
    """TestSchemaReplicationEverywhereStrategy.create_and_fill_user_table()."""
    session = await manager.get_cql_exclusive(node)
    try:
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {EVERYWHERE_KS} WITH REPLICATION = {{'class': 'EverywhereStrategy'}} AND TABLETS = {{'enabled': false}}")
        session.execute(f"CREATE TABLE {EVERYWHERE_KS}.test1 (key varchar PRIMARY KEY, c1 varchar, c2 varchar)")
        session.execute(SimpleStatement(f"INSERT INTO {EVERYWHERE_KS}.test1 (key, c1, c2) VALUES ('key1', 'txt1', 'txt2')",
                                        consistency_level=ConsistencyLevel.ALL))
    finally:
        session.cluster.shutdown()


async def verify_data_in_user_table(manager: ScyllaClusterManager, node) -> None:
    """TestSchemaReplicationEverywhereStrategy.verify_data_in_user_table(): the
    row read at ONE through the node alone."""
    session = await manager.get_cql_exclusive(node)
    try:
        res = list(session.execute(SimpleStatement(f"Select * from {EVERYWHERE_KS}.test1;", consistency_level=ConsistencyLevel.ONE)))
    finally:
        session.cluster.shutdown()
    assert len(res) == 1, f"Wrong number of rows: {res}"
    assert res[0].c1 == "txt1" and res[0].c2 == "txt2", f"Row {res[0]} has wrong column values"


# Ported from scylla-dtest schema_everywhere_test.py::TestSchemaReplicationEverywhereStrategy::test_replicating_user_table_to_new_node.
# On two nodes (populate(2): SimpleSnitch, datacenter1/rack1), a row of an
# EverywhereStrategy keyspace reaches a bootstrapped node3, which alone reads
# it, and a node4 replacing node1, which alone reads it too.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_replicating_user_table_to_new_node(manager: ScyllaClusterManager):
    config = {**CONFIG, "endpoint_snitch": "SimpleSnitch"}
    property_file = {"dc": "datacenter1", "rack": "rack1"}
    node1, node2 = [await manager.server_add(config=config, property_file=property_file) for _ in range(2)]
    await create_and_fill_user_table(manager, node1)
    node3 = await manager.server_add(config=config, property_file=property_file)
    for node in (node1, node2):
        await manager.server_stop_gracefully(node.server_id)
        await manager.others_not_see_server(node.ip_addr)
    await verify_data_in_user_table(manager, node3)
    await manager.server_start(node2.server_id)
    node4 = await manager.server_add(ReplaceConfig(replaced_id=node1.server_id, reuse_ip_addr=False, use_host_id=True),
                                     config=config, property_file=property_file)
    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_stop_gracefully(node3.server_id)
    await verify_data_in_user_table(manager, node4)


# Ported from scylla-dtest schema_everywhere_test.py::TestSchemaReplicationEverywhereStrategy::test_replicating_user_table_in_mutlidc.
# On two DCs of two nodes (populate([2, 2]): dc1 and dc2, rack RAC1), the
# row of an EverywhereStrategy keyspace written at ALL is read by each node
# started alone.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_replicating_user_table_in_mutlidc(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=CONFIG, property_file={"dc": dc, "rack": "RAC1"}) for dc in ["dc1", "dc1", "dc2", "dc2"]]
    await create_and_fill_user_table(manager, servers[0])
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        await start_node_wait_cql(manager, s)
        await verify_data_in_user_table(manager, s)
        await manager.server_stop_gracefully(s.server_id)

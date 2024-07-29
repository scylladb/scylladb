#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import time
import asyncio
import logging
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for, wait_for_view
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.topology.util import trigger_snapshot, wait_until_topology_upgrade_finishes, enter_recovery_state, reconnect_driver, \
        delete_raft_topology_state, delete_raft_data_and_upgrade_state, wait_until_upgrade_finishes
from test.topology.conftest import skip_mode
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest


logger = logging.getLogger(__name__)

async def create_keyspace(cql):
    ks_name = 'ks'
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    return ks_name

async def create_table(cql):
    await cql.run_async(f"CREATE TABLE ks.t (p int, c int, PRIMARY KEY (p, c))")

async def create_mv(cql, view_name):
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.{view_name} AS SELECT * FROM ks.t WHERE c IS NOT NULL and p IS NOT NULL PRIMARY KEY (c, p)")

async def get_view_builder_version(cql, **kwargs):
    result = await cql.run_async("SELECT value FROM system.scylla_local WHERE key='view_builder_version'", **kwargs)
    if len(result) == 0:
        return 1
    else:
        return int(result[0].value)

async def view_builder_is_v2(cql, **kwargs):
    v = await get_view_builder_version(cql, **kwargs)
    return v == 2 or None

async def view_is_built_v2(cql, ks_name, view_name, node_count, **kwargs):
    done = await cql.run_async(f"SELECT COUNT(*) FROM system.view_build_status_v2 WHERE keyspace_name='{ks_name}' \
                                 AND view_name = '{view_name}' AND status = 'SUCCESS' ALLOW FILTERING", **kwargs)
    return done[0][0] == node_count or None

async def wait_for_view_v2(cql, ks_name, view_name, node_count, **kwargs):
    await wait_for(lambda: view_is_built_v2(cql, ks_name, view_name, node_count, **kwargs), time.time() + 60)

# Verify a new cluster uses the view_build_status_v2 table.
# Create a materialized view and check that the view's build status
# is stored in view_build_status_v2 and all nodes see all the other
# node's statuses.
@pytest.mark.asyncio
async def test_view_build_status_v2_table(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count)
    cql, hosts = await manager.get_ready_cql(servers)

    # The cluster should init with view_builder v2
    for h in hosts:
        v = await get_view_builder_version(cql, host=h)
        assert v == 2

    await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt")

    await asyncio.gather(*(wait_for(lambda: view_is_built_v2(cql, 'ks', 'vt', node_count, host=h), time.time() + 60) for h in hosts))

# The table system_distributed.view_build_status is set to be a virtual table reading
# from system.view_build_status_v2, so verify that reading from each of them provides
# the same output.
@pytest.mark.asyncio
async def test_view_build_status_virtual_table(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count)
    cql, hosts = await manager.get_ready_cql(servers)

    async def select_v1():
        r = await cql.run_async("SELECT * FROM system_distributed.view_build_status")
        return r

    async def select_v2():
        r = await cql.run_async("SELECT * FROM system.view_build_status_v2")
        return r

    async def assert_v1_eq_v2():
        r1, r2 = await select_v1(), await select_v2()
        assert r1 == r2

    ks_name = await create_keyspace(cql)
    await create_table(cql)

    await assert_v1_eq_v2()

    await create_mv(cql, "vt1")
    await wait_for_view(cql, "vt1", node_count)
    await assert_v1_eq_v2()
    assert len(await select_v2()) == node_count

    await create_mv(cql, "vt2")
    await wait_for_view(cql, "vt2", node_count)
    await assert_v1_eq_v2()
    assert len(await select_v2()) == node_count * 2

    # verify SELECT ... WHERE works
    r1 = await cql.run_async("SELECT * FROM system_distributed.view_build_status WHERE keyspace_name='{ks_name}' AND view_name='vt1'")
    r2 = await cql.run_async("SELECT * FROM system.view_build_status_v2 WHERE keyspace_name='{ks_name}' AND view_name='vt1'")
    assert r1 == r2

    # verify SELECT COUNT(*) works
    r1 = await cql.run_async("SELECT COUNT(*) FROM system_distributed.view_build_status")
    r2 = await cql.run_async("SELECT COUNT(*) FROM system.view_build_status_v2")
    assert r1 == r2

    await cql.run_async(f"DROP MATERIALIZED VIEW {ks_name}.vt1")

    async def view_rows_removed():
        r = await select_v2()
        return (len(r) == node_count) or None
    await wait_for(view_rows_removed, time.time() + 60)

    await assert_v1_eq_v2()
    assert len(await select_v2()) == node_count

# Cluster with 3 nodes.
# Create materialized views. Start new server and it should get a snapshot on bootstrap.
# Stop 3 `old` servers and query the new server to validate if it has the same view build status.
@pytest.mark.asyncio
async def test_view_build_status_snapshot(manager: ManagerClient):
    servers = await manager.servers_add(3)
    cql, _ = await manager.get_ready_cql(servers)

    await create_keyspace(cql)
    await create_table(cql)

    await create_mv(cql, "vt1")
    await create_mv(cql, "vt2")

    await wait_for_view(cql, "vt1", 3)
    await wait_for_view(cql, "vt2", 3)

    # we don't know who the leader is, so trigger the snapshot on all nodes
    await asyncio.gather(*(trigger_snapshot(manager, s) for s in servers))

    result_before = await cql.run_async("SELECT * FROM system.view_build_status_v2")

    # Add a new server which will recover from the snapshot
    new_server = await manager.server_add()
    all_servers = servers + [new_server]
    await wait_for_cql_and_get_hosts(cql, all_servers, time.time() + 60)
    await manager.servers_see_each_other(all_servers)

    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))

    # Read the table on the new server, verify it contains all the previous table content
    await manager.driver_connect(server=new_server)
    cql = manager.get_cql()
    result_after = await cql.run_async("SELECT * FROM system.view_build_status_v2")

    rows_before = {(r.keyspace_name, r.view_name, r.host_id): r for r in result_before}
    rows_after = {(r.keyspace_name, r.view_name, r.host_id): r for r in result_after}

    # Check that all the rows exist in the table on the new node.
    # We may have now additional rows inserted by the new node.
    for k in rows_before.keys():
        assert rows_after[k] == rows_before[k]

# Start cluster in view_builder v1 mode and migrate to v2.
# Verify the migration copies the v1 data, and the new v2 table
# is used after the migration.
@pytest.mark.asyncio
async def test_view_build_status_migration_to_v2(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True}

    servers = [await manager.server_add(config=cfg)]
    # Enable raft-based node operations for subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['force_gossip_topology_changes']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]

    logging.info("Waiting until driver connects to every server")
    cql, hosts = await manager.get_ready_cql(servers)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt1")

    # Verify we're using v1 now
    v = await get_view_builder_version(cql)
    assert v == 1

    result = await cql.run_async("SELECT * FROM system_distributed.view_build_status")
    assert len(result) == 3

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == 0

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking migrated data in system")

    await asyncio.gather(*(wait_for(lambda: view_builder_is_v2(cql, host=h), time.time() + 60) for h in hosts))

    # Check that new writes are written to the v2 table
    await create_mv(cql, "vt2")
    await wait_for_view_v2(cql, "ks", "vt2", 3)

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == 6

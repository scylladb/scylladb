#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import time
import asyncio
import logging
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for, wait_for_view, wait_for_view_v1
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.internal_types import ServerInfo
from test.topology.util import trigger_snapshot, wait_until_topology_upgrade_finishes, enter_recovery_state, reconnect_driver, \
        delete_raft_topology_state, delete_raft_data_and_upgrade_state, wait_until_upgrade_finishes, wait_for
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
        return int(result[0].value) // 10

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

    await asyncio.gather(*(wait_for_view_v2(cql, 'ks', 'vt', node_count, host=h) for h in hosts))

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

    async def wait_for_view_on_host(cql, name, node_count, host, timeout: int = 120):
        async def view_is_built():
            done = await cql.run_async(f"SELECT COUNT(*) FROM system.view_build_status_v2 WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING", host=host)
            return done[0][0] == node_count or None
        deadline = time.time() + timeout
        await wait_for(view_is_built, deadline)

    ks_name = await create_keyspace(cql)
    await create_table(cql)

    await assert_v1_eq_v2()

    await create_mv(cql, 'vt1')
    await asyncio.gather(*(wait_for_view_on_host(cql, 'vt1', node_count, h) for h in hosts))
    await assert_v1_eq_v2()
    assert len(await select_v2()) == node_count

    await create_mv(cql, 'vt2')
    await asyncio.gather(*(wait_for_view_on_host(cql, 'vt2', node_count, h) for h in hosts))
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

    # verify paging
    r1 = await cql.run_async(SimpleStatement(f"SELECT * FROM system_distributed.view_build_status", fetch_size=1))
    r2 = await cql.run_async(SimpleStatement(f"SELECT * FROM system.view_build_status_v2", fetch_size=1))
    assert r1 == r2

    # select with range
    s0_host_id = await manager.get_host_id(servers[0].server_id)
    r1 = await cql.run_async(f"SELECT * FROM system_distributed.view_build_status WHERE keyspace_name='{ks_name}' AND view_name='vt1' AND host_id >= {s0_host_id}")
    r2 = await cql.run_async(f"SELECT * FROM system.view_build_status_v2 WHERE keyspace_name='{ks_name}' AND view_name='vt1' AND host_id >= {s0_host_id}")
    assert r1 == r2

    # select with allow filtering
    r1 = await cql.run_async(f"SELECT * FROM system_distributed.view_build_status WHERE status='SUCCESS' ALLOW FILTERING")
    r2 = await cql.run_async(f"SELECT * FROM system.view_build_status_v2 WHERE status='SUCCESS' ALLOW FILTERING")
    assert r1 == r2

    await cql.run_async(f"DROP MATERIALIZED VIEW {ks_name}.vt1")

    async def view_rows_removed(host):
        r = await cql.run_async("SELECT * FROM system.view_build_status_v2", host=host)
        return (len(r) == node_count) or None
    await asyncio.gather(*(wait_for(lambda: view_rows_removed(h), time.time() + 60) for h in hosts))
    await assert_v1_eq_v2()

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

    for s in servers:
        await manager.driver_connect(server=s)
        cql = manager.get_cql()
        await wait_for_view(cql, "vt1", 3)
        await wait_for_view(cql, "vt2", 3)

    # we don't know who the leader is, so trigger the snapshot on all nodes
    await asyncio.gather(*(trigger_snapshot(manager, s) for s in servers))

    # Add a new server which will recover from the snapshot
    new_server = await manager.server_add()
    all_servers = servers + [new_server]
    await wait_for_cql_and_get_hosts(cql, all_servers, time.time() + 60)
    await manager.servers_see_each_other(all_servers)

    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))

    # Read the table on the new server, verify it contains all the previous table content
    await manager.driver_connect(server=new_server)
    cql = manager.get_cql()
    await wait_for_view(cql, "vt1", 4)
    await wait_for_view(cql, "vt2", 4)

# Start cluster in view_builder v1 mode and migrate to v2.
# Verify the migration copies the v1 data, and the new v2 table
# is used after the migration.
@pytest.mark.asyncio
async def test_view_build_status_migration_to_v2(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True, 'enable_tablets': False}

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
    await asyncio.gather(*(wait_for_view_v2(cql, "ks", "vt2", 3, host=h) for h in hosts))

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == 6

# Migrate the view_build_status table to v2 and write to the table during the migration.
# The migration process goes through an intermediate stage where it writes to
# both the old and new table, so the write should not be lost.
@pytest.mark.asyncio
async def test_view_build_status_migration_to_v2_with_write_during_migration(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True, 'enable_tablets': False}

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

    inj_insert = "view_builder_pause_add_new_view"
    await manager.api.enable_injection(servers[1].ip_addr, inj_insert, one_shot=True)

    await create_mv(cql, "vt1")

    # pause the migration between reading the old table and writing to the new table, so we have
    # a time window where new writes may be lost.
    # we don't know who the coordinator is so inject in all nodes.
    inj_upgrade = "view_builder_pause_in_migrate_v2"
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, inj_upgrade, one_shot=True)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking migrated data in system")

    # Now that the upgrade is paused, write the new view.
    await manager.api.message_injection(servers[1].ip_addr, inj_insert)
    await asyncio.sleep(1)

    # continue the migration
    for s in servers:
        await manager.api.message_injection(s.ip_addr, inj_upgrade)

    await asyncio.gather(*(wait_for(lambda: view_builder_is_v2(cql, host=h), time.time() + 60) for h in hosts))

    await asyncio.gather(*(wait_for_view_v2(cql, 'ks', 'vt1', 3, host=h) for h in hosts))

# Migrate the view_build_status table to v2 while there is an 'old' write operation in progress.
# The migration should wait for the old operations to complete before continuing, otherwise
# these writes may be lost.
@pytest.mark.asyncio
async def test_view_build_status_migration_to_v2_barrier(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True, 'enable_tablets': False}

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

    # Create MV and delay the write operation to the old table
    inj_insert = "view_builder_pause_add_new_view"
    await manager.api.enable_injection(servers[1].ip_addr, inj_insert, one_shot=True)
    await create_mv(cql, "vt1")

    # The upgrade should perform a barrier and wait for the delayed operation to complete before continuing.
    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    # the upgrade should now be waiting for the insert to complete.
    # unpause the insert
    await asyncio.sleep(1)
    await manager.api.message_injection(servers[1].ip_addr, inj_insert)

    logging.info("Checking migrated data in system")

    await asyncio.gather(*(wait_for(lambda: view_builder_is_v2(cql, host=h), time.time() + 60) for h in hosts))

    await asyncio.gather(*(wait_for_view_v2(cql, 'ks', 'vt1', 3, host=h) for h in hosts))

# Test that when removing a node from the cluster, we clean its rows from
# the view build status table.
@pytest.mark.asyncio
async def test_view_build_status_cleanup_on_remove_node(manager: ManagerClient):
    node_count = 4
    servers = await manager.servers_add(node_count)
    cql, _ = await manager.get_ready_cql(servers)

    await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt1")
    await create_mv(cql, "vt2")

    await wait_for_view(cql, "vt1", node_count)
    await wait_for_view(cql, "vt2", node_count)

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == node_count * 2

    await manager.server_stop_gracefully(servers[-1].server_id)
    await manager.remove_node(servers[0].server_id, servers[-1].server_id)

    # The 2 rows belonging to the node that was removed, one for each view, should
    # be deleted from the table.
    async def node_rows_removed():
        result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
        return (len(result) == (node_count - 1) * 2) or None

    await wait_for(node_rows_removed, time.time() + 60)

# Replace a node and verify that the view_build_status has rows for the new node and
# no rows for the old node
@pytest.mark.asyncio
async def test_view_build_status_with_replace_node(manager: ManagerClient):
    node_count = 4
    servers = await manager.servers_add(node_count)
    cql, _ = await manager.get_ready_cql(servers)

    await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt1")
    await create_mv(cql, "vt2")

    await wait_for_view(cql, "vt1", node_count)
    await wait_for_view(cql, "vt2", node_count)

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == node_count * 2

    # replace a node
    removed_host_id = await manager.get_host_id(servers[0].server_id)
    await manager.server_stop_gracefully(servers[0].server_id);
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    servers.append(await manager.server_add(replace_cfg))
    servers = servers[1:]
    added_host_id = await manager.get_host_id(servers[-1].server_id)

    # wait for the old node rows to be removed and new node rows to be added
    async def node_rows_replaced():
        result = await cql.run_async(f"SELECT * FROM system.view_build_status_v2 WHERE host_id={removed_host_id} ALLOW FILTERING")
        if len(result) != 0:
            return None

        result = await cql.run_async(f"SELECT * FROM system.view_build_status_v2 WHERE host_id={added_host_id} ALLOW FILTERING")
        if len(result) != 2:
            return None

        result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
        if len(result) != node_count * 2:
            return None

        return True

    await wait_for(node_rows_replaced, time.time() + 60)

# Start with view_build_status v1 mode, and create entries such that
# some of them correspond to removed nodes or non-existent views.
# Then migrate to v2 table and verify that only valid entries belonging to known nodes
# and views are migrated to the new table.
@pytest.mark.asyncio
async def test_view_build_status_migration_to_v2_with_cleanup(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True, 'enable_tablets': False}

    servers = [await manager.server_add(config=cfg)]
    # Enable raft-based node operations for subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['force_gossip_topology_changes']

    # We start with total 4 nodes and we will remove one of them before the migration.
    servers += [await manager.server_add(config=cfg) for _ in range(3)]

    logging.info("Waiting until driver connects to every server")
    cql, hosts = await manager.get_ready_cql(servers)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    # Create a view. This will insert 4 entries to the view build status table, one for each node.
    ks_name = await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt1")

    await wait_for_view_v1(cql, "vt1", 4)

    result = await cql.run_async("SELECT * FROM system_distributed.view_build_status")
    assert len(result) == 4

    # Insert a row that doesn't correspond to an existing view, but does correspond to a known host.
    # This row should get cleaned during migration.
    await cql.run_async(f"INSERT INTO system_distributed.view_build_status(keyspace_name, view_name, host_id, status) \
                          VALUES ('ks', 'view_doesnt_exist', {result[0].host_id}, 'SUCCESS')")

    # Remove the last node. the entry for this node in the view build status remains and it
    # corresponds now to an unknown node. The migration should remove it.
    logging.info("Removing last node")
    await manager.server_stop_gracefully(servers[-1].server_id)
    await manager.remove_node(servers[0].server_id, servers[-1].server_id)

    servers = servers[:-1]
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking migrated data in system")

    # Wait for migration and upgrade to view build status v2.
    await asyncio.gather(*(wait_for(lambda: view_builder_is_v2(cql, host=h), time.time() + 60) for h in hosts))
    await wait_for_view_v2(cql, ks_name, "vt1", 3)

    # Verify that after migration we kept only the entries for the known nodes and views.
    async def rows_migrated():
        result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
        return (len(result) == 3) or None

    await wait_for(rows_migrated, time.time() + 60)

# Reproduces scylladb/scylladb#20754
# View build status migration is doing read with CL=ALL, so it requires all nodes to be up.
# Before the fix, the migration was triggered too early, causing unavailable exception in topology coordinator.
# The error was triggered when the cluster started in raft topology without view build status v2.
# It wasn't happening in gossip topology -> raft topology upgrade.
@pytest.mark.asyncio
@skip_mode('release', 'error injection is not supported in release mode')
async def test_migration_on_existing_raft_topology(request, manager: ManagerClient):
    cfg = {
        "error_injections_at_startup": [
            {
                "name": "suppress_features",
                "value": "VIEW_BUILD_STATUS_ON_GROUP0"
            },
            "skip_vb_v2_version_mut"
        ]
    }
    servers = await manager.servers_add(3, config=cfg)

    logging.info("Waiting until driver connects to every server")
    cql, hosts = await manager.get_ready_cql(servers)

    await create_keyspace(cql)
    await create_table(cql)
    await create_mv(cql, "vt1")

    # Verify we're using v1 now
    v = await get_view_builder_version(cql)
    assert v == 1

    async def _view_build_finished():
        result = await cql.run_async("SELECT * FROM system_distributed.view_build_status")
        return len(result) == 3
    await wait_for(_view_build_finished, time.time() + 10, period=.5)

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == 0

    # Enable suppressed `VIEW_BUILD_STATUS_ON_GROUP0` cluster feature
    for srv in servers:
        await manager.server_stop_gracefully(srv.server_id)
        await manager.server_update_config(srv.server_id, "error_injections_at_startup", [])
        await manager.server_start(srv.server_id)
        await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)

    logging.info("Waiting until view builder status is migrated")
    await asyncio.gather(*(wait_for(lambda: view_builder_is_v2(cql, host=h), time.time() + 60) for h in hosts))

    # Check that new writes are written to the v2 table
    await create_mv(cql, "vt2")
    await asyncio.gather(*(wait_for_view_v2(cql, "ks", "vt2", 3, host=h) for h in hosts))

    result = await cql.run_async("SELECT * FROM system.view_build_status_v2")
    assert len(result) == 6

    # Check if there is no error logs from raft topology
    for srv in servers:
        log = await manager.server_open_log(srv.server_id)
        res = await log.grep(r'ERROR.*\[shard [0-9]: [a-z]+\] raft_topology - topology change coordinator fiber got error exceptions::unavailable_exception \(Cannot achieve consistency level for')
        assert len(res) == 0

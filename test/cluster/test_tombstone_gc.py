#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
from datetime import datetime, timezone
from functools import partial
import json
import logging
import time
import pytest

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.util import delete_discovery_state_and_group0_id, delete_raft_group_data, disable_schema_agreement_wait, new_test_keyspace, new_test_table, reconnect_driver

logger = logging.getLogger(__name__)

# No row cache, so that the reads below see what compaction left, and no flush
# cache, so that each repair records the time of its own flush as the repair time.
RF_ONE_CFG = {'enable_cache': False, 'repair_hints_batchlog_flush_cache_time_in_ms': 0}


async def get_tombstone_counts(cql, hosts, table: str) -> list[int]:
    """The number of partition tombstones of the table on each host"""
    counts = []
    for host in hosts:
        mf_query = f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE partition_region = 0 ALLOW FILTERING"
        res = list(await cql.run_async(mf_query, host=host))
        logger.info(f"Host {host} returned {res}")
        counts.append(len(res))
    return counts


async def flush_and_compact(manager, servers, keyspace: str):
    await asyncio.gather(*[manager.api.flush_all_keyspaces(srv.ip_addr) for srv in servers])
    await asyncio.gather(*[manager.api.keyspace_compaction(srv.ip_addr, keyspace) for srv in servers])


def check_tombstone_gc_mode(cql, table, mode):
    s = list(cql.execute(f"DESC {table}"))[0].create_statement
    assert f"'mode': '{mode}'" in s


@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc(manager: ScyllaClusterManager, rf: int, tablets: bool):
    _ = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(manager, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int") as table:
            check_tombstone_gc_mode(cql, table, "repair")


@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc_does_not_override(manager: ScyllaClusterManager, rf: int, tablets: bool):
    _ = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(manager, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int", " with tombstone_gc = {'mode': 'disabled'}") as table:
            await cql.run_async(f"ALTER TABLE {table} add y int")
            check_tombstone_gc_mode(cql, table, "disabled")


async def test_group0_tombstone_gc(manager: ScyllaClusterManager):
    """
    Regression test for #15607.

    Test that the tombstones are being cleaned after the group 0 catching up.

    Test #1:
        Arrange:
            - start 3 nodes
        Act:
            - create new group0 tombstones by updating the schema (create/alter/delete randome tables)
        Assert:
            - the tombstones are cleaned up eventually

    Test #2:
        Arrange:
            - stop one of the nodes
        Act:
            - create new group0 tombstones by updating the schema (create/alter/delete randome tables)
        Assert:
            - the tombstones are not cleaned up (one of the nodes is down, so they can't catch up)

    Test #3:
        Arrange:
            - start the node again
        Act:
            - tombstones exist from the previous test
        Assert:
            - the tombstones are cleaned up eventually

    Test #4:
        Regression test for https://github.com/scylladb/scylladb/issues/26534

        Arrange:
            - kill one of the nodes
        Act:
            - create new group0 tombstones by updating the schema (create/alter/delete random tables),
            - perform the Raft-based recovery procedure, which creates a new group0
        Assert:
            - (4a) the tombstones are not cleaned up after both live nodes join the new group0 even though the killed
              node doesn't belong to it,
            - (4b) the tombstones are cleaned up after removing the killed node
    """
    cmdline = [
        # disabling caches as the tombstones still remain in the cache even after the compaction
        # (the alternative would be to drop the caches after the compaction or to filter the mutation fragments listing)
        '--enable-cache', '0',
    ]
    cfg = {
        'group0_tombstone_gc_refresh_interval_in_ms': 1000,  # this is 1 hour by default
    }
    servers = await manager.servers_add(3, cmdline=cmdline, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r2"}])

    cql = manager.get_cql()
    hosts = [(await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]
             for s in servers]

    host_primary = hosts[0]

    # create/alter/drop a few tables
    async def alter_system_schema(keyspace=None, table_count=3):
        if not keyspace:
            async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 2}", host=host_primary) as keyspace:
                await alter_system_schema(keyspace, table_count)
                return

        for _ in range(table_count):
            async with new_test_table(manager, keyspace, "p int primary key, x int", host=host_primary, reuse_tables=False) as table:
                await cql.run_async(f"ALTER TABLE {table} add y int")

    def get_tombstone(row):
        if row.metadata is None:
            return None
        metadata = json.loads(row.metadata)
        return metadata.get("tombstone")

    async def list_tombstones(tombstone_mark, host):
        tombstones = {}
        for tbl in ("tables", "columns"):
            res = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS(system_schema.{tbl})", host=host))
            tombstones[tbl] = []
            for row in res:
                tombstone = get_tombstone(row)
                if tombstone and datetime.fromtimestamp(float(tombstone["timestamp"])/1_000_000, timezone.utc) < tombstone_mark:
                    tombstones[tbl].append(tombstone)
        return tombstones

    async def tombstone_gc_completed(tombstone_mark):
        # flush and compact the keyspace
        await asyncio.gather(*[manager.api.keyspace_flush(srv.ip_addr, "system_schema")
                               for srv in servers])
        await asyncio.gather(*[manager.api.keyspace_compaction(srv.ip_addr, "system_schema")
                               for srv in servers])

        # check the remanining tombstones
        tombstones_count_total = 0
        tombstones_per_host = await asyncio.gather(*[list_tombstones(tombstone_mark, host)
                                                     for host in hosts])
        for tombstones in tombstones_per_host:
            for tbl in tombstones.keys():
                tombstones_remaining = tombstones[tbl]
                tombstones_count = len(tombstones_remaining)
                tombstones_count_total += tombstones_count
                logger.info(f"{tbl} tombstones remaining: {tombstones_count}")
        if tombstones_count_total != 0:
            return None
        return True

    # should usually run much faster than 30s, but left some margin to avoid flakiness
    async def verify_tombstone_gc(tombstone_mark, timeout=30):
        # wait for 2 sec to let the current tombstones fully expire
        await asyncio.sleep(2)

        deadline = time.time() + timeout

        # perform a single change to generate a new state_id and set the previous tombstones to expire
        # (this is needed because we deduct 1s of the tombstone expiration time to account for the changes coming
        # in the same second)
        await alter_system_schema(table_count=1)

        await wait_for(partial(tombstone_gc_completed, tombstone_mark), deadline)

    with disable_schema_agreement_wait(cql):
        async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 2}", host=host_primary) as keyspace:
            await alter_system_schema(keyspace)
            tombstone_mark = datetime.now(timezone.utc)

            # test #1: the tombstones are cleaned up eventually
            await verify_tombstone_gc(tombstone_mark)

            # shut down one server
            down_server = servers.pop()
            down_host = hosts.pop()
            await manager.server_stop_gracefully(down_server.server_id)
            await asyncio.gather(*[manager.server_not_sees_other_server(srv.ip_addr, down_server.ip_addr)
                                   for srv in servers])

            await alter_system_schema(keyspace)
            tombstone_mark = datetime.now(timezone.utc)

            # test #2: the tombstones are not cleaned up when one node is down
            with pytest.raises(AssertionError, match="timed out"):
                # waiting for shorter time (5s normally enough for a successful case, we expect the timeout here)
                await verify_tombstone_gc(tombstone_mark, timeout=5)

            # start the server again
            await manager.server_start(down_server.server_id)
            await asyncio.gather(*[manager.server_sees_other_server(srv.ip_addr, down_server.ip_addr)
                                   for srv in servers])

            servers.append(down_server)
            hosts.append(down_host)

            # make sure the hosts are available (avoid test flakiness because of ConnectionError)
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

            # test #3: the tombstones are cleaned up after the node is started again
            await verify_tombstone_gc(tombstone_mark)

            first_group0_id = (await cql.run_async(
                "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

            # Kill one server.
            await manager.server_stop(down_server.server_id, convict=True)
            servers.pop()

            await alter_system_schema(keyspace)
            tombstone_mark = datetime.now(timezone.utc)

            # Start the Raft-based recovery procedure.
            await manager.rolling_restart(servers, wait_for_cql=False)

            # TODO: do not reconnect the driver (here and below) once scylladb/python-driver#295 is fixed.
            # It doesn't go well with disable_schema_agreement_wait above.
            cql = await reconnect_driver(manager)
            cql.cluster.max_schema_agreement_wait = 0
            hosts = [(await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]
                     for s in servers]

            for h in hosts:
                await delete_discovery_state_and_group0_id(cql, h)

            recovery_leader_id = await manager.get_host_id(servers[0].server_id)

            async def set_recovery_leader(srv: ServerInfo):
                await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

            await manager.rolling_restart(servers, with_down=set_recovery_leader, wait_for_cql=False)

            cql = await reconnect_driver(manager)
            cql.cluster.max_schema_agreement_wait = 0
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

            # test #4a: the tombstones are not cleaned up after both live nodes join the new group0
            with pytest.raises(AssertionError, match="timed out"):
                await verify_tombstone_gc(tombstone_mark, timeout=5)

            await manager.remove_node(servers[0].server_id, down_server.server_id)

            # test #4b: the tombstones are cleaned up after removing the killed node
            await verify_tombstone_gc(tombstone_mark)

            # Finish the Raft-based recovery procedure. We leave the cluster in a clean state for future tests.
            for srv in servers:
                await manager.server_remove_config_option(srv.server_id, 'recovery_leader')
            for h in hosts:
                await delete_raft_group_data(first_group0_id, cql, h)


@pytest.mark.skip_mode(mode='release', reason="test only needs to run once - allowing only the 'dev' mode")
@pytest.mark.skip_mode(mode='debug', reason="test only needs to run once - allowing only the 'dev' mode")
async def test_group0_state_id_failure(manager: ScyllaClusterManager):
    """
    Issue #21117 regression test.

    Make sure that the "endpoint_state_map does not contain endpoint" warning is not triggered when the state_id is updated.

    Arrange:
        - start 1 node
    Act:
        - restart the node
    Assert:
        - the "endpoint_state_map does not contain endpoint" didn't appear in the log
    """

    # Start the node

    srv = await manager.server_add()

    # Restart the node

    await manager.server_restart(srv.server_id)

    # Check the log

    log = await manager.server_open_log(srv.server_id)
    matches = await log.grep(r"Fail to apply application_state.*endpoint_state_map does not contain endpoint .* application_states = {GROUP0_STATE_ID")

    assert not matches, "The 'endpoint_state_map does not contain endpoint' warning appeared in the log"


@pytest.mark.parametrize("tablets", (True, False))
async def test_tombstone_gc_rf_one(manager: ScyllaClusterManager, tablets: bool):
    """ Check Tombstone GC with RF=1

    In particular, check that:
    * All tables default to tombstone_gc mode 'repair', even with RF=1
    * When RF=1, tombstone-gc mode 'repair' doesn't purge tombstones without a repair
    * When RF=1, repair succeeds and it records the hints and batchlog flush
      time as the repair time, so tombstones written before it are purged,
      while those written after it are not
    * When RF>1, tombstone-gc mode 'repair' works like normal repair-based tombstone gc
    """
    # Disable the flush cache, so each repair records the time of its own flush as the repair time.
    servers = await manager.servers_add(2, auto_rack_dc="dc1", config={'enable_cache': False, 'repair_hints_batchlog_flush_cache_time_in_ms': 0})

    cql, hosts = await manager.get_ready_cql(servers)

    if tablets:
        tablets_enabled = "true"
        rf1_replication = "['rack1']"
        rf2_replication = "['rack1', 'rack2']"
    else:
        tablets_enabled = "false"
        rf1_replication = "1"
        rf2_replication = "2"

    async def get_partition_tombstone_count(table: str, p: int):
        count = 0
        for host in hosts:
            mf_query = f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE p = {p} AND partition_region = 0 ALLOW FILTERING"
            res = list(await cql.run_async(mf_query, host=host))
            logger.info(f"Host {host} returned {res}")
            count += len(res)
        return count

    async def repair(table: str):
        keyspace, table_name = table.split(".")
        if tablets:
            await manager.api.repair(servers[0].ip_addr, keyspace, table_name)
        else:
            # Each node owns different ranges, all of them have to be repaired.
            for srv in servers:
                await manager.api.repair(srv.ip_addr, keyspace, table_name)

    async def compact(keyspace: str):
        await asyncio.gather(*[manager.api.flush_all_keyspaces(srv.ip_addr) for srv in servers])
        await asyncio.gather(*[manager.api.keyspace_compaction(srv.ip_addr, keyspace) for srv in servers])

    async def delete(table: str, p: int, rf: int):
        await manager.api.log(servers[0].ip_addr, f"delete({table}, {p}, {rf})")
        await cql.run_async(SimpleStatement(f"DELETE FROM {table} WHERE p = {p}", consistency_level=ConsistencyLevel.ALL))
        assert await get_partition_tombstone_count(table, p) == rf
        await asyncio.sleep(1) # wait a bit to ensure the tombstone deletion time is in the past

    async with new_test_keyspace(manager, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'dc1': {rf1_replication}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key") as table:
            # default should be repair, even with RF=1
            check_tombstone_gc_mode(cql, table, "repair")

            await cql.run_async(f"ALTER TABLE {table} WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}")

            # No repair yet: the tombstone is not purged.
            await delete(table, 1, 1)
            await compact(keyspace)
            assert await get_partition_tombstone_count(table, 1) == 1

            # Repair: the tombstone written before it is purged.
            await repair(table)
            # Written after the repair: the tombstone is not purged.
            await delete(table, 2, 1)
            await compact(keyspace)
            assert await get_partition_tombstone_count(table, 1) == 0
            assert await get_partition_tombstone_count(table, 2) == 1

            await cql.run_async(f"ALTER KEYSPACE {keyspace} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'dc1': {rf2_replication}}}")
            if not tablets:
                # Populate the new replicas.
                await repair(table)

            # RF=2, no repair since the tombstone was written: the tombstone is not purged.
            await delete(table, 3, 2)
            await compact(keyspace)
            assert await get_partition_tombstone_count(table, 3) == 2

            # RF=2, repair: the tombstone is purged.
            await repair(table)
            await compact(keyspace)
            assert await get_partition_tombstone_count(table, 3) == 0


@pytest.mark.parametrize("rf", (1, 2))
async def test_tablet_repair_fails_without_flush(manager: ScyllaClusterManager, rf: int):
    """ Check that a tablet repair fails when hints and batchlog cannot be flushed, here
    because a node which is not a replica is down, even for a table that is not in repair
    mode: the repair time it records is the flush time, so without a flush it must record
    nothing, like any failed repair, rather than the epoch.

    rf=1 covers the single-replica repair, rf=2 the row-level one.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=RF_ONE_CFG)
    cql, hosts = await manager.get_ready_cql(servers)

    # The replicas are on servers[0] (rack1) and, with rf=2, servers[1] (rack2).
    racks = ", ".join(f"'rack{i + 1}'" for i in range(rf))
    replication = f"{{ 'class': 'NetworkTopologyStrategy', 'dc1': [{racks}] }} and tablets = {{ 'enabled': true }}"
    async with new_test_keyspace(manager, f"with replication = {replication}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key", " with tombstone_gc = {'mode': 'timeout'}") as table:
            table_name = table.split(".")[1]

            await manager.server_stop_gracefully(servers[2].server_id)

            log = await manager.server_open_log(servers[0].server_id)
            mark = await log.mark()
            with pytest.raises(Exception):
                await manager.api.repair(servers[0].ip_addr, keyspace, table_name)
            await log.wait_for("Flush is needed for repair .* but failed", from_mark=mark, timeout=60)


@pytest.mark.parametrize("tablets", (True, False))
async def test_tombstone_gc_rf_one_node_down(manager: ScyllaClusterManager, tablets: bool):
    """ Check repair of an RF=1 table while a node which is not a replica is down

    Hints and batchlog cannot be flushed on the down node, so the repair must
    fail, record no repair time, and tombstones must not be purged.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=RF_ONE_CFG)
    cql, hosts = await manager.get_ready_cql(servers)

    if tablets:
        # The only replica is on servers[0], in rack1.
        replication = "{ 'class': 'NetworkTopologyStrategy', 'dc1': ['rack1'] } and tablets = { 'enabled': true }"
    else:
        replication = "{ 'class': 'NetworkTopologyStrategy', 'dc1': 1 } and tablets = { 'enabled': false }"

    keys = range(30)

    async def get_tombstone_count(table: str):
        return (await get_tombstone_counts(cql, hosts[:1], table))[0]

    async with new_test_keyspace(manager, f"with replication = {replication}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key", " with tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}") as table:
            table_name = table.split(".")[1]

            await asyncio.gather(*[cql.run_async(SimpleStatement(f"DELETE FROM {table} WHERE p = {k}", consistency_level=ConsistencyLevel.ALL)) for k in keys])
            await asyncio.sleep(1) # wait a bit to ensure the tombstone deletion time is in the past
            await manager.api.flush_keyspace(servers[0].ip_addr, keyspace)
            tombstone_count = await get_tombstone_count(table)
            assert tombstone_count > 0

            await manager.server_stop_gracefully(servers[2].server_id)

            with pytest.raises(Exception):
                await manager.api.repair(servers[0].ip_addr, keyspace, table_name)

            await manager.api.keyspace_compaction(servers[0].ip_addr, keyspace)
            assert await get_tombstone_count(table) == tombstone_count


async def test_tombstone_gc_rf_one_small_table_optimization(manager: ScyllaClusterManager):
    """ Check repair of an RF=1 vnode table with the small table optimization

    A single small table optimization repair request covers the whole ring.
    When every token has a single replica, there is nothing to repair, so
    check that:
    * no row-level repair is done, the repair just records the hints and
      batchlog flush time as the repair time, on all nodes,
    * tombstones written before the repair are purged on all nodes,
    * if a node is down, the repair fails and records nothing.
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1", config=RF_ONE_CFG)
    cql, hosts = await manager.get_ready_cql(servers)
    for srv in servers:
        await manager.api.set_logger_level(srv.ip_addr, "repair", "debug")

    keys = range(20)

    async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 1} and tablets = { 'enabled': false }") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key", " with tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}") as table:
            table_name = table.split(".")[1]

            await asyncio.gather(*[cql.run_async(SimpleStatement(f"DELETE FROM {table} WHERE p = {k}", consistency_level=ConsistencyLevel.ALL)) for k in keys])
            await asyncio.sleep(1) # wait a bit to ensure the tombstone deletion time is in the past
            await flush_and_compact(manager, servers, keyspace)
            counts = await get_tombstone_counts(cql, hosts, table)
            # The tombstones are spread over both nodes, none of them is purged.
            assert sum(counts) == len(keys) and all(c > 0 for c in counts), counts

            # A node is down: the repair fails and the tombstones are not purged.
            await manager.server_stop_gracefully(servers[1].server_id)
            with pytest.raises(Exception):
                await manager.api.repair(servers[0].ip_addr, keyspace, table_name, small_table_optimization=True)
            await manager.server_start(servers[1].server_id)
            cql, hosts = await manager.get_ready_cql(servers)
            await flush_and_compact(manager, servers, keyspace)
            assert sum(await get_tombstone_counts(cql, hosts, table)) == len(keys)

            # All nodes are up: the repair records the repair time on all nodes, without row-level repair.
            log = await manager.server_open_log(servers[0].server_id)
            mark = await log.mark()
            await manager.api.repair(servers[0].ip_addr, keyspace, table_name, small_table_optimization=True)
            assert await log.grep("status=skipped_single_replica", from_mark=mark)
            assert not await log.grep(r"Started Row Level Repair \(Master\)", from_mark=mark)
            await flush_and_compact(manager, servers, keyspace)
            assert await get_tombstone_counts(cql, hosts, table) == [0, 0]


async def test_tombstone_gc_rf_one_auto_repair(manager: ScyllaClusterManager):
    """ Check that auto repair repairs RF=1 tablet tables in repair mode

    The repair records the hints and batchlog flush time as the repair time,
    so tombstones written before it are eventually purged.
    """
    cfg = RF_ONE_CFG | {'auto_repair_enabled_default': True, 'auto_repair_threshold_default_in_seconds': 1}
    servers = await manager.servers_add(2, auto_rack_dc="dc1", config=cfg)
    cql, hosts = await manager.get_ready_cql(servers)

    keys = range(10)

    async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'dc1': ['rack1'] } and tablets = { 'enabled': true }") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key", " with tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}") as table:
            await asyncio.gather(*[cql.run_async(SimpleStatement(f"DELETE FROM {table} WHERE p = {k}", consistency_level=ConsistencyLevel.ALL)) for k in keys])

            async def tombstones_purged():
                await manager.api.flush_keyspace(servers[0].ip_addr, keyspace)
                await manager.api.keyspace_compaction(servers[0].ip_addr, keyspace)
                return (await get_tombstone_counts(cql, hosts[:1], table))[0] == 0 or None

            await wait_for(tombstones_purged, time.time() + 120, period=1)


async def test_tombstone_gc_rf_one_incremental_repair_promotes_sstables(manager: ScyllaClusterManager):
    """ The incremental repair of a single-replica tablet promotes its sstables to repaired

    A compaction of the repaired set checks only the repaired sstables for data a
    tombstone shadows. That is safe only if everything delivered up to a repair's
    flush is repaired before the repair time is committed. A tablet that once had
    two replicas has a repaired set; after it shrinks to one replica its repairs
    take the single-replica shortcut, which has to promote the sstables written
    since, or a tombstone in the repaired set can be purged over data it shadows.
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1", config=RF_ONE_CFG)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = {await manager.get_host_id(s.server_id): s for s in servers}

    async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2'] } and tablets = { 'enabled': true, 'initial': 1 }") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, v int", " with tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}") as table:
            table_name = table.split('.')[1]
            # The compactions are started by hand, so that nothing is purged before it should be.
            for s in servers:
                await manager.api.disable_autocompaction(s.ip_addr, keyspace, table_name)

            # T, promoted to repaired by an incremental repair of the two replicas.
            await cql.run_async(SimpleStatement(f"DELETE FROM {table} USING TIMESTAMP 20 WHERE p = 1", consistency_level=ConsistencyLevel.ALL))
            for s in servers:
                await manager.api.flush_keyspace(s.ip_addr, keyspace)
            await manager.api.tablet_repair(servers[0].ip_addr, keyspace, table_name, "all", incremental_mode='incremental')

            # D, shadowed by T: a write that was in flight during the repair, delivered after it.
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (1, 42) USING TIMESTAMP 10", consistency_level=ConsistencyLevel.ALL))
            for s in servers:
                await manager.api.flush_keyspace(s.ip_addr, keyspace)
            assert not list(await cql.run_async(f"SELECT * FROM {table} WHERE p = 1"))

            # Down to a single replica, in rack1.
            await cql.run_async(f"ALTER KEYSPACE {keyspace} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'dc1': ['rack1'] }}")
            async def single_replica():
                replicas = await get_all_tablet_replicas(manager, servers[0], keyspace, table_name)
                assert len(replicas) == 1
                return replicas[0].replicas if len(replicas[0].replicas) == 1 else None
            replicas = await wait_for(single_replica, time.time() + 60)
            server = host_ids[replicas[0][0]]
            host = next(h for h in hosts if h.address == server.ip_addr)
            log = await manager.server_open_log(server.server_id)
            mark = await log.mark()

            # With propagation_delay 0, T is GC-eligible once a repair time later than its
            # deletion time is committed, which the next repair's is if it starts in a later second.
            await asyncio.sleep(1.1)
            # The repair takes the single-replica shortcut, which promotes D's sstable.
            await manager.api.tablet_repair(servers[0].ip_addr, keyspace, table_name, "all", incremental_mode='incremental')
            await log.wait_for("Marking sstable=.* for incremental repair", from_mark=mark, timeout=30)

            # A compaction of the repaired set finds D there and keeps T.
            await manager.api.keyspace_compaction(server.ip_addr, keyspace, table_name)
            rows = list(await cql.run_async(SimpleStatement(f"SELECT p, v FROM {table} WHERE p = 1 BYPASS CACHE", consistency_level=ConsistencyLevel.ONE), host=host))
            assert not rows, f"Resurrected: {rows}"

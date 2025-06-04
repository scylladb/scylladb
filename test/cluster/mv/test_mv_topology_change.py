#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import time
import logging
import requests
import re

from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_view
from test.cluster.mv.tablets.test_mv_tablets import get_tablet_replicas


logger = logging.getLogger(__name__)

# This test reproduces issues #17786 and #18709
# In the test, we create a keyspace with a table and a materialized view.
# We then start writing to the table, causing the materialized view to be updated.
# While the writes are in progress, we add then decommission a node in the cluster.
# The test verifies that no node crashes as a result of the topology change combined
# with the writes.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_topology_change(manager: ManagerClient):
    cfg = {'force_gossip_topology_changes': True,
           'tablets_mode_for_new_keyspaces': 'disabled',
           'error_injections_at_startup': ['delay_before_get_view_natural_endpoint']}

    servers = [await manager.server_add(config=cfg) for _ in range(3)]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")
        await cql.run_async(f"CREATE materialized view {ks}.t_view AS select pk, v from {ks}.t where v is not null primary key (v, pk)")

        stop_event = asyncio.Event()
        concurrency = 10
        async def do_writes(start_it, repeat) -> int:
            iteration = start_it
            while not stop_event.is_set():
                start_time = time.time()
                try:
                    await cql.run_async(f"insert into {ks}.t (pk, v) values ({iteration}, {iteration})")
                except NoHostAvailable as e:
                    for _, err in e.errors.items():
                        # ConnectionException can be raised when the node is shutting down.
                        if not isinstance(err, ConnectionException):
                            logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                            raise
                except Exception as e:
                    logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                    raise
                iteration += concurrency
                if not repeat:
                    break
                await asyncio.sleep(0.01)
            return iteration


        # to hit the issue #18709 it's enough to start one batch of writes, the effective
        # replication maps for base and view will change after the writes start but before they finish
        tasks = [asyncio.create_task(do_writes(i, repeat=False)) for i in range(concurrency)]

        server = await manager.server_add()

        await asyncio.gather(*tasks)

        [await manager.api.disable_injection(s.ip_addr, "delay_before_get_view_natural_endpoint") for s in servers]

        # to hit the issue #17786 we need to run multiple batches of writes, so that some write is processed while the 
        # effective replication maps for base and view are different
        tasks = [asyncio.create_task(do_writes(i, repeat=True)) for i in range(concurrency)]
        await manager.decommission_node(server.server_id)

        stop_event.set()
        await asyncio.gather(*tasks)

# Reproduces #19152
# Verify a pending replica is not doing unnecessary work of building and sending view updates.
# 1) we have a table with a materialized view with RF=1.
#    the base and view tablets start on node 1.
# 2) start migrating a base-table tablet from node 1 to node 2
# 3) while node 2 is a pending replica, write to the table
# 4) complete migration
# 5) verify node 2 did not build view updates
# With the parameter intranode=True it's the same except the tablet
# is migrating between two shards on the same node.
@pytest.mark.parametrize("intranode", [True, False])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_update_on_pending_replica(manager: ManagerClient, intranode):
    cfg = {'tablets_mode_for_new_keyspaces': 'enabled'}
    cmd = ['--smp', '2']
    servers = [await manager.server_add(config=cfg, cmdline=cmd)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv1 AS SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, pk);")

        table_id = await manager.get_table_id(ks, 'test')

        servers.append(await manager.server_add(config=cfg, cmdline=cmd))

        key = 7 # Whatever
        tablet_token = 0 # Doesn't matter since there is one tablet
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 0)")

        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        src_shard = replica[1]
        dst_shard = 1-replica[1]
        assert replica[0] == s0_host_id

        if intranode:
            dst_host = s0_host_id
            dst_ip = servers[0].ip_addr
            streaming_wait_injection = "intranode_migration_streaming_wait"
        else:
            dst_host = s1_host_id
            dst_ip = servers[1].ip_addr
            streaming_wait_injection = "stream_mutation_fragments"

        await manager.api.enable_injection(dst_ip, streaming_wait_injection, one_shot=True)

        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", s0_host_id, src_shard, dst_host, dst_shard, tablet_token))

        async def tablet_is_streaming():
            res = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id={table_id}")
            stage = res[0].stage
            return stage == 'streaming' or None

        await wait_for(tablet_is_streaming, time.time() + 60)

        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, {1})")

        # Release abandoned streaming
        await manager.api.message_injection(dst_ip, streaming_wait_injection)

        logger.info("Waiting for migration to finish")
        await migration_task
        logger.info("Migration done")

        def get_view_updates_on_wrong_node_count(server):
            metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
            pattern = re.compile("^scylla_database_total_view_updates_on_wrong_node")
            for metric in metrics.split('\n'):
                if pattern.match(metric) is not None:
                    return int(float(metric.split()[1]))

        assert all(map(lambda x: x is None or x == 0, [get_view_updates_on_wrong_node_count(server) for server in servers]))

        res = await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk={key}")
        assert [1] == [x.c for x in res]
        res = await cql.run_async(f"SELECT c FROM {ks}.mv1 WHERE pk={key} ALLOW FILTERING")
        assert [1] == [x.c for x in res]

# Reproduces issue #19529
# Write to a table with MV while one node is stopped, and verify
# it doesn't cause MV write timeouts or preventing topology changes.
# The writes that are targeted to the stopped node are with CL=ANY so
# they should store a hint and then complete successfuly.
# If the MV write handler is not completed after storing the hint, as in
# issue #19529, it remains active until it timeouts, preventing topology changes
# during this time.
@pytest.mark.asyncio
@skip_mode('debug', 'the test requires a short timeout for remove_node, but it is unpredictably slow in debug')
async def test_mv_write_to_dead_node(manager: ManagerClient):
    servers = await manager.servers_add(4, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
        {"dc": "dc1", "rack": "r3"}
    ])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")
        await cql.run_async(f"CREATE materialized view {ks}.t_view AS select pk, v from {ks}.t where v is not null primary key (v, pk)")

        await manager.server_stop_gracefully(servers[-1].server_id)

        # Do inserts. some should generate MV writes to the stopped node
        for i in range(100):
            await cql.run_async(f"insert into {ks}.t (pk, v) values ({i}, {i+1})")

        # Remove the node to trigger a topology change.
        # If the MV write is not completed, as in issue #19529, the topology change
        # will be held for long time until the write timeouts.
        # Otherwise, it is expected to complete in short time.
        await manager.remove_node(servers[0].server_id, servers[-1].server_id, timeout=180)

async def test_mv_pairing_during_replace(manager: ManagerClient):
    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"}
    ])
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.t_view AS SELECT pk, v FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, pk)")

    stop_event = asyncio.Event()
    async def do_writes() -> int:
        i = 0
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i+1})", consistency_level=ConsistencyLevel.ONE))
            except NoHostAvailable as e:
                for _, err in e.errors.items():
                    # ConnectionException can be raised when the node is shutting down.
                    if not isinstance(err, ConnectionException):
                        logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                        raise
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                raise
            i += 1
            await asyncio.sleep(0.1)

    write_task = asyncio.create_task(do_writes())
    metrics_before = await manager.metrics.query(servers[2].ip_addr)
    failed_pairing_before = metrics_before.get('scylla_database_total_view_updates_failed_pairing')

    await manager.server_stop_gracefully(servers[1].server_id)
    await manager.others_not_see_server(server_ip=servers[1].ip_addr)
    replace_cfg = ReplaceConfig(replaced_id = servers[1].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg=replace_cfg, property_file={"dc": "dc1", "rack": "r1"},)
    stop_event.set()
    await write_task
    metrics_after = await manager.metrics.query(servers[2].ip_addr)
    failed_pairing_after = metrics_after.get('scylla_database_total_view_updates_failed_pairing')
    assert failed_pairing_before == failed_pairing_after
# Reproduces https://github.com/scylladb/scylladb/issues/21492
# In this test we perform a write that generates a view update while the replication
# factor of the keyspace is being changed and the corresponding base tablet finishes
# the migration at a different time the view tablet does.
@pytest.mark.asyncio
@pytest.mark.parametrize("delayed_replica", ["base", "mv"])
@pytest.mark.parametrize("altered_dc", ["dc1", "dc2"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_rf_change(manager: ManagerClient, delayed_replica: str, altered_dc: str):
    servers = []
    servers.append(await manager.server_add(config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc1', 'rack': 'myrack1'}))
    servers.append(await manager.server_add(config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc1', 'rack': 'myrack2'}))
    servers.append(await manager.server_add(config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc2', 'rack': 'myrack1'}))
    servers.append(await manager.server_add(config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc2', 'rack': 'myrack2'}))

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE ks.base (pk int, ck int, PRIMARY KEY (pk, ck))")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT pk, ck FROM ks.base WHERE ck IS NOT NULL PRIMARY KEY (ck, pk)")
    await wait_for_view(cql, "mv", 4)

    # We'll wait for a specific tablet migration later in the test, so now wait until
    # the base and view tablets are balanced. There are 4 nodes and 4 tablets, so
    # each replica should have 1 tablet.
    async def replicas_balanced():
        base_replicas = [replica[0] for replica in await get_tablet_replicas(manager, servers[0], "ks", "base", 0)]
        view_replicas = [replica[0] for replica in await get_tablet_replicas(manager, servers[0], "ks", "mv", 0)]
        return len(set(base_replicas) & set(view_replicas)) == 0 or None
    await wait_for(replicas_balanced, time.time() + 60)

    # Insert a row so that there's data to be streamed during tablet migration
    await cql.run_async("INSERT INTO ks.base (pk,ck) VALUES (0,0)")

    # Block (on streaming) the base or view tablet migrations, so that one table will have
    # temporarily more replicas (that aren't pending) than the other.
    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, f"block_tablet_streaming", False, parameters={'keyspace': 'ks', 'table': delayed_replica}) for s in servers])
    ks_fut = cql.run_async(f"ALTER KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', '{altered_dc}':2}}")

    # Wait until the not blocked migration finishes before performing a write
    async def first_migration_done():
        stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='ks' ALLOW FILTERING")
        logger.info(f"Current stages: {[row.stage for row in stages]}")
        return set([None, "streaming"]) == set([row.stage for row in stages]) or None
    await wait_for(first_migration_done, time.time() + 60)

    async def get_replica_mismatches():
        metrics = await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
        if delayed_replica == "base":
            return sum([server_metrics.get('scylla_database_total_view_updates_due_to_replica_count_mismatch') or 0 for server_metrics in metrics])
        else:
            return sum([server_metrics.get('scylla_database_total_view_updates_failed_pairing') or 0 for server_metrics in metrics])
    replica_mismatches_before = await get_replica_mismatches()

    # If the base was delayed, there may be just 1 base replica in the DC even though RF is now 2, so use cl=TWO, one from each DC
    await cql.run_async(SimpleStatement("INSERT INTO ks.base (pk,ck) VALUES (1,1)", consistency_level=ConsistencyLevel.TWO))

    # Unblock the tablet migration to allow the RF change to complete
    await asyncio.gather(*[manager.api.message_injection(s.ip_addr, f"block_tablet_streaming") for s in servers])
    await ks_fut

    # Confirm that the problematic scenario occurred
    replica_mismatches = await get_replica_mismatches()
    assert replica_mismatches == replica_mismatches_before + 1

    # Confirm the content of the view after the migration has finished
    async def migrations_done():
        stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='ks' ALLOW FILTERING")
        return [row.stage for row in stages] == [None, None] or None
    await wait_for(migrations_done, time.time() + 60)

    res = await cql.run_async(SimpleStatement(f"SELECT * FROM ks.mv", consistency_level=ConsistencyLevel.ONE))
    assert len(res) == 2

# The same scenario as in the test above, but the RF change affects the first replica in a DC
@pytest.mark.asyncio
@pytest.mark.parametrize("delayed_replica", ["base", "mv"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_first_replica_in_dc(manager: ManagerClient, delayed_replica: str):
    servers = []
    # If we run the test with more than 1 shard and the tablet for the view table gets allocated on the same shard as the tablet of the base table,
    # we'll perform an intranode migration of one of these tablets to the other shard. This migration can be confused with the migration to the
    # new dc in the "first_migration_done()" below. To avoid this, run servers with only 1 shard.
    servers.append(await manager.server_add(cmdline=['--smp', '1'], config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc1', 'rack': 'myrack1'}))
    servers.append(await manager.server_add(cmdline=['--smp', '1'], config={'rf_rack_valid_keyspaces': False}, property_file={'dc': f'dc2', 'rack': 'myrack1'}))

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE ks.base (pk int, ck int, PRIMARY KEY (pk, ck))")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT pk, ck FROM ks.base WHERE ck IS NOT NULL PRIMARY KEY (ck, pk)")
    await wait_for_view(cql, "mv", 2)

    # Insert a row so that there's data to be streamed during tablet migration
    await cql.run_async("INSERT INTO ks.base (pk,ck) VALUES (0,0)")

    # Block (on streaming) the base or view tablet migrations, so that one table will have
    # temporarily more replicas (that aren't pending) than the other.
    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, f"block_tablet_streaming", False, parameters={'keyspace': 'ks', 'table': delayed_replica}) for s in servers])
    ks_fut = cql.run_async(f"ALTER KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc2':1}}")

    # Wait until the not blocked migration finishes before performing a write
    async def first_migration_done():
        stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='ks' ALLOW FILTERING")
        logger.info(f"Current stages: {[row.stage for row in stages]}")
        return set([None, "streaming"]) == set([row.stage for row in stages]) or None
    await wait_for(first_migration_done, time.time() + 60)

    async def get_replica_mismatches():
        metrics = await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
        if delayed_replica == "base":
            return sum([server_metrics.get('scylla_database_total_view_updates_due_to_replica_count_mismatch') or 0 for server_metrics in metrics])
        else:
            return sum([server_metrics.get('scylla_database_total_view_updates_failed_pairing') or 0 for server_metrics in metrics])
    replica_mismatches_before = await get_replica_mismatches()

    await cql.run_async(SimpleStatement("INSERT INTO ks.base (pk,ck) VALUES (1,1)", consistency_level=ConsistencyLevel.ONE))

    # Unblock the tablet migration to allow the RF change to complete
    await asyncio.gather(*[manager.api.message_injection(s.ip_addr, f"block_tablet_streaming") for s in servers])
    await ks_fut

    # Confirm that the problematic scenario occurred
    replica_mismatches = await get_replica_mismatches()
    assert replica_mismatches == replica_mismatches_before + 1

    # Confirm the content of the view after the migration has finished
    async def migrations_done():
        stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='ks' ALLOW FILTERING")
        return [row.stage for row in stages] == [None, None] or None
    await wait_for(migrations_done, time.time() + 60)

    res = await cql.run_async(SimpleStatement(f"SELECT * FROM ks.mv", consistency_level=ConsistencyLevel.ONE))
    assert len(res) == 2

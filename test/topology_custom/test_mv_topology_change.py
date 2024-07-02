#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
import time
import logging
import requests
import re

from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replica
from test.topology.conftest import skip_mode
from test.pylib.util import wait_for


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
    cfg = {'force_gossip_topology_changes': True, 'error_injections_at_startup': ['delay_before_get_view_natural_endpoint']}

    servers = [await manager.server_add(config=cfg, timeout=60) for _ in range(3)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    stop_event = asyncio.Event()
    concurrency = 10
    async def do_writes(start_it, repeat) -> int:
        iteration = start_it
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(f"insert into ks.t (pk, v) values ({iteration}, {iteration})")
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
    [await manager.api.enable_injection(s.ip_addr, "delay_after_erm_update", False, parameters={'ks_name': 'ks', 'cf_name': 't'}) for s in servers]

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
    cfg = {'enable_tablets': True}
    cmd = ['--smp', '2']
    servers = [await manager.server_add(config=cfg, cmdline=cmd)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await cql.run_async("CREATE MATERIALIZED VIEW test.mv1 AS SELECT * FROM test.test WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, pk);")

    table_id = await manager.get_table_id('test', 'test')

    servers.append(await manager.server_add(config=cfg, cmdline=cmd))

    key = 7 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, 0)")

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)
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
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", s0_host_id, src_shard, dst_host, dst_shard, tablet_token))

    async def tablet_is_streaming():
        res = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id={table_id}")
        stage = res[0].stage
        return stage == 'streaming' or None

    await wait_for(tablet_is_streaming, time.time() + 60)

    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, {1})")

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

    res = await cql.run_async(f"SELECT c FROM test.test WHERE pk={key}")
    assert [1] == [x.c for x in res]
    res = await cql.run_async(f"SELECT c FROM test.mv1 WHERE pk={key} ALLOW FILTERING")
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
async def test_mv_write_to_dead_node(manager: ManagerClient):
    servers = await manager.servers_add(4)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    await manager.server_stop_gracefully(servers[-1].server_id)

    # Do inserts. some should generate MV writes to the stopped node
    for i in range(100):
        await cql.run_async(f"insert into ks.t (pk, v) values ({i}, {i+1})")

    # Remove the node to trigger a topology change.
    # If the MV write is not completed, as in issue #19529, the topology change
    # will be held for long time until the write timeouts.
    # Otherwise, it is expected to complete in short time.
    await manager.remove_node(servers[0].server_id, servers[-1].server_id, timeout=30)

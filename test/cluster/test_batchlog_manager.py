#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import os
import pytest
import logging
import time
from cassandra import ConsistencyLevel, Timeout, Unavailable  # type: ignore
from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
from test.cluster.util import cassandra_stress, create_new_test_keyspace, new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_batchlog_replay_while_a_node_is_down(manager: ScyllaClusterManager) -> None:
    """ Test that batchlog replay handles the case when a node is down while replaying a batch.
        Reproduces issue #24599.
    1. Create a cluster with 3 nodes.
    2. Write a batch and inject an error to fail it before it's removed from the batchlog, so it
       needs to be replayed.
    3. Stop server 1.
    4. Server 0 tries to replay the batch. it sends the mutation to all replicas, but one of them is down,
       so it should fail.
    5. Bring server 1 back up.
    6. Verify that the batch is replayed and removed from the batchlog eventually.
    """

    cmdline=['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c))")

        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False) for s in servers])

        # make sure the batch is replayed only after the server is stopped
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "skip_batch_replay", one_shot=False) for s in servers])

        s0_log = await manager.server_open_log(servers[0].server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.tab (key, c, v) VALUES (0,0,0); INSERT INTO {ks}.tab (key, c, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # injected error is expected
            logger.error(f"Error executing batch: {e}")

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog") for s in servers])

        await manager.server_stop(servers[1].server_id, convict=True)

        batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=hosts[0]))[0].count
        assert batchlog_row_count > 0

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "skip_batch_replay") for s in servers if s != servers[1]])

        # The batch is replayed while server 1 is down
        await s0_log.wait_for('Replaying batch', timeout=60)
        await asyncio.sleep(1)

        # Bring server 1 back up and verify that eventually the batch is replayed and removed from the batchlog
        await manager.server_start(servers[1].server_id)

        s0_mark = await s0_log.mark()
        await s0_log.wait_for('Finished replayAllFailedBatches', timeout=60, from_mark=s0_mark)

        async def batchlog_empty() -> bool:
            batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=hosts[0]))[0].count
            if batchlog_row_count == 0:
                return True
        await wait_for(batchlog_empty, time.time() + 60)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_batchlog_replay_aborted_on_shutdown(manager: ScyllaClusterManager) -> None:
    """ Similar to the previous test, but also verifies that the batchlog replay is aborted on shutdown,
        and node shutdown is not stuck.
    1. Create a cluster with 3 nodes.
    2. Write a batch and inject an error to fail it before it's removed from the batchlog, so it
       needs to be replayed.
    3. Stop server 1.
    4. Server 0 tries to replay the batch. it sends the mutation to all replicas, but one of them is down,
       so it should fail.
    5. Shut down server 0 gracefully, which should abort the batchlog replay which is in progress.
    6. Bring server 0 and server 1 back up.
    6. Verify that the batch is replayed and removed from the batchlog eventually.
    """

    cmdline=['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c))")

        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False) for s in servers])

        # make sure the batch is replayed only after the server is stopped
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "skip_batch_replay", one_shot=False) for s in servers])

        s0_log = await manager.server_open_log(servers[0].server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.tab (key, c, v) VALUES (0,0,0); INSERT INTO {ks}.tab (key, c, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # injected error is expected
            logger.error(f"Error executing batch: {e}")

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog") for s in servers])

        await manager.server_stop(servers[1].server_id, convict=True)

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "skip_batch_replay") for s in servers if s != servers[1]])

        batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=hosts[0]))[0].count
        assert batchlog_row_count > 0

        # The batch is replayed while server 1 is down
        await s0_log.wait_for('Replaying batch', timeout=60)
        await asyncio.sleep(1)

        # verify shutdown is not stuck
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        await manager.server_start(servers[1].server_id)

        cql = await reconnect_driver(manager)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        async def batchlog_empty() -> bool:
            batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=hosts[0]))[0].count
            if batchlog_row_count == 0:
                return True
        await wait_for(batchlog_empty, time.time() + 60)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_batchlog_replay_includes_cdc(manager: ScyllaClusterManager) -> None:
    """ Test that when a batch is replayed from the batchlog, it includes CDC mutations.
    1. Create a cluster with a single node.
    2. Create a table with CDC enabled.
    3. Write a batch and inject an error to fail it after it's written to the batchlog but before the mutation is applied.
    4. Wait for the batch to be replayed.
    5. Verify that the data is written to the base table.
    6. Verify that CDC mutations are also applied and visible in the CDC log table.
    """

    cmdline = ['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}

    servers = await manager.servers_add(1, config=config, cmdline=cmdline)
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        # Create table with CDC enabled
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c)) WITH cdc = {{'enabled': true}}")

        # Enable error injection to make the batch fail after writing to batchlog
        await manager.api.enable_injection(servers[0].ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False)

        # Execute a batch that will fail due to injection but be written to batchlog
        try:
            await cql.run_async(
                "BEGIN BATCH " +
                f"INSERT INTO {ks}.tab(key, c, v) VALUES (10, 20, 30); " +
                f"INSERT INTO {ks}.tab(key, c, v) VALUES (40, 50, 60); " +
                "APPLY BATCH"
            )
        except Exception as e:
            logger.info(f"Expected error executing batch: {e}")

        await manager.api.disable_injection(servers[0].ip_addr, "storage_proxy_fail_remove_from_batchlog")

        # Wait for data to appear in the base table
        async def data_written():
            result1 = await cql.run_async(f"SELECT * FROM {ks}.tab WHERE key = 10 AND c = 20")
            result2 = await cql.run_async(f"SELECT * FROM {ks}.tab WHERE key = 40 AND c = 50")
            if len(result1) > 0 and len(result2) > 0:
                return True
        await wait_for(data_written, time.time() + 60)

        # Check that CDC log table exists and has the CDC mutations
        cdc_table_name = f"{ks}.tab_scylla_cdc_log"

        # Wait for CDC mutations to be visible
        async def cdc_data_present():
            result1 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 10 ALLOW FILTERING")
            result2 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 40 ALLOW FILTERING")
            if len(result1) > 0 and len(result2) > 0:
                return True
        await wait_for(cdc_data_present, time.time() + 60)

        result1 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 10 ALLOW FILTERING")
        assert len(result1) == 1, f"Expected 1 CDC mutation for key 10, got {len(result1)}"

        result2 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 40 ALLOW FILTERING")
        assert len(result2) == 1, f"Expected 1 CDC mutation for key 40, got {len(result2)}"

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_mutations_for_dropped_table(manager: ScyllaClusterManager) -> None:
    """
    This test is an adjusted version of `test_batchlog_replay_while_a_node_is_down`.
    We want to verify that batchlog replay is aborted when the corresponding table has been dropped.

    This test reproduces scylladb/scylladb#24806.

    1. Create a cluster with 2 nodes, a keyspace, and a table.
    2. Enable error injections. We need to ensure that:
       - The mutations are not removed from the batchlog.
       - The mutations are not replayed from the batchlog before we drop the table.
    3. Write a batch. Because of step 2, the mutations will stay in the batchlog.
    4. Drop the table.
    5. Resume batchlog replay.
    6. Wait for the replay to finish. Verify that the batchlog is empty.

    Note: This test will most likely work even with a 1-node cluster, but let's
          use 2 nodes to make sure we're dealing with a realistic scenario.
    """

    cmdline=["--logger-log-level", "batchlog_manager=trace"]
    config = {"error_injections_at_startup": ["short_batchlog_manager_replay_interval"],
              "write_request_timeout_in_ms": 2000}

    servers = await manager.servers_add(2, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    s1, _ = servers

    cql, hosts = await manager.get_ready_cql(servers)
    host1, _ = hosts

    async def get_batchlog_row_count():
        rows = await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=host1)
        row = rows[0]
        assert hasattr(row, "count")

        result = row.count
        logger.debug(f"Batchlow row count={result}")

        return result

    async def enable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, injection, one_shot=False) for s in servers])

    async def disable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, injection) for s in servers])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.my_table (pk int, ck int, v int, PRIMARY KEY (pk, ck))")

        # Make sure the mutations stay in the batchlog.
        await enable_injection("storage_proxy_fail_remove_from_batchlog")
        # Make sure the mutations are not replayed too early (i.e. before dropping the table).
        await enable_injection("skip_batch_replay")

        s1_log = await manager.server_open_log(s1.server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.my_table (pk, ck, v) VALUES (0,0,0);"
                                f"INSERT INTO {ks}.my_table (pk, ck, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # Injected error is expected.
            logger.error(f"Error executing batch: {e}")

        # Once the mutations are in the batchlog, waiting to be replayed, we can disable this.
        await disable_injection("storage_proxy_fail_remove_from_batchlog")

        batchlog_row_count = await get_batchlog_row_count()
        assert batchlog_row_count > 0

        await cql.run_async(f"DROP TABLE {ks}.my_table")

        s1_mark = await s1_log.mark()
        # Once the table is dropped, we can resume the replay. The bug can
        # be triggered from now on (if it's present).
        await disable_injection("skip_batch_replay")

        # We don't need these, but let's keep them just so we know the replay
        # is really going on and the mutations don't just disappear.
        await s1_log.wait_for("Replaying batch", timeout=60, from_mark=s1_mark)
        await s1_log.wait_for("Finished replayAllFailedBatches", timeout=60, from_mark=s1_mark)

        async def batchlog_empty() -> bool:
            batchlog_row_count = await get_batchlog_row_count()
            return True if batchlog_row_count == 0 else None

        await wait_for(batchlog_empty, time.time() + 60)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("repair_cache", [True, False])
async def test_batchlog_replay_failure_during_repair(manager: ScyllaClusterManager, repair_cache: bool) -> None:
    """
    We want to verify that repair_time will not be updated if batchlog replay fails.

    This test reproduces scylladb/scylladb#24415.

    1. Create a cluster with 2 nodes, a keyspace, and a table.
    2. Do some preparations to ensure that mutations won't be removed from the batchlog.
    3. Write a batch.
    4. Delete a row (with the key that is modified in the batch).
    5. Enable injections to make batchlog replay fail.
    6. Run repair on the table.
    7. Compact the keyspace.
    8. Disable injection and wait for the replay to finish.
    9. Verify that the row is deleted and there is no data resurrection.
    """

    write_timeout_ms = 2000

    cmdline=['--enable-cache', '0',
             "--hinted-handoff-enabled", "0",
             "--logger-log-level", "batchlog_manager=trace:repair=debug",
             "--repair-hints-batchlog-flush-cache-time-in-ms", "1000000" if repair_cache else "0"]
    config = {"error_injections_at_startup": ["short_batchlog_manager_replay_interval"],
              "write_request_timeout_in_ms": write_timeout_ms,
              'group0_tombstone_gc_refresh_interval_in_ms': 1000,
              'tablets_mode_for_new_keyspaces': 'disabled'
    }

    servers = await manager.servers_add(2, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    s1, s2 = servers

    cql, hosts = await manager.get_ready_cql(servers)
    host1, _ = hosts

    async def get_batchlog_row_count():
        rows = await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=host1)
        row = rows[0]
        assert hasattr(row, "count")

        result = row.count
        logger.debug(f"Batchlow row count={result}")

        return result

    async def enable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, injection, one_shot=False) for s in servers])

    async def disable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, injection) for s in servers])

    async def batchlog_empty() -> bool:
        batchlog_row_count = await get_batchlog_row_count()
        return True if batchlog_row_count == 0 else None

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.my_table (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '1'}} AND compaction = {{ 'class' : 'NullCompactionStrategy' }}")

        await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.my_table (pk, ck, v) VALUES (0,0,0);"
                                f"INSERT INTO {ks}.my_table (pk, ck, v) VALUES (1,1,1); APPLY BATCH")

        await manager.api.flush_keyspace(s1.ip_addr, ks)
        await manager.api.flush_keyspace(s2.ip_addr, ks)

        await wait_for(batchlog_empty, time.time() + 60)

        # Make sure the mutations stay in the batchlog.
        await enable_injection("storage_proxy_fail_remove_from_batchlog")
        # Make sure the mutations are not replayed too early (i.e. before dropping the table).
        await enable_injection("skip_batch_replay")

        s1_log = await manager.server_open_log(s1.server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.my_table (pk, ck, v) VALUES (0,0,1);"
                                f"INSERT INTO {ks}.my_table (pk, ck, v) VALUES (1,1,2); APPLY BATCH")
        except Exception as e:
            # Injected error is expected.
            logger.error(f"Error executing batch: {e}")

        await cql.run_async(f"DELETE FROM {ks}.my_table WHERE pk=0 AND ck=0")

        await asyncio.sleep((write_timeout_ms // 1000) * 2 + 1)

        batchlog_row_count = await get_batchlog_row_count()
        assert batchlog_row_count > 0

        s1_log = await manager.server_open_log(s1.server_id)
        s1_mark = await s1_log.mark()

        # Once the mutations are in the batchlog, waiting to be replayed, we can disable this.
        await disable_injection("storage_proxy_fail_remove_from_batchlog")
        await enable_injection("storage_proxy_fail_replay_batch")
        # Once the table is dropped, we can resume the replay. The bug can
        # be triggered from now on (if it's present).
        await disable_injection("skip_batch_replay")

        await s1_log.wait_for("Batchlog replay on shard 0: done", timeout=60, from_mark=s1_mark)
        await s1_log.wait_for("Batchlog replay on shard 1: done", timeout=60, from_mark=s1_mark)

        await manager.api.repair(s1.ip_addr, ks, "my_table")

        await manager.api.flush_keyspace(s1.ip_addr, ks)
        await manager.api.flush_keyspace(s2.ip_addr, ks)

        await manager.api.keyspace_compaction(s1.ip_addr, ks)
        await manager.api.keyspace_compaction(s2.ip_addr, ks)

        await disable_injection("storage_proxy_fail_replay_batch")

        await s1_log.wait_for("Replaying batch", timeout=60, from_mark=s1_mark)
        await s1_log.wait_for("Finished replayAllFailedBatches", timeout=60, from_mark=s1_mark)

        await wait_for(batchlog_empty, time.time() + 60)

        batchlog_row_count = await get_batchlog_row_count()
        assert batchlog_row_count == 0

        res = await cql.run_async(f"SELECT * FROM {ks}.my_table WHERE pk=0 BYPASS CACHE")
        assert len(res) == 0


# A LOGGED batch at CL=ANY is accepted with one of three replicas (RF=3)
# down: the batchlog only needs two live nodes. Once the node is back, it
# has the batch's rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_logged_batch_at_cl_any_with_node_down(manager: ScyllaClusterManager) -> None:
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config={"tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (id int PRIMARY KEY, name text)")
        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.server_not_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)
        await cql.run_async(SimpleStatement(f"""
            BEGIN BATCH
            INSERT INTO {ks}.t (id, name) VALUES (0, 'Jack')
            INSERT INTO {ks}.t (id, name) VALUES (1, 'Will')
            APPLY BATCH""", consistency_level=ConsistencyLevel.ANY))
        await manager.server_start(servers[2].server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        # The rows reach the restarted node by hint replay.
        async def rows_arrived():
            rows = await cql.run_async(f"SELECT id FROM MUTATION_FRAGMENTS({ks}.t) WHERE mutation_fragment_kind = 'clustering row' ALLOW FILTERING", host=hosts[2])
            return sorted(r.id for r in rows) == [0, 1] or None
        await wait_for(rows_arrived, time.time() + 60)


async def batch_prepare(manager: ScyllaClusterManager, nodes: int, **kwargs):
    """The dtest's prepare(): a node per rack, and keyspace ks of RF=nodes
    with its clicks and users tables. Returns the servers and the keyspace."""
    servers = await manager.servers_add(nodes, auto_rack_dc="dc1", **kwargs)
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}} AND tablets = {{'enabled': false}}")
    await cql.run_async(f"CREATE TABLE {ks}.clicks (userid int, url text, total counter, PRIMARY KEY (userid, url))")
    await cql.run_async(f"CREATE TABLE {ks}.users (id int, firstname text, lastname text, PRIMARY KEY (id))")
    return servers, ks


def users_batch(ks: str) -> str:
    return (f"BEGIN BATCH INSERT INTO {ks}.users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') "
            f"INSERT INTO {ks}.users (id, firstname, lastname) VALUES (1, 'Will', 'Turner') APPLY BATCH")


# A logged batch fails with Unavailable when its coordinator has no live
# node to write the batchlog to: the other two of three nodes are down.
async def test_logged_batch_throws_uae(manager: ScyllaClusterManager) -> None:
    servers, ks = await batch_prepare(manager, 3)
    for s in servers[1:]:
        await manager.server_stop_gracefully(s.server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[1].ip_addr)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)
    cql = await manager.get_cql_exclusive(servers[0])
    with pytest.raises(Unavailable):
        await cql.run_async(SimpleStatement(users_batch(ks), consistency_level=ConsistencyLevel.ONE,
                                            retry_policy=FallthroughRetryPolicy()))


# With the other two of three nodes killed and not yet seen down, the
# batchlog write of a logged batch gets no response: the batch times out
# with no acknowledgment (or, if the coordinator already saw them down,
# fails with Unavailable with no live replica).
async def test_acknowledged_by_batchlog_not_set_when_batchlog_write_fails(manager: ScyllaClusterManager) -> None:
    servers, ks = await batch_prepare(manager, 3)
    cql = await manager.get_cql_exclusive(servers[0])
    for s in servers[1:]:
        await manager.server_stop(s.server_id, convict=False)
    with pytest.raises((Timeout, Unavailable)) as ex:
        await cql.run_async(SimpleStatement(users_batch(ks), consistency_level=ConsistencyLevel.ONE,
                                            retry_policy=FallthroughRetryPolicy()), timeout=None)
    if isinstance(ex.value, Timeout):
        assert ex.value.received_responses == 0
    else:
        assert ex.value.alive_replicas == 0



# Ported from scylla-dtest batch_test.py::TestBatch::test_acknowledged_by_batchlog_set_when_batchlog_write_succeeds.
# With one of three nodes killed, the batchlog write of a logged batch at
# CL=THREE succeeds on the two live nodes, but the batch itself times out with
# their two responses (or, if the coordinator already saw the node down, fails
# with Unavailable with two live replicas).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_acknowledged_by_batchlog_set_when_batchlog_write_succeeds(manager: ScyllaClusterManager) -> None:
    servers, ks = await batch_prepare(manager, 3)
    cql = await manager.get_cql_exclusive(servers[0])
    await manager.server_stop(servers[-1].server_id, convict=False)
    with pytest.raises((Timeout, Unavailable)) as ex:
        await cql.run_async(SimpleStatement(users_batch(ks), consistency_level=ConsistencyLevel.THREE,
                                            retry_policy=FallthroughRetryPolicy()), timeout=None)
    if isinstance(ex.value, Timeout):
        assert ex.value.received_responses == 2
    else:
        assert ex.value.alive_replicas == 2

# Reproduces #3229: logged batches at CL=ALL on four nodes, whose racks are
# named so that the batchlog manager's choice of batchlog replicas from
# other racks hashes them in various orders, must not make any node log
# "unknown endpoint". The batches are the dtest's cassandra-stress user
# profile of LOGGED batches.
@pytest.mark.parametrize("rack_names", [["rc", "rc2", "rc3", "rc4"], ["1a", "1b", "1c", "1d"], ["rc1", "rc2", "rc3", "rc4"]])
async def test_batchlog_manager_issue(manager: ScyllaClusterManager, rack_names) -> None:
    servers = await manager.servers_add(4, property_file=[{"dc": "dc1", "rack": rack} for rack in rack_names], config={"tablets_mode_for_new_keyspaces": "disabled"})
    profile = os.path.join(os.path.dirname(__file__), "..", "resource", "cassandra-stress", "batch_complex_schema.yaml")
    await cassandra_stress(manager, servers[-1], "user", "no-warmup", f"profile={os.path.realpath(profile)}", "ops(insert=1)",
                           "cl=ALL", "duration=5s", "-rate", "threads=100", "-pop", "seq=1..500", create_keyspace=False)
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        log = await manager.server_open_log(s.server_id)
        assert not await log.grep("unknown endpoint")

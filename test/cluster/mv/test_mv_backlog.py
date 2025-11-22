#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import logging
import time
import asyncio
import pytest
from test.cluster.conftest import skip_mode
from test.pylib.util import wait_for_view, wait_for
from test.cluster.mv.tablets.test_mv_tablets import pin_the_only_tablet
from test.pylib.tablets import get_tablet_replica, get_tablet_replicas
from test.cluster.util import new_test_keyspace
from test.cluster.test_view_building_coordinator import delete_table_sstables

logger = logging.getLogger(__name__)


# This test reproduces issue #18542
# In the test, we create a table and perform a write to it a couple of times
# Each time, we check that a view update backlog on some shard increased
# due to the write.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_view_backlog_increased_after_write(manager: ManagerClient) -> None:
    node_count = 2
    # Use a higher smp to make it more likely that the writes go to a different shard than the coordinator.
    servers = await manager.servers_add(node_count, cmdline=['--smp', '5'], config={'error_injections_at_startup': ['never_finish_remote_view_updates'], 'tablets_mode_for_new_keyspaces': 'enabled'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (base_key int, view_key int, v text, PRIMARY KEY (base_key, view_key))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE view_key IS NOT NULL and base_key IS NOT NULL PRIMARY KEY (view_key, base_key) ")

        await wait_for_view(cql, 'mv_cf_view', node_count)
        # Only remote updates hold on to memory, so make the update remote
        await pin_the_only_tablet(manager, ks, "tab", servers[0])
        (_, shard) = await get_tablet_replica(manager, servers[0], ks, "tab", 0)
        await pin_the_only_tablet(manager, ks, "mv_cf_view", servers[1])

        for v in [1000, 4000, 16000, 64000, 256000]:
            # Don't use a prepared statement, so that writes are likely sent to a different shard
            # than the one containing the key.
            await cql.run_async(f"INSERT INTO {ks}.tab (base_key, view_key, v) VALUES ({v}, {v}, '{v*'a'}')")
            # The view update backlog should increase on the node generating view updates
            local_metrics = await manager.metrics.query(servers[0].ip_addr)
            view_backlog = local_metrics.get('scylla_storage_proxy_replica_view_update_backlog', {'shard':str(shard)})
            # The read view_backlog might still contain backlogs from the previous iterations, so we only assert that it is large enough
            assert view_backlog > v

# This test reproduces issues #18461 and #18783
# In the test, we create a table and perform a write to it that fills the view update backlog.
# After a gossip round is performed, the following write should succeed.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_gossip_same_backlog(manager: ManagerClient) -> None:
    node_count = 2
    servers = await manager.servers_add(node_count, config={'error_injections_at_startup': ['view_update_limit', 'update_backlog_immediately'], 'tablets_mode_for_new_keyspaces': 'enabled'})
    cql, hosts = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
        await wait_for_view(cql, 'mv_cf_view', node_count)

        # Only remote updates hold on to memory, so make the update remote
        await pin_the_only_tablet(manager, ks, "tab", servers[0])
        await pin_the_only_tablet(manager, ks, "mv_cf_view", servers[1])

        stmt = cql.prepare(f"INSERT INTO {ks}.tab (key, c, v) VALUES (?, ?, ?)")

        await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "never_finish_remote_view_updates", one_shot=False) for s in servers))
        await cql.run_async(stmt, [0, 0, 240000*'a'], host=hosts[0])
        await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "never_finish_remote_view_updates") for s in servers))
        # The next write should be admitted eventually, after a gossip round (1s) is performed
        await cql.run_async(stmt, [0, 0, 'a'], host=hosts[0])

# A test for the view_flow_control_delay_limit_in_ms parameter.
#
# The test creates a table with a materialized view and pins all tablets
# of the base on one node, and all tablets of the view to another node.
#
# Then, for a bunch of some possible values of the parameter, do the following:
#
# - Perform a live update of the parameter on both nodes.
# - Do a large write to the base table while using the `never_finish_remote_view_updates`
#   error injection to keep the remote view update resident and occupying
#   memory, influencing the view update backlog as a result.
#   Follow it with one more small write to make sure that it is delayed
#   by the first write's backlog.
# - Read the `scylla_storage_proxy_coordinator_mv_flow_control_delay_total` metric,
#   before and after the second write, in order to measure the calculated delay
#   of last view update.
#
# Finally, calculate ratios between the measured delays - the ratios should be
# the same as ratios of the view_flow_control_delay_limit_in_ms parameter
# that was set during the measurement.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_configurable_mv_control_flow_delay(manager: ManagerClient) -> None:
    node_count = 2
    servers = await manager.servers_add(node_count,
                                        config={'error_injections_at_startup': ['update_backlog_immediately', 'view_update_limit', 'skip_updating_local_backlog_via_view_update_backlog_broker'], 'tablets_mode_for_new_keyspaces': 'enabled'},
                                        cmdline=['--smp=1'])
    cql, hosts = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
        await wait_for_view(cql, 'mv_cf_view', node_count)

        # Only remote updates hold on to memory, so make the update remote
        srv_base = servers[0]
        srv_view = servers[1]
        host_base = next(h for h in hosts if h.address == srv_base.ip_addr)
        await pin_the_only_tablet(manager, ks, "tab", srv_base)
        await pin_the_only_tablet(manager, ks, "mv_cf_view", srv_view)

        # All nodes in the cluster run with --smp=1, so there is only shard 0
        shard = 0

        delay_metric_name = 'scylla_storage_proxy_coordinator_mv_flow_control_delay_total'
        throttled_writes_metric_name = 'scylla_storage_proxy_coordinator_throttled_base_writes_total'

        delay_limits = [0, 500, 1000, 2000, 10000]
        computed_delays = []

        stmt = cql.prepare(f"INSERT INTO {ks}.tab (key, c, v) VALUES (?, ?, ?)")

        for delay_limit in delay_limits:
            logger.info(f"delay_limit = {delay_limit}")

            # Update the delay
            await asyncio.gather(*(cql.run_async(f"UPDATE system.config SET value = '{delay_limit}' WHERE name = 'view_flow_control_delay_limit_in_ms'", host=h) for h in hosts))

            # Make sure that view updates will hang
            await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "never_finish_remote_view_updates", one_shot=False) for s in servers))

            # Generate a large view update and then a small one.
            # The reason why we do two writes is as follows: view backlog is propagated
            # in responses from base writes and the coordinator caches it but it will
            # not necessarily use it when calculating the delay of the same write.
            # The second small write will use the value of the backlog from the previous write.
            await cql.run_async(stmt, [0, 0, 100000*'a'], host=host_base)

            # Measure the total delay before the second write, and the number of delayed writes
            local_metrics = await manager.metrics.query(srv_base.ip_addr)
            before_computed_delay = local_metrics.get(delay_metric_name, {'scheduling_group_name': 'sl:default', 'shard': str(shard)}) or 0.0
            before_total_throttled_writes = local_metrics.get(throttled_writes_metric_name, {'scheduling_group_name': 'sl:default', 'shard': str(shard)}) or 0.0

            # Do the second write, as mentioned previously
            await cql.run_async(stmt, [0, 0, ''], host=host_base)

            # Make sure that there is exactly one throttled write and calculate a delay for it.
            # If we're testing the 0ms delay, instead make sure that there were no delayed writes.
            local_metrics = await manager.metrics.query(srv_base.ip_addr)
            after_computed_delay = local_metrics.get(delay_metric_name, {'scheduling_group_name': 'sl:default', 'shard': str(shard)}) or 0.0
            after_total_throttled_writes = local_metrics.get(throttled_writes_metric_name, {'scheduling_group_name': 'sl:default', 'shard': str(shard)}) or 0.0

            if delay_limit == 0:
                assert after_total_throttled_writes == before_total_throttled_writes
            else:
                assert after_total_throttled_writes == before_total_throttled_writes + 1

            computed_delay = after_computed_delay - before_computed_delay
            computed_delays.append(computed_delay)

            # Unpause the view update and wait until it is drained in order to prepare for the next pass
            await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "never_finish_remote_view_updates") for s in servers))
            async def view_updates_drained():
                local_metrics = await manager.metrics.query(srv_base.ip_addr)
                backlog = local_metrics.get('scylla_storage_proxy_replica_view_update_backlog', {'shard':str(shard)})
                if backlog == 0:
                    return True
            await wait_for(view_updates_drained, deadline=time.time() + 30.0)

        ratios = [delay / limit for delay, limit in zip(computed_delays, delay_limits) if limit != 0]

        logger.info(f"delay_limits: {delay_limits}")
        logger.info(f"computed_delays: {computed_delays}")
        logger.info(f"ratios (for non-zero limits): {ratios}")

        # Check that the ratios are relatively stable, i.e. there is not much
        # relative difference between minimum and maximum
        assert min(ratios) / max(ratios) > 0.9

        # Additionally, check that the delay is zero for a zero value
        # of the view_flow_control_delay_limit_in_ms parameter
        assert computed_delays[0] == 0.0

# This test verifies that view update backlog doesn't grow unbounded
# when generating view updates from staging sstables.
# The test creates staging sstables by deleting normal sstables and repairing,
# then checks that view updates are processed with limited concurrency, by checking
# the view update backlog metric.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_view_backlog_bounded_during_staging_sstable_processing(manager: ManagerClient) -> None:
    # Use 3 nodes: 1 in rack1 (source for repair) and 2 in rack2 (base and view tablets)
    # With RF=2, this ensures we have a replica to repair from after deleting sstables
    servers = [
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack1'}),
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack2'}),
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack2'}),
    ]

    # Disable automatic tablet load balancing since we're setting the layout manually
    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    cql = manager.get_cql()
    # Use RF=2 so that after deleting sstables on one node, repair can restore data from the other replica
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")

        await wait_for_view(cql, 'mv_cf_view', len(servers))

        # Move base table tablet to servers[1] (rack2) and view tablet to servers[2] (rack2)
        # This ensures remote updates between the two nodes in rack2
        host_ids = [await manager.get_host_id(s.server_id) for s in servers]
        base_replicas = await get_tablet_replicas(manager, servers[0], ks, "tab", 0)
        view_replicas = await get_tablet_replicas(manager, servers[0], ks, "mv_cf_view", 0)

        for base_host_id, base_shard in base_replicas:
            if base_host_id == host_ids[2]:
                await manager.api.move_tablet(servers[0].ip_addr, ks, "tab", base_host_id, base_shard, host_ids[1], 0, 0)
                break

        for view_host_id, view_shard in view_replicas:
            if view_host_id == host_ids[1]:
                await manager.api.move_tablet(servers[0].ip_addr, ks, "mv_cf_view", view_host_id, view_shard, host_ids[2], 0, 0)
                break

        await manager.api.enable_injection(servers[1].ip_addr, "never_finish_remote_view_updates", one_shot=False)

        # First, measure the overhead per row by checking backlog with exactly one view update
        # Then use all rows of that size
        row_value = 'a' * 100
        await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES (0, 0, '{row_value}')")

        # Wait until the backlog is non-zero (view update generated)
        async def backlog_nonzero():
            local_metrics = await manager.metrics.query(servers[1].ip_addr)
            backlog = local_metrics.get('scylla_storage_proxy_replica_view_update_backlog')
            return backlog or None

        backlog_per_row = await wait_for(backlog_nonzero, deadline=time.time() + 10.0)
        logger.info(f"Backlog per row (overhead): {backlog_per_row}")

        await manager.api.disable_injection(servers[1].ip_addr, "never_finish_remote_view_updates")

        # Populate the base table with enough data to exceed concurrency limit
        # The concurrency limit is 128, so 200 rows should be sufficient
        rows = 200
        for i in range(rows):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, '{row_value}')")

        # Flush to create sstables on all replicas
        await asyncio.gather(*(manager.api.keyspace_flush(s.ip_addr, ks) for s in servers))

        # Prepare servers[1] for accepting staging sstables
        await delete_table_sstables(manager, servers[1], ks, "tab")
        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.server_start(servers[1].server_id)

        log_file = await manager.server_open_log(servers[1].server_id)

        await manager.api.enable_injection(servers[1].ip_addr, "never_finish_remote_view_updates", one_shot=False)
        await manager.api.repair(servers[1].ip_addr, ks, "tab")

        await log_file.wait_for(f"view_update_generator - Processing {ks}.tab", timeout=60)

        # Check that view update backlog is bounded (not growing unbounded)
        # The backlog should respect the concurrency limit of 128
        local_metrics = await manager.metrics.query(servers[1].ip_addr)
        view_backlog = local_metrics.get('scylla_storage_proxy_replica_view_update_backlog')

        # With concurrency limit of 128, the backlog should be approximately 128 * backlog_per_row
        assert view_backlog <= backlog_per_row * 128, f"View backlog {view_backlog} exceeds expected limit {backlog_per_row * 128}"

        await manager.api.disable_injection(servers[1].ip_addr, "never_finish_remote_view_updates")

#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest
import time
import asyncio

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


async def get_injection_params(manager, node_ip, injection):
    res = await manager.api.get_injection(node_ip, injection)
    logger.debug(f"get_injection_params({injection}): {res}")
    assert len(res) == 1
    shard_res = res[0]
    assert shard_res["enabled"]
    if "parameters" in shard_res:
        return {item["key"]: item["value"] for item in shard_res["parameters"]}
    else:
        return {}


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_enable_compacting_data_for_streaming_and_repair_live_update(manager):
    """
    Check that enable_compacting_data_for_streaming_and_repair is live_update.
    This config item has a non-trivial path of propagation and live-update was
    silently broken in the past.
    """
    cmdline = ["--enable-compacting-data-for-streaming-and-repair", "0", "--smp", "1", "--logger-log-level", "api=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY)")

    config_item = "enable_compacting_data_for_streaming_and_repair"

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    for host in (host1, host2):
        res = list(cql.execute(f"SELECT value FROM system.config WHERE name = '{config_item}'", host=host))
        assert res[0].value == "false"

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Before the first repair, there should be no parameters present
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {}

    # After the initial repair, we should see the config item value matching the value set via the command-line.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "false"

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '1' WHERE name = '{config_item}'", host=host)

    # After the update to the config above, the next repair should pick up the updated value.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "true"


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tombstone_gc_for_streaming_and_repair(manager):
    """
    Check that:
    * enable_tombstone_gc_for_streaming_and_repair=1 works as expected
    * enable_tombstone_gc_for_streaming_and_repair=0 works as expected
    * enable_tombstone_gc_for_streaming_and_repair is live-update
    """
    cmdline = [
            "--enable-compacting-data-for-streaming-and-repair", "1",
            "--enable-tombstone-gc-for-streaming-and-repair", "1",
            "--enable-cache", "0",
            "--hinted-handoff-enabled", "0",
            "--smp", "1",
            "--logger-log-level", "api=trace:database=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'}")

    await manager.server_stop_gracefully(node2.server_id)

    stmt = SimpleStatement("DELETE FROM ks.tbl WHERE pk = 0 AND ck = 0", consistency_level=ConsistencyLevel.ONE)
    cql.execute(stmt)

    await manager.server_start(node2.server_id, wait_others=1)

    # Flush memtables and remove commitlog, so we can freely GC tombstones.
    await manager.server_restart(node1.server_id, wait_others=1)

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    config_item = "enable_tombstone_gc_for_streaming_and_repair"

    def check_nodes_have_data(node1_has_data, node2_has_data):
        for (host, host_has_data) in ((host1, node1_has_data), (host2, node2_has_data)):
            res = list(cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0", host=host))
            print(res)
            if host_has_data:
                assert len(res) == 3
            else:
                assert len(res) < 3

    # Initial start-condition check
    check_nodes_have_data(True, False)

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Make the tombstone purgeable
    cql.execute("ALTER TABLE ks.tbl WITH tombstone_gc = {'mode': 'immediate'}")

    # With enable_tombstone_gc_for_streaming_and_repair=1, repair
    # should not find any differences and thus not replicate the GCable
    # tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "true"}
    check_nodes_have_data(True, False)

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '0' WHERE name = '{config_item}'", host=host)

    # With enable_tombstone_gc_for_streaming_and_repair=0, repair
    # should find the differences and replicate the GCable tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "false"}
    check_nodes_have_data(True, True)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_succeeds_with_unitialized_bm(manager):
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.enable_injection(servers[1].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized", True, {})

    await manager.api.repair(servers[0].ip_addr, "ks", "tbl")

async def do_batchlog_flush_in_repair(manager, cache_time_in_ms):
    """
    Check that repair batchlog flush handler caches the flush request
    """
    nr_repairs_per_node = 3
    nr_repairs = 2 * nr_repairs_per_node
    total_repair_duration = 0

    cmdline = ["--repair-hints-batchlog-flush-cache-time-in-ms", str(cache_time_in_ms), "--smp", "1", "--logger-log-level", "api=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY) WITH tombstone_gc = {'mode': 'repair'}")

    for node in (node1, node2):
        await manager.api.enable_injection(node.ip_addr, "repair_flush_hints_batchlog_handler", one_shot=False)
        await manager.api.enable_injection(node.ip_addr, "add_delay_to_batch_replay", one_shot=False)

    for node in (node1, node2):
        assert (await get_injection_params(manager, node.ip_addr, "repair_flush_hints_batchlog_handler")) == {}

    async def do_repair(node):
        await manager.api.repair(node.ip_addr, "ks", "tbl")

    async def repair(label):
        start = time.time()
        await asyncio.gather(*(do_repair(node) for x in range(nr_repairs_per_node) for node in [node1, node2]))
        duration = time.time() - start
        params = await get_injection_params(manager, node1.ip_addr, "repair_flush_hints_batchlog_handler")
        logger.debug(f"After {label} repair cache_time_in_ms={cache_time_in_ms} injection_params={params} repair_duration={duration}")
        return (params, duration)

    params, duration = await repair("First")
    total_repair_duration += duration

    await asyncio.sleep(1 + (cache_time_in_ms / 1000))

    params, duration = await repair("Second")
    total_repair_duration += duration

    assert (int(params['issue_flush']) > 0)
    if cache_time_in_ms > 0:
        assert (int(params['skip_flush']) > 0)
    else:
        assert (not 'skip_flush' in params)

    logger.debug(f"Repair nr_repairs={nr_repairs} cache_time_in_ms={cache_time_in_ms} total_repair_duration={total_repair_duration}")

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_with_cache(manager):
    await do_batchlog_flush_in_repair(manager, 5000);

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_without_cache(manager):
    await do_batchlog_flush_in_repair(manager, 0);

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_abort(manager):
    cfg = {'enable_tablets': True}
    await manager.server_add(config=cfg)
    await manager.server_add(config=cfg)
    servers = await manager.running_servers()

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.client.post(f"/task_manager/ttl", params={ "ttl": "100000" },
                                                    host=servers[0].ip_addr)

    await manager.api.enable_injection(servers[0].ip_addr, "repair_tablet_repair_task_impl_run", False, {})

    # Start repair.
    sequence_number = await manager.api.client.post_json(f"/storage_service/repair_async/ks", host=servers[0].ip_addr)

    # Get repair id.
    stats_list = await manager.api.client.get_json("/task_manager/list_module_tasks/repair", host=servers[0].ip_addr)
    ids = [stats["task_id"] for stats in stats_list if stats["sequence_number"] == sequence_number]
    assert len(ids) == 1
    id = ids[0]

    # Abort repair.
    await manager.api.client.post("/storage_service/force_terminate_repair", host=servers[0].ip_addr)

    await manager.api.message_injection(servers[0].ip_addr, "repair_tablet_repair_task_impl_run")
    await manager.api.disable_injection(servers[0].ip_addr, "repair_tablet_repair_task_impl_run")

    # Check if repair was aborted.
    await manager.api.client.get_json(f"/task_manager/wait_task/{id}", host=servers[0].ip_addr)
    statuses = await manager.api.client.get_json(f"/task_manager/task_status_recursive/{id}", host=servers[0].ip_addr)
    assert all([status["state"] == "failed" for status in statuses])

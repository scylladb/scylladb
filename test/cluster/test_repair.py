#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import pytest
import time
import asyncio
import json
import random
import uuid

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace


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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_enable_compacting_data_for_streaming_and_repair_live_update(manager):
    """
    Check that enable_compacting_data_for_streaming_and_repair is live_update.
    This config item has a non-trivial path of propagation and live-update was
    silently broken in the past.
    """
    cmdline = ["--enable-compacting-data-for-streaming-and-repair", "0", "--smp", "1", "--logger-log-level", "api=trace"]
    node1, node2 = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")

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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
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
    node1, node2 = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")

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

    # Disable incremental repair so that the second repair can still work on the repaired data set
    for node in [node1, node2]:
        await manager.api.enable_injection(node.ip_addr, "repair_tablet_no_update_sstables_repair_at", False, {})

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

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_succeeds_with_unitialized_bm(manager):
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
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

    cfg = { 'tablets_mode_for_new_keyspaces': 'disabled' }
    cmdline = ["--repair-hints-batchlog-flush-cache-time-in-ms", str(cache_time_in_ms), "--smp", "1", "--logger-log-level", "api=trace"]
    node1, node2 = await manager.servers_add(2, config=cfg, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY) WITH tombstone_gc = {'mode': 'repair'}")

    for node in (node1, node2):
        await manager.api.enable_injection(node.ip_addr, "repair_flush_hints_batchlog_handler", one_shot=False)

    async def do_repair(node):
        await manager.api.repair(node.ip_addr, "ks", "tbl")

    async def repair(label):
        start = time.time()
        await asyncio.gather(*(do_repair(node) for x in range(nr_repairs_per_node) for node in [node1, node2]))
        duration = time.time() - start
        logger.debug(f"After {label} repair cache_time_in_ms={cache_time_in_ms} repair_duration={duration}")
        return duration

    duration = await repair("First")
    total_repair_duration += duration

    await asyncio.sleep(1 + (cache_time_in_ms / 1000))

    count_before = await manager.api.get_injection_enter_count(node1.ip_addr, "repair_flush_hints_batchlog_handler")
    duration = await repair("Second")
    total_repair_duration += duration
    count_after = await manager.api.get_injection_enter_count(node1.ip_addr, "repair_flush_hints_batchlog_handler")

    # Each repair sends flush requests to all nodes, so node1 receives nr_repairs
    # requests per round. With cache, only the first triggers an actual replay.
    replays_per_round = 1 if cache_time_in_ms > 0 else nr_repairs
    assert count_after - count_before == replays_per_round

    logger.debug(f"Repair nr_repairs={nr_repairs} cache_time_in_ms={cache_time_in_ms} total_repair_duration={total_repair_duration}")

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_with_cache(manager):
    await do_batchlog_flush_in_repair(manager, 5000);

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_batchlog_flush_in_repair_without_cache(manager):
    await do_batchlog_flush_in_repair(manager, 0);

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_keyspace_drop_during_data_sync_repair(manager):
    cfg = {
        'tablets_mode_for_new_keyspaces': 'disabled',
        'error_injections_at_startup': ['get_keyspace_erms_throw_no_such_keyspace']
    }
    await manager.server_add(config=cfg)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.server_add(config=cfg)

async def test_vnode_keyspace_describe_ring(manager: ManagerClient):
    cfg = {
        'tablets_mode_for_new_keyspaces': 'disabled',
    }
    servers = await manager.servers_add(2, config=cfg)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        keys = dict()
        cql = manager.get_cql()
        await cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")
        for i in range(100):
            key = random.randint(-1000000000, 1000000000)
            await cql.run_async(f"INSERT into {ks}.tbl (pk) VALUES({key})")
            token = (await cql.run_async(f"SELECT token(pk) from {ks}.tbl WHERE pk = {key}"))[0].system_token_pk
            keys[key] = token

        res = await manager.api.describe_ring(servers[0].ip_addr, ks)
        end_tokens = dict()
        for item in res:
            end_tokens[int(item['start_token'])] = int(item['end_token'])
            logger.debug(f"{item=}")
        logger.debug("Verifying that the describe_ring result covering the full token ring")
        sorted_tokens = sorted(end_tokens.keys())
        logger.debug(f"{sorted_tokens=}")
        for i in range(1, len(sorted_tokens)):
            assert end_tokens[sorted_tokens[i-1]] == sorted_tokens[i]
        assert end_tokens[sorted_tokens[-1]] == sorted_tokens[0]

        def get_ring_endpoints(token):
            for item in res:
                if int(item['start_token']) < int(item['end_token']):
                    if int(item['start_token']) < token <= int(item['end_token']):
                        return item['endpoints']
                elif token > int(item['start_token']) or token <= int(item['end_token']):
                    return item['endpoints']
            pytest.fail(f"Token {token} not found in describe_ring result")

        cql = manager.get_cql()
        for key, token in keys.items():
            natural_endpoints = await manager.api.natural_endpoints(servers[0].ip_addr, ks, "tbl", key)
            ring_endpoints = get_ring_endpoints(token)
            assert natural_endpoints == ring_endpoints, f"natural_endpoint mismatch describe_ring for {key=} {token=} {natural_endpoints=} {ring_endpoints=}"


async def test_repair_timtestamp_difference(manager):
    cmdline = [ "--smp", "1", "--logger-log-level", "api=trace", "--hinted-handoff-enabled", "0" ]
    node1, node2 = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck UUID, v text, PRIMARY KEY (pk, ck))")

    nodes = [node1, node2]
    host1, host2 = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 30)

    pk = 1
    ck = uuid.uuid1()
    v = 'ze-value'
    original_timestamp = 1000
    update1_timestamp = 2000
    update2_timestamp = 3000

    cql.execute(f"INSERT INTO ks.tbl (pk, ck, v) VALUES ({pk}, {ck}, '{v}') USING TIMESTAMP {original_timestamp}")

    async def write(node, timestamp):
        other_nodes = [n for n in nodes if n != node]

        for other_node in other_nodes:
            await manager.api.enable_injection(other_node.ip_addr, "database_apply", False, parameters={"ks_name": "ks", "cf_name": "tbl", "what": "throw"})

        query = f"UPDATE ks.tbl USING TIMESTAMP {timestamp} SET v = '{v}' WHERE pk = {pk} AND ck = {ck}"
        manager.get_cql().execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE))

        for other_node in other_nodes:
            await manager.api.disable_injection(other_node.ip_addr, "database_apply")

        await manager.api.flush_keyspace(node.ip_addr, "ks")

    await write(node1, update1_timestamp)
    await write(node2, update2_timestamp)

    async def check(expected_timestamps):
        for host, expected_timestamp in expected_timestamps.items():
            rows = list(await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = {pk} AND ck = {ck} AND mutation_source > 'sstable:' ALLOW FILTERING", host=host))
            assert len(rows) == 1
            assert json.loads(rows[0].metadata)['columns']['v']['timestamp'] == expected_timestamp

    logger.info("Checking timestamps before repair")
    await check({host1: update1_timestamp, host2: update2_timestamp})

    await manager.api.repair(node1.ip_addr, "ks", "tbl")

    await asyncio.gather(*[manager.api.keyspace_compaction(node.ip_addr, "ks") for node in nodes])

    logger.info("Checking timestamps after repair")
    await check({host1: update2_timestamp, host2: update2_timestamp})

async def test_small_table_optimization_repair(manager):
    servers = await manager.servers_add(2, auto_rack_dc="dc1")

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND TABLETS = {'enabled': false}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.repair(servers[0].ip_addr, "ks", "tbl", small_table_optimization=True)

    rows = await cql.run_async(f"SELECT * from system.repair_history")
    assert len(rows) == 1


@pytest.mark.parametrize("reason", ["rebuild", "bootstrap", "decommission"])
async def test_small_table_optimization_for_rbno_auto_detect_by_size(manager, reason):
    """Verify that the small table optimization is automatically enabled for a
    user table during repair based node operations based on the table's on-disk
    size, controlled by the small_table_optimization_for_rbno_max_table_size
    config option.

    Two user tables are created without listing any of them in the extra tables
    config option. One table is left empty (below the size threshold) and the
    other is populated and flushed so its on-disk size exceeds the threshold.
    During the node operation the small table must be optimized (single range)
    while the large one must fall back to the regular ranged repair. The decision
    is made by probing the table size on the replicas the operation syncs from,
    so it must hold for the join style operations where the data lives on the
    peers (bootstrap, rebuild) as well as for decommission where the data is
    local to the coordinating node.
    """
    ks = "auto_opt_ks"
    small_tbl = "small_tbl"
    big_tbl = "big_tbl"
    # A tiny threshold so that any flushed sstable exceeds it, while an empty
    # table stays below it.
    config = {
        "enable_small_table_optimization_for_rbno": True,
        "small_table_optimization_for_rbno_max_table_size": 1024,
        # bootstrap and decommission are not repair based by default, so enable
        # them explicitly (rebuild already is).
        "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission",
    }
    cmdline = ["--smp", "1", "--logger-log-level", "repair=info"]
    # decommission removes a node, so start with a spare so that the RF=2
    # keyspace still has enough replicas afterwards.
    initial_nodes = 3 if reason == "decommission" else 2
    servers = await manager.servers_add(initial_nodes, config=config, cmdline=cmdline, auto_rack_dc="dc1")

    cql = manager.get_cql()

    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} AND TABLETS = {{'enabled': false}}")
    cql.execute(f"CREATE TABLE {ks}.{small_tbl} (pk int PRIMARY KEY, v int)")
    cql.execute(f"CREATE TABLE {ks}.{big_tbl} (pk int PRIMARY KEY, v text)")
    # Populate only the big table and flush it so its on-disk size is well above
    # the configured threshold. The small table is left empty (0 bytes on disk)
    # and stays below the threshold.
    for i in range(100):
        cql.execute(f"INSERT INTO {ks}.{big_tbl} (pk, v) VALUES ({i}, '{'x' * 200}')")
    for s in servers:
        await manager.api.keyspace_flush(s.ip_addr, ks)

    # The sync-data repair is coordinated on the node that is joining or leaving,
    # so its log records the per-keyspace and per-table decisions. For bootstrap
    # the coordinator is the newly added node, which does not exist yet, so the
    # log is opened and scanned from the beginning after the node has joined.
    if reason == "bootstrap":
        coordinator = await manager.server_add(config=config, cmdline=cmdline,
                                               property_file={"dc": "dc1", "rack": "rack99"})
        log = await manager.server_open_log(coordinator.server_id)
        mark = None
    else:
        coordinator = servers[-1]
        log = await manager.server_open_log(coordinator.server_id)
        mark = await log.mark()
        if reason == "rebuild":
            await manager.rebuild_node(coordinator.server_id)
        else:
            await manager.decommission_node(coordinator.server_id)

    # The empty small table is auto-detected as small (optimized), the populated
    # big table exceeds the threshold and uses the regular ranged repair.
    await log.wait_for(
        f"small table optimization size check keyspace={ks}, table={small_tbl}, .*small_table_optimization=true",
        from_mark=mark)
    await log.wait_for(
        f"small table optimization size check keyspace={ks}, table={big_tbl}, .*small_table_optimization=false",
        from_mark=mark)
    await log.wait_for(
        f"sync data for keyspace={ks}, status=started, reason={reason}, "
        f"small_table_optimization_tables=1, normal_tables=1",
        from_mark=mark)


async def test_repair_rejects_equal_start_and_end_token(manager):
    """Verify that repair rejects a request where startToken == endToken.
    When start == end, the wrapping range (T, T] covers the full token ring,
    causing an unintended full repair instead of a no-op.
    Reproduces https://scylladb.atlassian.net/browse/CUSTOMER-358
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1")

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND TABLETS = {'enabled': false}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY)")

    token = "1558831538804957103"
    params = {
        "columnFamilies": "tbl",
        "startToken": token,
        "endToken": token,
    }
    with pytest.raises(HTTPError, match="Start and end tokens must be different"):
        await manager.api.client.post_json(f"/storage_service/repair_async/ks",
                                           host=servers[0].ip_addr, params=params)

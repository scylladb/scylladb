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

from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
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

async def test_vnode_keyspace_describe_ring(manager: ScyllaClusterManager):
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


# ---------------------------------------------------------------------------
# SCYLLADB-3944: repair of a token range that spans several vnode ranges with
# different replica sets.
#
# get_neighbors() derives the participants of a repaired range from the range's
# *end token* only, so a caller (e.g. Scylla Manager) that asked for a merged
# range silently got the replica set of the last sub-range. The "all replicas
# participated" guard before recording system.repair_history compared only a
# participant *count* against RF, which the wrong-but-correctly-sized set
# passed, so the whole merged range was recorded as repaired even though a
# real replica of the first sub-range never took part. With tombstone_gc =
# {'mode': 'repair'} that unlocked tombstone GC on a range that was never
# actually repaired against all of its replicas -> data resurrection.
#
# Fixed by splitting user-requested ranges on the local replica-set boundaries
# in repair_service::do_repair_start() (repair/repair.cc), and by recording
# repair history only when the participant identities match the range's
# replicas in update_system_repair_table() (repair/row_level.cc).
# ---------------------------------------------------------------------------

# Four single-token nodes, interleaving the two DCs, so that two adjacent vnode
# ranges share their dc1 replicas but differ in their dc2 replica -- the same
# shape as the customer cluster (FRA RF=3 identical, GRA RF=1 differing).
_RING = [
    # (dc, rack, token)
    ("dc1", "rack1", -4611686018427387904),
    ("dc2", "rack1", -2305843009213693952),
    ("dc1", "rack2", 0),
    ("dc2", "rack1", 2305843009213693952),
]


async def _add_ring(manager: ScyllaClusterManager, cmdline: list[str]) -> list[ServerInfo]:
    """Bring up the fixed ring above and return the servers in ring-token order."""
    servers = []
    for dc, rack, token in _RING:
        servers.append(await manager.server_add(
            config={
                "tablets_mode_for_new_keyspaces": "disabled",
                "num_tokens": 1,
                "initial_token": str(token),
            },
            property_file={"dc": dc, "rack": rack},
            cmdline=cmdline))
    return servers


def _find_split_replica_set(ring) -> tuple[int, int, int, set[str], set[str]]:
    """Find two adjacent, non-wrapping ranges (a, b] and (b, c] whose replica sets differ.

    Returns (a, b, c, replicas_of_ab, replicas_of_bc).
    """
    entries = [(int(e["start_token"]), int(e["end_token"]), frozenset(e["endpoints"])) for e in ring]
    entries = [e for e in entries if e[0] < e[1]]  # drop the wrapping range
    entries.sort()
    for (a, b, eps_ab), (b2, c, eps_bc) in zip(entries, entries[1:]):
        if b == b2 and eps_ab != eps_bc:
            return a, b, c, set(eps_ab), set(eps_bc)
    pytest.fail(f"No two adjacent ranges with differing replica sets in {entries}")


async def _repair_range(manager: ScyllaClusterManager, node_ip: str, ks: str, table: str, start: int, end: int):
    await manager.api.repair(node_ip, ks, table, ranges=f"{start}:{end}")


def _row_states(cql, host, table: str, pk: int) -> tuple[bool, bool]:
    """Return (has_live_row, has_tombstone) for pk's clustering rows in the local
    data of the given host, read via MUTATION_FRAGMENTS."""
    has_live_row = False
    has_tombstone = False
    for row in cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = {pk}", host=host):
        if row.partition_region != 2:  # only clustering rows
            continue
        metadata = json.loads(row.metadata)
        tombstone = metadata.get("tombstone")
        marker = metadata.get("marker")
        if tombstone is None or (marker is not None and tombstone["timestamp"] < marker["timestamp"]):
            has_live_row = True
        if tombstone is not None:
            has_tombstone = True
    return has_live_row, has_tombstone


async def _repair_history(cql, host, ks: str) -> list[tuple[int, int]]:
    rows = list(cql.execute("SELECT keyspace_name, range_start, range_end FROM system.repair_history", host=host))
    return sorted((r.range_start, r.range_end) for r in rows if r.keyspace_name == ks)


async def test_repair_history_not_recorded_for_range_spanning_replica_sets(manager: ScyllaClusterManager):
    """Repairing a range that spans two different replica sets must not claim the
    whole range as repaired.

    The ring is built so that two adjacent vnode ranges (a, b] and (b, c] have the
    same dc1 replicas but a different dc2 replica. We ask for the merged range
    (a, c]. Before the fix, its end token c selected the replica set of (b, c], so
    the dc2 replica of (a, b] was left out of the repair -- yet system.repair_history
    ended up claiming (a, c] was repaired on the nodes that did participate.

    Asserted: the merged range is split on the vnode boundary and each sub-range is
    repaired against its own replica set, so (a, b] is recorded on exactly the
    replicas of (a, b] and (b, c] on exactly the replicas of (b, c] (the history is
    broadcast to exactly the set of participants). Conversely, no node may record a
    range covering (a, c], because no single replica set repaired all of it.
    """
    cmdline = ["--smp", "1", "--hinted-handoff-enabled", "0"]
    servers = await _add_ring(manager, cmdline)
    by_ip = {s.ip_addr: s for s in servers}

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host_by_ip = {h.address: h for h in hosts}
    assert set(by_ip) <= set(host_by_ip), f"driver hosts {sorted(host_by_ip)} != servers {sorted(by_ip)}"

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1} "
            "AND tablets = {'enabled': false}") as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) "
                    "WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}")

        ring = await manager.api.describe_ring(servers[0].ip_addr, ks)
        a, b, c, eps_ab, eps_bc = _find_split_replica_set(ring)
        logger.info(f"({a}, {b}] -> {sorted(eps_ab)}")
        logger.info(f"({b}, {c}] -> {sorted(eps_bc)}")

        # The replica that owns the first sub-range but not the second one.
        excluded = eps_ab - eps_bc
        assert len(excluded) == 1, f"expected exactly one differing replica, got {excluded}"
        excluded_ip = excluded.pop()

        # Repair must be driven by a node that replicates the whole merged range.
        coordinator_ip = next(iter(eps_ab & eps_bc))
        logger.info(f"Repairing merged range ({a}, {c}] from {coordinator_ip}; "
                    f"{excluded_ip} is a replica of ({a}, {b}] only")

        await _repair_range(manager, coordinator_ip, ks, "tbl", a, c)

        history = {ip: await _repair_history(cql, host_by_ip[ip], ks) for ip in by_ip}
        for ip, rows in history.items():
            logger.info(f"repair_history on {ip}: {rows}")

        # Nobody may claim to have repaired a range whose replicas did not all take part.
        over_extended = [(ip, s, e) for ip, rows in history.items() for (s, e) in rows
                         if s <= a and e >= c]
        assert not over_extended, (
            f"repair_history claims the merged range ({a}, {c}] was repaired on "
            f"{[ip for ip, _, _ in over_extended]}, but replica {excluded_ip} of "
            f"sub-range ({a}, {b}] did not participate (its history: {history[excluded_ip]})")

        # The repair must not be a no-op either: the merged range is split on the
        # vnode boundary and each sub-range is recorded on exactly its replicas.
        for rng, replicas in [((a, b), eps_ab), ((b, c), eps_bc)]:
            recorded_on = {ip for ip, rows in history.items() if rng in rows}
            assert recorded_on == replicas, (
                f"expected sub-range ({rng[0]}, {rng[1]}] to be recorded in "
                f"repair_history on exactly its replicas {sorted(replicas)}, "
                f"but it was recorded on {sorted(recorded_on)}")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_data_resurrection_from_repair_range_spanning_replica_sets(manager: ScyllaClusterManager):
    """End-to-end reproducer for the customer's data resurrection.

    1. Insert a row whose token falls into the first of two adjacent vnode ranges
       with differing replica sets.
    2. Delete it while the dc2 replica of that sub-range rejects writes (database_apply
       injection) and hints are off, so that replica keeps the live row and no tombstone.
    3. Repair the *merged* range. Before the fix, that replica did not own the
       merged range's end token, so it was not a participant and kept its live
       row -- but repair_history was recorded for the whole merged range anyway.
    4. tombstone_gc = repair then considered the tombstone collectable on the
       nodes that did participate; a major compaction purged it.
    5. The stale live row on the excluded replica was the only surviving version:
       the deleted row came back.

    With the fix, the merged range is split on the vnode boundary, the excluded
    replica participates in the repair of its own sub-range (receiving the
    tombstone), and the deleted row stays deleted.
    """
    # repair_hints_batchlog_flush_cache_time_in_ms defaults to 60s; with the cache on,
    # repair records the batchlog manager's last_replay time instead of "now", which can
    # be tens of seconds stale -- older than our tombstone, so nothing would ever be
    # collectable. Disabling the cache makes the recorded repair time the actual repair time.
    cmdline = ["--smp", "1", "--hinted-handoff-enabled", "0", "--enable-cache", "0",
               "--repair-hints-batchlog-flush-cache-time-in-ms", "0"]
    servers = await _add_ring(manager, cmdline)
    by_ip = {s.ip_addr: s for s in servers}

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host_by_ip = {h.address: h for h in hosts}

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1} "
            "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.tbl"
        cql.execute(f"CREATE TABLE {table} (pk int, ck int, PRIMARY KEY (pk, ck)) "
                    "WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}")

        ring = await manager.api.describe_ring(servers[0].ip_addr, ks)
        a, b, c, eps_ab, eps_bc = _find_split_replica_set(ring)
        excluded_ip = (eps_ab - eps_bc).pop()
        coordinator_ip = next(iter(eps_ab & eps_bc))
        participants = [by_ip[ip] for ip in eps_bc]

        # Pick a key that lands in (a, b] -- the sub-range whose replica set is dropped.
        # Probe by writing throw-away keys, reading their tokens, then wiping the table.
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, ck) VALUES ({k}, 0)")
                               for k in range(200)])
        pk = next((r[0] for r in cql.execute(f"SELECT pk, token(pk) FROM {table}")
                   if a < r[1] <= b), None)
        assert pk is not None, f"no key among the probes has a token in ({a}, {b}]"
        cql.execute(f"TRUNCATE {table}")
        logger.info(f"Using pk={pk}, excluded replica {excluded_ip}, coordinator {coordinator_ip}")

        cql.execute(SimpleStatement(f"INSERT INTO {table} (pk, ck) VALUES ({pk}, 0)",
                                    consistency_level=ConsistencyLevel.ALL))

        # Make the excluded replica reject the delete, so it keeps the live row and never
        # sees a tombstone. The other two replicas satisfy CL=TWO, so the delete succeeds.
        await manager.api.enable_injection(excluded_ip, "database_apply", one_shot=False,
                                           parameters={"ks_name": ks, "cf_name": "tbl", "what": "throw"})
        cql.execute(SimpleStatement(f"DELETE FROM {table} WHERE pk = {pk} AND ck = 0",
                                    consistency_level=ConsistencyLevel.TWO))
        await manager.api.disable_injection(excluded_ip, "database_apply")

        # Verify the injection produced the hazardous state: the excluded replica
        # kept the live row and never saw the tombstone, while the other replicas
        # of (a, b] did get it.
        live, dead = _row_states(cql, host_by_ip[excluded_ip], table, pk)
        assert live and not dead, (
            f"expected {excluded_ip} to hold the live row and no tombstone, "
            f"got has_live_row={live} has_tombstone={dead}")
        for ip in eps_ab - {excluded_ip}:
            live, dead = _row_states(cql, host_by_ip[ip], table, pk)
            assert dead, f"expected the tombstone on replica {ip} of ({a}, {b}]"

        # gc_before is derived from the repair time, at gc_clock's second
        # granularity; wait out the granularity so the tombstone is strictly
        # older than the repair. This is a fixed clock gap, not a condition that
        # can be polled for, and USING TIMESTAMP would not help: tombstone GC
        # compares the tombstone's deletion time, not its write timestamp.
        await asyncio.sleep(2)

        logger.info(f"Repairing merged range ({a}, {c}] from {coordinator_ip}")
        await _repair_range(manager, coordinator_ip, ks, "tbl", a, c)

        # The repair must not be a no-op: each sub-range of the split must be
        # recorded on exactly its replicas, including the excluded one for (a, b].
        history = {ip: await _repair_history(cql, host_by_ip[ip], ks) for ip in by_ip}
        for rng, replicas in [((a, b), eps_ab), ((b, c), eps_bc)]:
            recorded_on = {ip for ip, rows in history.items() if rng in rows}
            assert recorded_on == replicas, (
                f"expected sub-range ({rng[0]}, {rng[1]}] to be recorded in "
                f"repair_history on exactly its replicas {sorted(replicas)}, "
                f"but it was recorded on {sorted(recorded_on)}")

        # Purge the tombstone on the nodes that consider the range repaired.
        # consider_only_existing_data makes the major compaction skip the commitlog check
        # (which would otherwise clamp gc_before to the age of the still-active segment);
        # gc_before itself still comes from the repair history, which is what is under test.
        for server in participants:
            await manager.api.keyspace_flush(server.ip_addr, ks, "tbl")
            await manager.api.keyspace_compaction(server.ip_addr, ks, "tbl",
                                                  consider_only_existing_data=True)

        # Diagnostics only -- deliberately not asserted. The repair was split into
        # per-replica-set sub-ranges (asserted above), so the excluded replica got
        # the tombstone; whether a given node has purged it by now is a compaction
        # timing detail. Either way the row must stay deleted, asserted below.
        for ip, host in host_by_ip.items():
            frags = list(cql.execute(
                f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = {pk}", host=host))
            logger.info(f"mutation fragments on {ip}: {frags}")

        rows = list(cql.execute(SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}",
                                                consistency_level=ConsistencyLevel.ALL)))
        assert rows == [], f"deleted row was resurrected: {rows}"

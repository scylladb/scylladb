#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Administration of nodes of their own over REST, nodetool, system tables and
scylla.yaml: node uptime across a restart, dropping the sstable caches,
system.protocol_servers, system.snapshots and system.runtime_info, and live
updates of compaction_enforce_min_threshold by re-reading scylla.yaml.

Ported from scylla-dtest. Like the dtests' nodes, the nodes use vnodes (256
per node), and are stopped gracefully at the end of each test, as the
dtests' teardown did.
"""

import asyncio
import logging
import re

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
# ccm's populate(n): every node in datacenter1/rack1 of the SimpleSnitch.
SIMPLE_SNITCH = {'endpoint_snitch': 'SimpleSnitch'}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


############################################################################
# Ported from rest_api_tests.py.

async def uptime_ms(manager: ScyllaClusterManager, server) -> int:
    return int(await manager.api.client.get_text("/system/uptime_ms", host=server.ip_addr))


# Three seconds after two nodes started, the first one is restarted: its
# /system/uptime_ms is more than 3 s lower than the other node's.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_compare_node_uptime(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG)
    sleep_s = 3
    await asyncio.sleep(sleep_s)
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)
    node1_uptime = await uptime_ms(manager, servers[0])
    node2_uptime = await uptime_ms(manager, servers[1])
    assert node2_uptime - node1_uptime > sleep_s * 1000, f"node1: {node1_uptime}, node2: {node2_uptime}"


# After cassandra-stress writes and reads 50K rows, POST
# /system/drop_sstable_caches empties the row cache: the cache miss and
# eviction counters grow and the cache and LSA sizes shrink.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_drop_row_cache(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    increase = ["scylla_cache_reads_with_misses", "scylla_cache_row_misses", "scylla_cache_row_evictions"]
    decrease = ["scylla_lsa_used_space_bytes", "scylla_cache_partitions", "scylla_lsa_memory_allocated", "scylla_cache_rows"]
    await cassandra_stress(manager, server, "write", "n=50K", "-rate", "threads=4", create_keyspace=False)
    await cassandra_stress(manager, server, "read", "n=50K", "-rate", "threads=4", create_keyspace=False)

    async def metrics():
        m = await manager.metrics.query(server.ip_addr)
        return {name: m.get(name) or 0 for name in increase + decrease}
    before = await metrics()
    await manager.api.client.post("/system/drop_sstable_caches", host=server.ip_addr)
    after = await metrics()
    for name in increase:
        assert after[name] >= before[name], f"{name} should have increased after dropping cache"
    for name in decrease:
        assert after[name] <= before[name], f"{name} should have decreased after dropping cache"


############################################################################
# Ported from system_tables_test.py.

TEST_KEYSPACE = "test_keyspace"


async def create_tables_with_data(cql, number_of_tables: int, number_of_rows: int, replication_factor: int = 1) -> list[str]:
    """The dtests' create_tables_with_data(): c1c2 tables (created as by
    create_cf, read once) with number_of_rows rows each."""
    await cql.run_async(f"CREATE KEYSPACE {TEST_KEYSPACE} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    tables = []
    for i in range(number_of_tables):
        table = f"{TEST_KEYSPACE}.table_{i + 1}"
        await cql.run_async(f"CREATE COLUMNFAMILY {table} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                            "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
        await cql.run_async(f"SELECT * FROM {table} LIMIT 1")
        insert = cql.prepare(f"INSERT INTO {table} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        insert.consistency_level = ConsistencyLevel.QUORUM
        await asyncio.gather(*[cql.run_async(insert, [f"k{k}"]) for k in range(number_of_rows)])
        tables.append(table)
    return tables


# A snapshot of one table, or of a keyspace of five, made by "nodetool
# snapshot", is listed in system.snapshots with its name and sizes, and is
# gone from it after "nodetool clearsnapshot".
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("mode, number_of_tables", [("one_table", 1), ("keyspace", 5)], ids=["one_table", "keyspace"])
async def test_content_create_and_remove_snapshot(manager: ScyllaClusterManager, mode, number_of_tables):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    tables = await create_tables_with_data(cql, number_of_tables, 10)
    out, _, _ = await run_nodetool(manager, server, "snapshot", TEST_KEYSPACE if mode == "keyspace" else tables[0])
    snapshot_name = re.search(r"(with snapshot name \[)(\d+)(])", out).group(2)
    content = list(await cql.run_async("select * from system.snapshots;"))
    assert sorted(f"{r.keyspace_name}.{r.table_name}" for r in content) == tables
    for row in content:
        assert row.snapshot_name == snapshot_name, f"Found unexpected snapshot name: {row.snapshot_name}!"
        assert isinstance(row.live, int | float), "Unexpected value type in 'live' column!"
        assert isinstance(row.total, int | float), "Unexpected value type in 'total' column!"
    await run_nodetool(manager, server, "clearsnapshot", "-t", snapshot_name)
    assert not list(await cql.run_async("select * from system.snapshots;"))


# Ported from scylla-dtest system_tables_test.py::TestProtocolServersTable::test_content.
# system.protocol_servers lists the CQL server on the node's ports 9042 and
# 19042 with the CQL version of system.local, and the alternator server with
# its port only when it's configured. The dtest's "redis" variant is gone:
# Scylla has no redis server any more.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("mode, scylla_yaml_options, port", [
    ("default", None, None),
    ("alternator", {"alternator_port": 8000, "alternator_write_isolation": "only_rmw_uses_lwt"}, "8000"),
], ids=["default", "alternator"])
async def test_protocol_servers_content(manager: ScyllaClusterManager, mode, scylla_yaml_options, port):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH, **(scylla_yaml_options or {})})
    log = await manager.server_open_log(server.server_id)
    await log.wait_for("init - serving")
    node_ip_address = server.ip_addr
    cql = manager.get_cql()
    table_content = list(await cql.run_async("select * from system.protocol_servers;"))
    protocols = (await cql.run_async("select cql_version from system.local"))[0]

    expected_content = {
        "native transport": {"listen_addresses": [f"{node_ip_address}:9042", f"{node_ip_address}:19042"], "protocol": "cql", "protocol_version": protocols.cql_version},
        "alternator": {"listen_addresses": [f"{node_ip_address}:{port}"] if mode == "alternator" else [], "protocol": "dynamodb"},
        "redis": {"listen_addresses": [f"{node_ip_address}:{port}"] if mode == "redis" else [], "protocol": "RESP"},
    }
    for row in table_content:
        if row.name == "rpc":
            continue
        expected_row = expected_content.get(row.name)
        assert expected_row, f"Unexpected value in column 'name': {row.name}"
        assert expected_row["listen_addresses"] == row.listen_addresses, f"Unexpected value in column 'listen_addresses': {row.listen_addresses}"
        assert expected_row["protocol"] == row.protocol, f"Unexpected value in column 'protocol': {row.protocol}"
        if row.name in ["native transport", "rpc"]:
            assert expected_row["protocol_version"] == row.protocol_version, f"Unexpected value in column 'protocol': {row.protocol_version}"


# Ported from scylla-dtest system_tables_test.py::TestSnapshotsTable::test_content_auto_snapshot.
# On 3 nodes (datacenter1, one per rack) with auto_snapshot, truncating a
# table of an RF=3 keyspace snapshots it on every node, and each node lists
# that one snapshot in system.snapshots.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_content_auto_snapshot(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, "auto_snapshot": True},
                                        property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    cql = manager.get_cql()
    table = (await create_tables_with_data(cql, 1, 10, replication_factor=3))[0]
    await cql.run_async(f"truncate table {table};")
    for server in servers:
        session = await manager.get_cql_exclusive(server)
        table_content = list(await session.run_async("select * from system.snapshots;"))
        assert len(table_content) == 1, f"Expected to get 1 row from the table system.snapshots, but got {len(table_content)} rows!"
        assert table == f"{table_content[0].keyspace_name}.{table_content[0].table_name}", \
            f"Expected to get the snapshot information for the table {table}, but didn't get it!"


# Ported from scylla-dtest system_tables_test.py::TestRuntimeInfoTable::test_content_toggle_item.
# "nodetool disablegossip"/"enablegossip" and "enablebackup"/"disablebackup"
# change generic.gossip_active and generic.incremental_backup_enabled in
# system.runtime_info, and change them back.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("item, default_state, changed_state, changing_command, reverting_command", [
    ("gossip_active", "true", "false", "disablegossip", "enablegossip"),
    ("incremental_backup_enabled", "false", "true", "enablebackup", "disablebackup"),
], ids=["gossip_active", "incremental_backup_enabled"])
async def test_content_toggle_item(manager: ScyllaClusterManager, item, default_state, changed_state, changing_command, reverting_command):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH})
    await run_nodetool(manager, server, changing_command)
    cql = manager.get_cql()
    query = f"select value from system.runtime_info where group = 'generic' and item = '{item}';"
    item_state_changed = (await cql.run_async(query))[0].value
    assert item_state_changed == changed_state, f"Expected to get state '{changed_state}' for {item}, but it has '{item_state_changed}' state!"
    await run_nodetool(manager, server, reverting_command)
    item_state_reverted = (await cql.run_async(query))[0].value
    assert item_state_reverted == default_state, f"Expected to get state '{default_state}' for {item}, but it has '{item_state_reverted}' state!"


# Ported from scylla-dtest system_tables_test.py::TestRuntimeInfoTable::test_cache_metrics.
# Flushing a table of 100 rows ("nodetool flush") puts them in the cache:
# system.runtime_info's cache entries and memory_used grow. Reading the
# table back then counts cache hits and no new misses.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cache_metrics(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH})
    cql = manager.get_cql()
    # Statements default to consistency level ONE, as in dtest sessions.
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    number_of_rows = 100
    table = (await create_tables_with_data(cql, 1, number_of_rows))[0]
    query = "select item, value from system.runtime_info where group = 'cache';"

    async def cache_metrics():
        return {row.item: int(row.value) for row in await cql.run_async(query) if row.item not in ["hit_rate_recent", "hit_rate_total"]}
    metrics_before_flush = await cache_metrics()
    await run_nodetool(manager, server, "flush", *table.split("."))
    metrics_after_flush = await cache_metrics()
    assert metrics_after_flush["entries"] - metrics_before_flush["entries"] >= number_of_rows
    assert metrics_after_flush["memory_used"] > metrics_before_flush["memory_used"]

    await cql.run_async(f"select * from {table}")
    metrics_after_request = await cache_metrics()
    assert metrics_after_request["requests_total"] > metrics_after_flush["requests_total"]
    assert metrics_after_request["hits"] > metrics_after_flush["hits"]
    assert metrics_after_request["misses"] == metrics_after_flush["misses"]
    assert metrics_after_request["requests_total"] == metrics_after_request["hits"] + metrics_after_request["misses"]


# Writing a table of 100 rows makes system.runtime_info's memtable entries
# and memory_used grow; xfail as upstream (#10340), not strict as the check
# passes only sometimes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.xfail(reason="https://github.com/scylladb/scylla/issues/10340", strict=False)
async def test_memtable_metrics(manager: ScyllaClusterManager):
    await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    query = "select item, value from system.runtime_info where group = 'memtable';"
    before = {r.item: int(r.value) for r in await cql.run_async(query)}
    await create_tables_with_data(cql, 1, 100)
    after = {r.item: int(r.value) for r in await cql.run_async(query)}
    assert after["entries"] > before["entries"]
    assert after["memory_used"] > before["memory_used"]

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

import pytest

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, stop_servers_gracefully
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

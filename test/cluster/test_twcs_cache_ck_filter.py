#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_twcs_ck_filtering_with_cold_cache(manager: ScyllaClusterManager) -> None:
    """A single-row read on a TWCS table, where every sstable passes the
    partition key filter but none of the sstables holding the partition
    passes the clustering key filter, must not make the row cache remember
    the partition as absent. Needs a cold cache, so the node is restarted
    after writing the sstables. Reproduces #8447."""
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        # bloom_filter_fp_chance = 1 makes every sstable pass the partition
        # key filter.
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, PRIMARY KEY (pk, ck)) "
                            "WITH compaction = {'class': 'TimeWindowCompactionStrategy'} AND bloom_filter_fp_chance = 1")
        await cql.run_async(f"INSERT INTO {ks}.t (pk, ck) VALUES (1, 0)")
        await manager.api.flush_all_keyspaces(server.ip_addr)
        await cql.run_async(f"INSERT INTO {ks}.t (pk, ck) VALUES (0, 1)")
        await manager.api.flush_all_keyspaces(server.ip_addr)

        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        # Only the second sstable, which doesn't have partition 1, passes the
        # clustering key filter for ck = 1.
        assert list(await cql.run_async(f"SELECT * FROM {ks}.t WHERE pk = 1 AND ck = 1")) == []
        # If the cache now wrongly believes partition 1 doesn't exist, this
        # returns nothing.
        assert [tuple(r) for r in await cql.run_async(f"SELECT * FROM {ks}.t WHERE pk = 1 AND ck = 0")] == [(1, 0)]

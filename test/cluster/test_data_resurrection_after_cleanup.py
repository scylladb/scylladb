#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error_one_shot
from test.cluster.util import new_test_keyspace, run_nodetool, wait_for_token_ring_and_group0_consistency
from test.cluster.test_compaction import wait_for_compactions

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_data_resurrection_after_cleanup(manager: ScyllaClusterManager):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'table=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int) WITH gc_grace_seconds=0;")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});") for k in keys])

        async def check(expected_keys):
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {table};")
            assert len(rows) == len(expected_keys)
            for r in rows:
                assert r.c == r.pk

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        await check(keys)

        logger.info("Adding new server")
        servers.append(await manager.server_add(cmdline=cmdline))

        time.sleep(1)
        await check(keys)

        await inject_error_one_shot(manager.api, servers[0].ip_addr, "major_compaction_before_cleanup")
        await manager.api.cleanup_keyspace(servers[0].ip_addr, ks)

        deleted_keys = range(128)
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {table} WHERE pk={k};") for k in deleted_keys])
        # Make sures tombstones are gone
        await manager.api.flush_keyspace(servers[1].ip_addr, ks)
        time.sleep(1)
        await manager.api.keyspace_compaction(servers[1].ip_addr, ks)

        # Regains ownership of deleted data

        logger.info(f"Decommissioning node {servers[1]}")
        await manager.decommission_node(servers[1].server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

        time.sleep(1)
        await check(range(128))


# Data written to two nodes (RF=2, racks r1 and r2) and cleaned up after a
# third node joined, then deleted and its tombstones purged (gc_grace_seconds
# 0, major compaction), does not resurrect when the third node is removed
# with "nodetool removenode" (#1239). Like the dtest, the keyspace uses
# vnodes; the table is the dtests' c1c2 table, written at CL=ALL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cluster_cleanup_no_resurrection(manager: ScyllaClusterManager):
    num_keys = 1000
    config = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
    servers = await manager.servers_add(2, config=config, property_file=[{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}])
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE COLUMNFAMILY ks.cf0 (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    await cql.run_async("SELECT * FROM ks.cf0 LIMIT 1")
    insert = cql.prepare("INSERT INTO ks.cf0 (key, c1, c2) VALUES (?, 'value1', 'value2')")
    insert.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(insert, [f"k{k}"]) for k in range(num_keys)])
    await cql.run_async("ALTER TABLE ks.cf0 WITH gc_grace_seconds=0")

    new_server = await manager.server_add(config=config, property_file={"dc": "dc1", "rack": "r1"})
    servers.append(new_server)
    for s in servers:
        await run_nodetool(manager, s, "cleanup", "ks")

    delete = cql.prepare("DELETE FROM ks.cf0 WHERE key=?")
    delete.consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*[cql.run_async(delete, [f"k{k}"]) for k in range(num_keys)])
    query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
    assert (await cql.run_async(query))[0][0] == 0

    await asyncio.sleep(1)
    for s in servers:
        await run_nodetool(manager, s, "compact")
    for s in servers:
        await wait_for_compactions(manager, s)

    host_id = await manager.get_host_id(new_server.server_id)
    await manager.server_stop_gracefully(new_server.server_id)
    await run_nodetool(manager, servers[0], "removenode", str(host_id))
    await run_nodetool(manager, servers[0], "snapshot", "ks", "-t", "after_removenode")
    assert (await cql.run_async(query))[0][0] == 0

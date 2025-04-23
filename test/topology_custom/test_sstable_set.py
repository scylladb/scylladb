# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
import time
import logging
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import create_new_test_keyspace
from test.topology.conftest import skip_mode

logger = logging.getLogger(__name__)

@pytest.mark.parametrize("mode", ['vnode', 'tablet'])
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_partitioned_sstable_set(manager: ManagerClient, mode):
    cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled',
    }

    cmdline = ['--smp=1']
    server = await manager.server_add(config=cfg, cmdline=cmdline)
    await manager.api.disable_tablet_balancing(server.ip_addr)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    if mode == 'tablet':
        ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4};")
    else:
        ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': 'false'};")

    cql.execute(f"""CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH compaction = {{
        'class' : 'IncrementalCompactionStrategy',
        'sstable_size_in_mb' : '0'
    }}""")

    await manager.api.disable_autocompaction(server.ip_addr, ks)

    keys = range(100)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

    # Expect flushed sstables to be stored as unleveled
    await manager.api.enable_injection(server.ip_addr, 'sstable_set_insertion_verification',
                                       one_shot=False,
                                       parameters={'table': 'test', 'expect_unleveled': '1'})

    logger.info("Verifying unsplit sstables are stored correctly in the set")

    await manager.api.keyspace_flush(server.ip_addr, ks, "test")

    await manager.api.disable_injection(server.ip_addr, 'sstable_set_insertion_verification')

    # Split all sstables without verification since they're compacted incrementally
    await manager.api.keyspace_compaction(server.ip_addr, ks, "test")

    # Expect all split sstables are stored as leveled
    await manager.api.enable_injection(server.ip_addr, 'sstable_set_insertion_verification',
                                       one_shot=False,
                                       parameters={'table': 'test', 'expect_unleveled': '0'})

    logger.info("Verifying split sstables are stored correctly in the set")

    await manager.api.keyspace_compaction(server.ip_addr, ks, "test")

    await cql.run_async(f"DROP KEYSPACE {ks}")

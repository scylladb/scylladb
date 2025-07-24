#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace


logger = logging.getLogger(__name__)

# Reproduces issue #23831
# Create a MV while inserts are ongoing to the base table, with propagation delay between shards.
# In the time that the the MV is committed on one shard but not yet on the other, the shard may try
# to generate view updates and apply them on the other shard, which doesn't have the MV table ready yet.
# We want to verify it's handled gracefully without errors.
@pytest.mark.asyncio
async def test_mv_create_during_inserts(manager: ManagerClient):
    cmdline = ['--smp', '2']
    servers = await manager.servers_add(1, cmdline=cmdline)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)")

        stop_writer = asyncio.Event()

        async def do_writes_async():
            i = 0
            while not stop_writer.is_set():
                await cql.run_async(f"INSERT INTO {ks}.test(pk, v) VALUES({i}, {i+1})")
                await asyncio.sleep(0.1)
                i += 1

        writer_task = asyncio.create_task(do_writes_async())

        log = await manager.server_open_log(servers[0].server_id)

        await manager.api.enable_injection(servers[0].ip_addr, "schema_applier_delay_between_commit_on_shards", one_shot=False)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE v IS NOT NULL PRIMARY KEY (v, pk)")

        # Stop the writer and wait for it to complete
        stop_writer.set()
        await writer_task

        matches = await log.grep("Error applying view update")
        assert len(matches) == 0, f"Found errors applying view updates: {matches[0]}"

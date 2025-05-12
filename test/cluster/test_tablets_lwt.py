#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module

from test.cluster.util import unique_name
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_lwt(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    config = {
        'rf_rack_valid_keyspaces': False
    }
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    quorum = len(hosts) // 2 + 1

    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    # We use capital letters to check that the proper quotes are used in paxos store queries.
    ks = unique_name() + '_Test'
    await cql.run_async(f"CREATE KEYSPACE IF NOT EXISTS \"{ks}\" WITH replication={{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets={{'initial': 1}}")

    async def check_paxos_state_table(exists: bool, replicas: int):
        async def query_host(h: Host):
            q = f"""
                SELECT table_name FROM system_schema.tables
                WHERE keyspace_name='{ks}' AND table_name='Test$paxos'
            """
            rows = await cql.run_async(q, host=h)
            return len(rows) > 0
        results = await asyncio.gather(*(query_host(h) for h in hosts))
        return results.count(exists) >= replicas

    # We run the checks several times to check that the base table
    # recreation is handled correctly.
    for i in range(3):
        await cql.run_async(f"CREATE TABLE \"{ks}\".\"Test\" (pk int PRIMARY KEY, c int);")

        await cql.run_async(f"INSERT INTO \"{ks}\".\"Test\" (pk, c) VALUES ({i}, {i}) IF NOT EXISTS")
        # For a Paxos round to succeed, a quorum of replicas must respond.
        # Therefore, the Paxos state table must exist on at least a quorum of replicas,
        # though it doesn't need to exist on all of them.
        # The driver calls wait_for_schema_agreement only for DDL queries,
        # so we need to handle quorums explicitly here.
        assert await check_paxos_state_table(True, quorum)

        await cql.run_async(f"UPDATE \"{ks}\".\"Test\" SET c = {i * 10} WHERE pk = {i} IF c = {i}")

        rows = await cql.run_async(f"SELECT pk, c FROM \"{ks}\".\"Test\";")
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == i
        assert row.c == i * 10

        # TRUNCATE goes through the topology_coordinator and triggers a full topology state
        # reload on replicas. We use this to verify that system.tablets remains in a consistent state—
        # e.g., that no orphan tablet maps remain after dropping the Paxos state tables.
        await cql.run_async(f"TRUNCATE TABLE \"{ks}\".\"Test\"")

        await cql.run_async(f"DROP TABLE \"{ks}\".\"Test\"")
        # The driver implicitly calls wait_for_schema_agreement,
        # so the table must be dropped from all replicas, not just a quorum.
        assert await check_paxos_state_table(False, len(hosts))

    await manager.get_cql().run_async(f"DROP KEYSPACE \"{ks}\"")

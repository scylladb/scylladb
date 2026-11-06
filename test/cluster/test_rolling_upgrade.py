#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
from pathlib import Path

import pytest
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_scylla_2025_1_description, get_scylla_2026_1_description
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


# The oldest release upgrade tests start from, and the last release.
@pytest.fixture(params=[get_scylla_2025_1_description, get_scylla_2026_1_description], ids=["2025.1", "2026.1"])
async def old_version(request, build_mode, internet_dependency_enabled) -> ScyllaVersionDescription:
    return await request.param(build_mode)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.skip_mode(mode='release', reason='dev mode is enough for this test')
@pytest.mark.skip_mode(mode='debug', reason='dev mode is enough for this test')
async def test_rolling_upgrade(manager: ScyllaClusterManager, scylla_binary: Path, old_version: ScyllaVersionDescription):
    """
    Upgrade a 3-node cluster, one node at a time, from an old release to the
    version under test. After every node's upgrade, the rows written so far
    must be readable and new rows writable at QUORUM, so the check runs in
    each mixed-version state as well as on the fully upgraded cluster.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1", version=old_version)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        written = 0

        async def write_and_check(cql):
            nonlocal written
            insert = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")
            insert.consistency_level = ConsistencyLevel.QUORUM
            await asyncio.gather(*[cql.run_async(insert, [k, k]) for k in range(written, written + 100)])
            written += 100
            rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {ks}.t", consistency_level=ConsistencyLevel.QUORUM))
            assert sorted((r.pk, r.v) for r in rows) == [(k, k) for k in range(written)]

        await write_and_check(cql)
        for server in servers:
            logger.info(f"Upgrading {server}")
            await manager.server_change_version(server.server_id, str(scylla_binary))
            cql, _ = await manager.get_ready_cql(servers)
            await write_and_check(cql)

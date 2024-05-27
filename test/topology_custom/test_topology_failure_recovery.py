#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
from test.topology.conftest import skip_mode
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, True) for s in servers]
    await asyncio.gather(*errs)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_drain_failure_during_decommission(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    servers = [await manager.server_add(config=cfg) for _ in range(3)]

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    await inject_error_on(manager, "stream_tablet_fail_on_drain", servers)

    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")

    matches = [await log.grep("raft_topology - rollback.*after decommissioning failure, moving transition state to rollback to normal",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1

    await cql.run_async("DROP KEYSPACE test;")


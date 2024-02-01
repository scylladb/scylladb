#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest
from test.pylib.manager_client import ManagerClient
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_explicit_disabling(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets', 'consistent-topology-changes']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false};")

    res = await cql.run_async("SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'test'")
    assert len(res) == 0, "tablets replication strategy turned on"


@pytest.mark.asyncio
async def test_tablet_change_initial_tablets(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets', 'consistent-topology-changes']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")

    await cql.run_async("ALTER KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2};")
    res = await cql.run_async("SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'test'")
    assert res[0].initial_tablets == 2, "initial_tablets not altered"

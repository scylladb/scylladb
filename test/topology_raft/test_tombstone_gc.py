#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
import pytest

from test.topology.util import new_test_keyspace, new_test_table

logger = logging.getLogger(__name__)

def check_tombstone_gc_mode(cql, table, mode):
    s = list(cql.execute(f"DESC {table}"))[0].create_statement
    assert f"'mode': '{mode}'" in s

def get_expected_tombstone_gc_mode(rf, tablets):
    return "repair" if tablets and rf > 1 else "timeout"

@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc(manager, rf, tablets):
    servers = [await manager.server_add(), await manager.server_add()]
    cql = manager.cql
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(cql, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(cql, keyspace, "p int primary key, x int") as table:
            check_tombstone_gc_mode(cql, table, get_expected_tombstone_gc_mode(rf, tablets))

@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc_does_not_override(manager, rf, tablets):
    servers = [await manager.server_add(), await manager.server_add()]
    cql = manager.cql
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(cql, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(cql, keyspace, "p int primary key, x int", " with tombstone_gc = {'mode': 'disabled'}") as table:
            await cql.run_async(f"ALTER TABLE {table} add y int")
            check_tombstone_gc_mode(cql, table, "disabled")

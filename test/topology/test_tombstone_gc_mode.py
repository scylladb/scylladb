#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
import pytest
import asyncio

from test.topology.util import new_test_keyspace, new_test_table
from cassandra.protocol import ConfigurationException

ksdef_local = "WITH REPLICATION = { 'class': 'LocalStrategy'}"

vnodes_ksdef_rf1 = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } AND TABLETS = {'enabled': false }"
vnodes_ksdef_rf3 = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 } AND TABLETS = {'enabled': false }"

tablets_ksdef_rf1 = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } AND TABLETS = {'enabled': true }"
tablets_ksdef_rf3 = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 } AND TABLETS = {'enabled': true }"


@pytest.mark.asyncio
async def test_tombstone_gc_mode(request, manager: ManagerClient):
    cql = manager.cql

    async def check_tombstone_gc(keyspace, create_mode, expected_mode):
        if create_mode is None:
            extra = ""
        else:
            extra = f"WITH tombstone_gc = {{'mode': '{create_mode}'}}"

        async with new_test_table(manager, keyspace, "p int primary key", extra) as table:
            s = list(cql.execute(f"DESC {table}"))[0].create_statement
            assert f"'mode': '{expected_mode}'" in s

    async with new_test_keyspace(manager, ksdef_local) as keyspace:
        await check_tombstone_gc(keyspace, None, "timeout")

        with pytest.raises(ConfigurationException):
            cql.execute(f"create table {keyspace}.t (pk int primary key) with tombstone_gc = {{'mode': 'repair'}}")

    async with new_test_keyspace(manager, vnodes_ksdef_rf1) as keyspace:
        await check_tombstone_gc(keyspace, None, "timeout")
        await check_tombstone_gc(keyspace, "repair", "repair")

    async with new_test_keyspace(manager, vnodes_ksdef_rf3) as keyspace:
        await check_tombstone_gc(keyspace, None, "timeout")
        await check_tombstone_gc(keyspace, "repair", "repair")

    async with new_test_keyspace(manager, tablets_ksdef_rf1) as keyspace:
        await check_tombstone_gc(keyspace, None, "repair")
        await check_tombstone_gc(keyspace, "repair", "repair")

    async with new_test_keyspace(manager, tablets_ksdef_rf3) as keyspace:
        await check_tombstone_gc(keyspace, None, "repair")
        await check_tombstone_gc(keyspace, "repair", "repair")

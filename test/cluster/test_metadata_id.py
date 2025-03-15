#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest

from test.cluster.util import new_test_keyspace

from test.cluster.conftest import cluster_con
from cassandra.policies import WhiteListRoundRobinPolicy

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_columns(request, manager):
    server = await manager.server_add()

    cql1 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql2 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql3 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        await cql1.run_async(f"CREATE TABLE {ks}.t (pk BIGINT PRIMARY KEY, c INT)")
        await cql1.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (0, 0)")

        prepared = cql1.prepare(f"SELECT * FROM {ks}.t")
        res = await cql1.run_async(prepared)
        assert len(res[0]) == 2, f"Result should have two columns but res[0]={res[0]}"

        prepared2 = cql2.prepare(f"SELECT * FROM {ks}.t")
        res2 = await cql2.run_async(prepared2)
        assert len(res2[0]) == 2, f"Result should have two columns but res2[0]={res2[0]}"

        await cql3.run_async(f"ALTER TABLE {ks}.t ADD c2 INT")
        await cql3.run_async(f"INSERT INTO {ks}.t (pk, c, c2) VALUES (1, 1, 1)")

        res = await cql1.run_async(prepared)
        assert len(res[0]) == 3, f"Result should have 3 columns but res[0]={res[0]}"

        res2 = await cql2.run_async(prepared2)
        assert len(res2[0]) == 3, f"Result should have 3 columns but res2[0]={res2[0]}"

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_types(request, manager):
    server = await manager.server_add()

    cql1 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql2 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql3 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        await cql1.run_async(f"CREATE TABLE {ks}.t (pk BIGINT PRIMARY KEY, c INT)")
        await cql1.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (0, 0)")

        prepared = cql1.prepare(f"SELECT * FROM {ks}.t")
        res = await cql1.run_async(prepared)
        prepared2 = cql2.prepare(f"SELECT * FROM {ks}.t")
        res2 = await cql2.run_async(prepared2)

        await cql3.run_async(f"ALTER TABLE {ks}.t DROP c")
        await cql3.run_async(f"ALTER TABLE {ks}.t ADD c TEXT")
        await cql3.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 'abc')")

        res = await cql1.run_async(prepared)
        assert len(res[0]) == 2, f"Result should have 2 columns but res[0]={res[0]}"

        res2 = await cql2.run_async(prepared2)
        assert len(res2[0]) == 2, f"Result should have 2 columns but res2[0]={res2[0]}"

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_udt(request, manager):
    server = await manager.server_add()

    cql1 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql2 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    cql3 = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        await cql1.run_async(f"CREATE TYPE {ks}.mytype (i1 int, i2 int)")
        await cql1.run_async(f"CREATE TABLE {ks}.t (pk bigint PRIMARY KEY, c mytype)")
        mytypeval = "{i1: 1, i2: 2}"
        await cql1.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (0, {mytypeval})")

        prepared = cql1.prepare(f"SELECT * FROM {ks}.t")
        res = await cql1.run_async(prepared)
        prepared2 = cql2.prepare(f"SELECT * FROM {ks}.t")
        res2 = await cql2.run_async(prepared2)

        await cql3.run_async(f"ALTER TYPE {ks}.mytype ADD i3 INT")
        mytypeval = "{i1: 1, i2: 2, i3: 3}"
        await cql3.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (1, {mytypeval})")

        res = await cql1.run_async(prepared)
        assert "i3" in str(res[0]), f"Result should contain i3 but res[0]={res[0]}"

        res2 = await cql2.run_async(prepared2)
        assert "i3" in str(res2[0]), f"Result should contain i3 but res2[0]={res2[0]}"

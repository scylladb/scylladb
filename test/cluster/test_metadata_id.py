#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest

from test.cluster.util import new_test_keyspace, new_test_table

from test.cluster.conftest import cluster_con
from cassandra.policies import WhiteListRoundRobinPolicy

logger = logging.getLogger(__name__)

async def create_server_and_cqls(manager, cqls_num):
    server = await manager.server_add()

    def create_cluster_connection():
        return cluster_con([server.ip_addr], 9042, False,
            load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    return tuple(create_cluster_connection() for _ in range(cqls_num))

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_columns(manager):
    cql1, cql2, cql3 = await create_server_and_cqls(manager, cqls_num=3)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        async with new_test_table(manager, ks, "pk BIGINT PRIMARY KEY, c INT") as table:
            cql1.execute(f"INSERT INTO {table} (pk, c) VALUES (0, 0)")

            prepared = cql1.prepare(f"SELECT * FROM {table}")
            assert len(cql1.execute(prepared).one()) == 2

            prepared2 = cql2.prepare(f"SELECT * FROM {table}")
            assert len(cql2.execute(prepared2).one()) == 2

            cql3.execute(f"ALTER TABLE {table} ADD c2 INT")
            cql3.execute(f"INSERT INTO {table} (pk, c, c2) VALUES (1, 1, 1)")

            assert len(cql1.execute(prepared).one()) == 3
            assert len(cql2.execute(prepared2).one()) == 3

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_types(manager):
    cql1, cql2, cql3 = await create_server_and_cqls(manager, cqls_num=3)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        async with new_test_table(manager, ks, "pk BIGINT PRIMARY KEY, c INT") as table:
            await cql1.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 0)")

            prepared = cql1.prepare(f"SELECT * FROM {table}")
            cql1.execute(prepared)
            prepared2 = cql2.prepare(f"SELECT * FROM {table}")
            cql2.execute(prepared2)

            cql3.execute(f"ALTER TABLE {table} DROP c")
            cql3.execute(f"ALTER TABLE {table} ADD c TEXT")
            cql3.execute(f"INSERT INTO {table} (pk, c) VALUES (1, 'abc')")

            assert len(cql1.execute(prepared).one()) == 2
            assert len(cql2.execute(prepared2).one()) == 2

@pytest.mark.asyncio
async def test_changed_prepared_statement_metadata_udt(manager):
    cql1, cql2, cql3 = await create_server_and_cqls(manager, cqls_num=3)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        cql1.execute(f"CREATE TYPE {ks}.mytype (i1 int, i2 int)")
        async with new_test_table(manager, ks, "pk BIGINT PRIMARY KEY, c mytype") as table:

            mytypeval = "{i1: 1, i2: 2}"
            cql1.execute(f"INSERT INTO {table} (pk, c) VALUES (0, {mytypeval})")

            prepared = cql1.prepare(f"SELECT * FROM {table}")
            cql1.execute(prepared)
            prepared2 = cql2.prepare(f"SELECT * FROM {table}")
            cql2.execute(prepared2)

            cql3.execute(f"ALTER TYPE {ks}.mytype ADD i3 INT")
            mytypeval = "{i1: 1, i2: 2, i3: 3}"
            cql3.execute(f"INSERT INTO {table} (pk, c) VALUES (1, {mytypeval})")

            assert "i3" in str(cql1.execute(prepared).one())
            assert "i3" in str(cql2.execute(prepared2).one())

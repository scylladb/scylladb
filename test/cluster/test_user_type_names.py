#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for user types with names that are valid only when quoted. A failure
# here can stop group 0 on the node, so each test starts its own node.

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace, reconnect_driver


@pytest.mark.xfail(reason="SCYLLADB-5202: the schema stores these type names unquoted and cannot read them back")
async def test_quoted_user_type_names(manager: ScyllaClusterManager):
    """
    Each name is a valid user type name only when quoted: a reserved type
    name, native type names, and a keyword that is not a type. Columns of
    these types, frozen or not, must keep their type when the schema is
    applied and when it is loaded again on restart.
    """
    names = ['byte', 'int', 'empty', 'ttl']
    server = await manager.server_add()
    cql, _ = await manager.get_ready_cql([server])
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        columns = {}
        for name in names:
            await cql.run_async(f'CREATE TYPE {ks}."{name}" (a int)')
            columns[f'f_{name}'] = f'frozen<"{name}">'
            columns[f'n_{name}'] = f'"{name}"'
        column_defs = ', '.join(f'{column} {type}' for column, type in columns.items())
        await cql.run_async(f'CREATE TABLE {ks}.t (p int PRIMARY KEY, {column_defs})')
        values = ', '.join('{a: 1}' for _ in columns)
        await cql.run_async(f'INSERT INTO {ks}.t (p, {", ".join(columns)}) VALUES (0, {values})')

        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)
        row = (await cql.run_async(f'SELECT * FROM {ks}.t'))[0]
        for column in columns:
            assert getattr(row, column).a == 1

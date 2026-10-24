#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for user types with names that are valid only when quoted. A failure
# here can stop group 0 on the node, so each test starts its own node.

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace, reconnect_driver


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


async def test_unquoted_user_type_names_from_older_versions(manager: ScyllaClusterManager):
    """
    Older versions stored these names without quotes. The node stopped group
    0 and, after a restart, skipped all tables of the keyspace. The node must
    load such a schema and read the names as the user types they are. A bare
    native type name like int is not tested: it is read as the native type,
    which the column already had in older versions.
    """
    server = await manager.server_add()
    cql, _ = await manager.get_ready_cql([server])
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        # Use the types frozen, in a vector, not frozen, and inside another type.
        columns = {}
        for name in ['byte', 'int', 'varchar', 'ttl']:
            await cql.run_async(f'CREATE TYPE {ks}."{name}" (a int)')
            columns[f'f_{name}'] = (f'frozen<"{name}">', '{a: 1}')
            columns[f'v_{name}'] = (f'vector<frozen<"{name}">, 1>', '[{a: 1}]')
        for name in ['byte', 'ttl']:
            columns[f'n_{name}'] = (f'"{name}"', '{a: 1}')
        await cql.run_async(f'CREATE TYPE {ks}.outer (f frozen<"byte">)')
        columns['o'] = ('frozen<outer>', '{f: {a: 1}}')
        column_defs = ', '.join(f'{column} {type}' for column, (type, _) in columns.items())
        await cql.run_async(f'CREATE TABLE {ks}.t (p int PRIMARY KEY, {column_defs})')
        values = ', '.join(value for _, value in columns.values())
        await cql.run_async(f'INSERT INTO {ks}.t (p, {", ".join(columns)}) VALUES (0, {values})')

        # Save what DESCRIBE and SELECT return, to compare after the restart.
        described = sorted(row.create_statement for row in await cql.run_async(f'DESCRIBE KEYSPACE {ks}'))
        # Only DESCRIBE checks the vector columns: SELECT JSON fails on a vector of user types.
        selected = ', '.join(column for column in columns if not column.startswith('v_'))
        rows = await cql.run_async(f'SELECT JSON {selected} FROM {ks}.t')

        # Store the types without quotes, the way older versions did.
        for row in await cql.run_async(f"SELECT table_name, column_name, type FROM system_schema.columns WHERE keyspace_name = '{ks}'"):
            old_type = row.type.replace('"', '')
            await cql.run_async(f"UPDATE system_schema.columns SET type = '{old_type}' WHERE keyspace_name = '{ks}' AND table_name = '{row.table_name}' AND column_name = '{row.column_name}'")
        for row in await cql.run_async(f"SELECT type_name, field_types FROM system_schema.types WHERE keyspace_name = '{ks}'"):
            old_types = ', '.join(f"'{type.replace('"', '')}'" for type in row.field_types)
            await cql.run_async(f"UPDATE system_schema.types SET field_types = [{old_types}] WHERE keyspace_name = '{ks}' AND type_name = '{row.type_name}'")

        # After the restart, DESCRIBE and SELECT return the same, and new schema changes work.
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)
        assert sorted(row.create_statement for row in await cql.run_async(f'DESCRIBE KEYSPACE {ks}')) == described
        assert await cql.run_async(f'SELECT JSON {selected} FROM {ks}.t') == rows
        await cql.run_async(f'CREATE TABLE {ks}.t2 (p int PRIMARY KEY)')

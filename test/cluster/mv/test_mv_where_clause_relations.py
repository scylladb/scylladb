#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging

from test.cluster.util import new_materialized_view, new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_view

logger = logging.getLogger(__name__)

# The number of relations in the view's WHERE clause, above the default
# max_relations_in_where_clause of 100.
RELATIONS = 110


def table_and_view_definition():
    """The key columns, the table schema and the view WHERE clause of a table
    whose every key column takes one relation in the view's WHERE clause."""
    clustering_columns = [f"c{i}" for i in range(RELATIONS - 1)]
    key_columns = ["p"] + clustering_columns
    schema = ", ".join([f"{c} int" for c in key_columns] + ["v int", f"PRIMARY KEY ({', '.join(key_columns)})"])
    where = " AND ".join(f"{c} IS NOT NULL" for c in key_columns)
    return key_columns, schema, where


def prepare_insert(cql, table, key_columns):
    return cql.prepare(f"INSERT INTO {table} ({', '.join(key_columns)}, v) VALUES ({', '.join(['?'] * (len(key_columns) + 1))})")


# Regression test for SCYLLADB-3583.
async def test_view_where_clause_above_default_relation_limit(manager: ManagerClient):
    """A view whose WHERE clause has more relations than the default limit is
    usable, no matter what the limit was when it was created. Scylla reparses
    the stored WHERE clause internally, and that reparse must not be subject to
    the client-facing limit."""
    await manager.server_add(config={'max_relations_in_where_clause': RELATIONS * 2})
    cql = manager.get_cql()

    key_columns, schema, where = table_and_view_definition()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, schema) as table:
            insert = prepare_insert(cql, table, key_columns)
            logger.info("writing a row before the view exists, to be picked up by the view builder")
            await cql.run_async(insert, list(range(len(key_columns))) + [0])

            async with new_materialized_view(manager, table, '*', ', '.join(key_columns), where) as mv:
                logger.info("writing a row while the view exists, generating a view update")
                await cql.run_async(insert, [1] + list(range(1, len(key_columns))) + [1])

                await wait_for_view(cql, mv.split('.')[1], 1)
                rows = await cql.run_async(f"SELECT v FROM {mv}")
                assert sorted(row.v for row in rows) == [0, 1]


# Regression test for SCYLLADB-3583.
async def test_rename_base_column_of_view_above_default_relation_limit(manager: ManagerClient):
    """Renaming a base column rewrites the stored WHERE clause of every view
    referring to it, which means reparsing that clause. Just like the reparse
    done for the view's select statement, it must not be subject to the
    client-facing relation limit."""
    await manager.server_add(config={'max_relations_in_where_clause': RELATIONS * 2})
    cql = manager.get_cql()

    key_columns, schema, where = table_and_view_definition()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, schema) as table:
            async with new_materialized_view(manager, table, '*', ', '.join(key_columns), where) as mv:
                view_name = mv.split('.')[1]
                await wait_for_view(cql, view_name, 1)

                logger.info("renaming a base clustering column which the view's WHERE clause refers to")
                await cql.run_async(f"ALTER TABLE {table} RENAME c1 TO c1_renamed")

                rows = await cql.run_async(f"SELECT where_clause FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = '{view_name}'")
                new_where = rows[0].where_clause.lower()
                assert "c1_renamed is not null" in new_where
                assert "c1 is not null" not in new_where

                renamed_columns = ["c1_renamed" if c == "c1" else c for c in key_columns]
                logger.info("writing a row after the rename, generating a view update against the rewritten WHERE clause")
                await cql.run_async(prepare_insert(cql, table, renamed_columns), list(range(len(key_columns))) + [0])

                rows = await cql.run_async(f"SELECT v FROM {mv}")
                assert [row.v for row in rows] == [0]

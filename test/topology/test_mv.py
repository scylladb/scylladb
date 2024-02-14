#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests for materialized views that need a three-node cluster.
Note that most materialized-view functional tests can make due with a single
node, and belong cql-pytest. We also have topology_experimental_raft/
test_mv_tablets.py for tests that each needs a cluster of a different
size, and/or special Scylla parameters.
"""
import asyncio
import logging
import pytest

from contextlib import asynccontextmanager
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)

@asynccontextmanager
async def new_test_keyspace(cql, opts):
    """
    A utility function for creating a new temporary keyspace with given
    options. It can be used in a "async with", as:
        async with new_test_keyspace(cql, '...') as keyspace:
    """
    keyspace = unique_name()
    await cql.run_async("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        await cql.run_async("DROP KEYSPACE " + keyspace)

previously_used_table_names = []
@asynccontextmanager
async def new_test_table(cql, keyspace, schema, extra=""):
    """
    A utility function for creating a new temporary table with a given schema.
    Because Scylla becomes slower when a huge number of uniquely-named tables
    are created and deleted (see https://github.com/scylladb/scylla/issues/7620)
    we keep here a list of previously used but now deleted table names, and
    reuse one of these names when possible.
    This function can be used in a "async with", as:
       async with create_table(cql, test_keyspace, '...') as table:
    """
    global previously_used_table_names
    if not previously_used_table_names:
        previously_used_table_names.append(unique_name())
    table_name = previously_used_table_names.pop()
    table = keyspace + "." + table_name
    await cql.run_async("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        await cql.run_async("DROP TABLE " + table)
        previously_used_table_names.append(table_name)

@asynccontextmanager
async def new_materialized_view(cql, table, select, pk, where, extra=""):
    """
    A utility function for creating a new temporary materialized view in
    an existing table.
    """
    keyspace = table.split('.')[0]
    mv = keyspace + "." + unique_name()
    await cql.run_async(f"CREATE MATERIALIZED VIEW {mv} AS SELECT {select} FROM {table} WHERE {where} PRIMARY KEY ({pk}) {extra}")
    try:
        yield mv
    finally:
        await cql.run_async(f"DROP MATERIALIZED VIEW {mv}")

@pytest.mark.asyncio
async def test_mv_tombstone_gc_setting(manager):
    """
    Test that the tombstone_gc parameter can be set on a materialized view,
    during CREATE MATERIALIZED VIEW and later with ALTER MATERIALIZED VIEW.
    Note that the test only tests the ability to set this parameter - not
    its actual function after it's set.
    The tombstone_gc feature is Scylla-only, so the test is scylla_only.

    tombstone_gc=repair is not supported on a table with RF=1, and RF>1
    is not supported on a single node, which is why this test needs to
    be here and not in the single-node cql-pytest.
    """
    cql = manager.cql
    async with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }") as keyspace:
        async with new_test_table(cql, keyspace, "p int primary key, x int") as table:
            # Adding "WITH tombstone_gc = ..." In the CREATE MATERIALIZED VIEW:
            async with new_materialized_view(cql, table, "*", "p, x", "p is not null and x is not null", "WITH tombstone_gc = {'mode': 'repair'}") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert "'mode': 'repair'" in s
            # Adding "WITH tombstone_gc = ..." In the ALTER MATERIALIZED VIEW:
            async with new_materialized_view(cql, table, "*", "p, x", "p is not null and x is not null") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert not "'mode': 'repair'" in s
                await cql.run_async("ALTER MATERIALIZED VIEW " + mv + " WITH tombstone_gc = {'mode': 'repair'}")
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert "'mode': 'repair'" in s

@pytest.mark.asyncio
async def test_mv_tombstone_gc_not_inherited(manager):
    """
    Test that the tombstone_gc parameter set on a base table is NOT inherited
    by a view created on it later.
    This behavior is not explicitly documented anywhere, but this test
    demonstrates the existing behavior.
    """
    cql = manager.cql
    async with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }") as keyspace:
        async with new_test_table(cql, keyspace, "p int primary key, x int", "WITH tombstone_gc = {'mode': 'repair'}") as table:
            s = list(cql.execute(f"DESC {table}"))[0].create_statement
            assert "'mode': 'repair'" in s
            async with new_materialized_view(cql, table, "*", "p, x", "p is not null and x is not null") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                # Base's setting is NOT inherited to the view:
                assert not "'mode': 'repair'" in s

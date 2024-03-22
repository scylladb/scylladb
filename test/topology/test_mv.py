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

from test.topology.util import new_test_keyspace, new_test_table, new_materialized_view

logger = logging.getLogger(__name__)

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

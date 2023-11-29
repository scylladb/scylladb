#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# Tests for interaction of materialized views with *tablets*

from test.pylib.manager_client import ManagerClient

import pytest
import asyncio
import logging


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_mv_create(manager: ManagerClient):
    """A basic test for creating a materialized view on a table stored
       with tablets on a one-node cluster. We just create the view and
       delete it - that's it, we don't read or write the table.
       Reproduces issue #16194.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")
    await cql.run_async("DROP KEYSPACE test")


@pytest.mark.asyncio
async def test_tablet_mv_simple(manager: ManagerClient):
    """A simple test for reading and writing a materialized view on a table
       stored with tablets on a one-node cluster. Because it's a one-node
       cluster, we don't don't need any sophisticated mappings or pairings
       to work correctly for this test to pass - everything is on this single
       node anyway.
       Reproduces issue #16209.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")
    await cql.run_async("INSERT INTO test.test (pk, c) VALUES (2, 3)")
    # We used SYNCHRONOUS_UPDATES=TRUE, so the view should be updated:
    assert [(3,2)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=3"))
    await cql.run_async("DROP KEYSPACE test")

@pytest.mark.asyncio
async def test_tablet_mv_simple_6node(manager: ManagerClient):
    """A simple reproducer for a bug of forgetting that the view table has a
       different tablet mapping from the base: Using the wrong tablet mapping
       for the base table or view table can cause us to send a view update
       to the wrong view replica - or not send a view update at all. A row
       that we write on the base table will not be readable in the view.
       We start a large-enough cluster (6 nodes) to increase the probability
       that if the mapping is different for the one row we write, and the test
       will fail if the bug exists.
       Reproduces #16227.
    """
    servers = await manager.servers_add(6)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")
    await cql.run_async("INSERT INTO test.test (pk, c) VALUES (2, 3)")
    # We used SYNCHRONOUS_UPDATES=TRUE, so the view should be updated:
    assert [(3,2)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=3"))
    await cql.run_async("DROP KEYSPACE test")

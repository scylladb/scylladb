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
    servers = [await manager.server_add() for i in range(1)]
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")
    await cql.run_async("DROP KEYSPACE test")

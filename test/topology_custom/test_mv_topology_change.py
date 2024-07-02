#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
import time
import logging
import requests
import re

from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode
from test.pylib.util import wait_for


logger = logging.getLogger(__name__)

# Reproduces issue #19529
# Write to a table with MV while one node is stopped, and verify
# it doesn't cause MV write timeouts or preventing topology changes.
# The writes that are targeted to the stopped node are with CL=ANY so
# they should store a hint and then complete successfuly.
# If the MV write handler is not completed after storing the hint, as in
# issue #19529, it remains active until it timeouts, preventing topology changes
# during this time.
@pytest.mark.asyncio
async def test_mv_write_to_dead_node(manager: ManagerClient):
    servers = [await manager.server_add() for _ in range(4)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    await manager.server_stop_gracefully(servers[-1].server_id)

    # Do inserts. some should generate MV writes to the stopped node
    for i in range(100):
        await cql.run_async(f"insert into ks.t (pk, v) values ({i}, {i+1})")

    # Remove the node to trigger a topology change.
    # If the MV write is not completed, as in issue #19529, the topology change
    # will be held for long time until the write timeouts.
    # Otherwise, it is expected to complete in short time.
    await manager.remove_node(servers[0].server_id, servers[-1].server_id)

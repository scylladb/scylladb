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
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)

# Write with RF=1 and CL=ANY to a dead node should write hints and succeed
@pytest.mark.asyncio
async def test_write_cl_any_to_dead_node_generates_hints(manager: ManagerClient):
    node_count = 2
    servers = [await manager.server_add() for _ in range(node_count)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    await manager.server_stop_gracefully(servers[1].server_id)

    def get_hints_written_count(server):
        c = 0
        metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
        pattern = re.compile("^scylla_hints_manager_written")
        for metric in metrics.split('\n'):
            if pattern.match(metric) is not None:
                c += int(float(metric.split()[1]))
        return c

    hints_before = get_hints_written_count(servers[0])

    # Some of the inserts will be targeted to the dead node.
    # The coordinator doesn't have live targets to send the write to, but it should write a hint.
    for i in range(100):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i+1})", consistency_level=ConsistencyLevel.ANY))

    # Verify hints are written
    hints_after = get_hints_written_count(servers[0])
    assert hints_after > hints_before

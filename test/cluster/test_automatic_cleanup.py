#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_no_cleanup_when_unnecessary(manager: ManagerClient):
    """The test runs two bootstraps and checks that there is no cleanup in between.
       Then it runs a decommission and checks that cleanup runs automatically and then
       it runs one more decommission and checks that no cleanup runs again.
       Second part checks manual cleanup triggering. It adds a node. Triggers cleanup
       through the REST API, checks that is runs, decommissions a node and check that the
       cleanup did not run again.
    """
    logger.info("start first server")
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack1"})

    logger.info("start another two servers")
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack2"})
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack3"})
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    servers = await manager.running_servers()
    host_id_2  = await manager.get_host_id(servers[2].server_id)
    logger.info(f"decommission {servers[2].server_id}")
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[2].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark)
               for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 2
    coordinator_log_matches = await logs[0].grep(
        f"vnodes cleanup required by 'leave' of the node {host_id_2}: running global_token_metadata_barrier",
        from_mark=marks[0])
    assert len(coordinator_log_matches) == 1

    servers = await manager.running_servers()
    logger.info(f"decommission {servers[1].server_id}")
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[1].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    logger.info("add another server")
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack4"})
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.cleanup_all(servers[0].ip_addr)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1

    logger.info(f"decommission {servers[1].server_id}")
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[1].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0


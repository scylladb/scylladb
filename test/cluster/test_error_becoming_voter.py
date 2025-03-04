#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest
import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_error_while_becoming_voter(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    """
    Test that a node is starting successfully if while joining a cluster and becoming a voter, it
    receives an unknown commit status error.
    Issue https://github.com/scylladb/scylladb/issues/20814

    1. Create a new cluster, start 2 nodes normally.
    2. Run one node with error injection for throwing an exception commit_status_unknown in modify_config,
       so that after bootstrapping the node would get a commit_status_unknown error.
    3. Make sure the node was started successfully. In case the error with the handling of commit_status_unknown is
       not handled properly, the node will fail to start.

    """
    logger.info("Creating a new cluster")
    await manager.servers_add(2)

    srv = await manager.server_add(config={
        "error_injections_at_startup": [
            {"name": "raft/throw_commit_status_unknown_in_modify_config", "one_shot": True}
        ]
    })
    await manager.server_start(srv.server_id)

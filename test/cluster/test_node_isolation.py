#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest

from cassandra.cluster import ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT  # type: ignore
from cassandra.cluster import NoHostAvailable, OperationTimedOut  # type: ignore
from cassandra.cluster import Cluster  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.cluster.util import create_new_test_keyspace


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.nightly
async def test_banned_node_notification(manager: ManagerClient) -> None:
    """Test that a node banned from the cluster get notification about been banned"""
    # Decrease the failure detector threshold so we don't have to wait for too long.
    config = {
        'failure_detector_timeout_in_ms': 2000
    }
    srvs = await manager.servers_add(3, config=config)
    cql = manager.get_cql()

    # Pause one of the servers so other nodes mark it as dead and we can remove it.
    # We deliberately don't shut it down, but only pause it - we want to test
    # that we solved the harder problem of safely removing nodes which didn't shut down.
    logger.info(f"Pausing server {srvs[2]}")
    await manager.server_pause(srvs[2].server_id)
    logger.info(f"Removing {srvs[2]} using {srvs[0]}")
    await manager.remove_node(srvs[0].server_id, srvs[2].server_id)
    # Perform a read barrier on srvs[1] so it learns about the ban.
    logger.info(f"Performing read barrier on server {srvs[1]}")
    await read_barrier(manager.api, srvs[1].ip_addr)
    logger.info(f"Unpausing {srvs[2]}")
    await manager.server_unpause(srvs[2].server_id)

    log = await manager.server_open_log(srvs[2].server_id)
    _, matches = await log.wait_for(f"received notification of being banned from the cluster from")

    # check that the node is notified about being banned
    assert len(matches) != 0, "The node did not log being banned from the cluster"
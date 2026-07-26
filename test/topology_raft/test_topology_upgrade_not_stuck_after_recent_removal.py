#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode
from test.topology.util import wait_until_topology_upgrade_finishes, \
        wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_topology_upgrade_not_stuck_after_recent_removal(request, manager: ManagerClient):
    """
    Regression test for https://github.com/scylladb/scylladb/issues/18198.
    1. Create a two node cluster in legacy mode
    2. Remove one of the nodes
    3. Upgrade the cluster to raft topology.
    4. Verify that the upgrade went OK and it did not get stuck.
    """
    # First, force the nodes to start in legacy mode due to the error injection
    cfg = {'force_gossip_topology_changes': True}

    logging.info("Creating a two node cluster")
    servers = [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.cql
    assert(cql)

    srv1, srv2 = servers

    logging.info("Removing the second node in the cluster")
    await manager.decommission_node(srv2.server_id)

    logging.info("Waiting until driver connects to the only server")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]

    logging.info("Checking the upgrade state")
    status = await manager.api.raft_topology_upgrade_status(host1.address)
    assert status == "not_upgraded"
    
    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(host1.address)

    logging.info("Waiting until upgrade finishes")
    await wait_until_topology_upgrade_finishes(manager, host1.address, time.time() + 60)

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, [host1], time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, [host1])

#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import asyncio
import pytest
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for
from test.pylib.rest_client import inject_error_one_shot
import time
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_wait_for_ip_persistence(manager: ManagerClient):
    logger.info("starting initial cluster with two nodes")
    s1 = await manager.server_add()
    s2 = await manager.server_add()
    logger.info(f"cluster started, topology coordinator {s1}, other node {s2}")

    logger.info(f"adding new stopped server")
    new_server = await manager.server_add(start=False)
    logger.info(f"server {new_server} added")

    logger.info(f"add injection to {s1}")
    s1_injection_handler = await inject_error_one_shot(manager.api, s1.ip_addr,
                                                       "topology_coordinator_after_wait_for_ip",
                                                       parameters={ "target_node_ip": new_server.ip_addr })

    logger.info(f"open log file of the topology coordinator node {s1}")
    s1_log_file = await manager.server_open_log(s1.server_id)

    logger.info(f"starting new server in the background")
    new_server_start_future = asyncio.create_task(manager.server_start(new_server.server_id))

    logger.info(f"waiting for topology coordinator {s1} to finish wait_for_ip")
    await s1_log_file.wait_for("topology_coordinator_after_wait_for_ip")

    logger.info(f"kill the second server {s2}")
    await manager.server_stop(s2.server_id)

    logger.info(f"restarting second server {s2}")
    await manager.server_start(s2.server_id)

    cql = manager.get_cql()
    logger.info(f"waiting for hosts on {s2}")
    host2 = (await wait_for_cql_and_get_hosts(cql, [s2], time.time() + 60))[0]
    peers_to_see = {s1.ip_addr, new_server.ip_addr}
    logger.info(f"waiting for {s2} to see its peers")
    async def see_peers():
        peers = set([r.peer for r in await cql.run_async("select peer from system.peers", host=host2)])
        remaining = peers_to_see - peers
        if not remaining:
            return True
        logger.info(f"waiting for {host2} to see its peers, peers_to_see {peers_to_see}, peers {peers}, remaining {remaining}")
    await wait_for(see_peers, time.time() + 60)

    logger.info(f"release the topology coordinator {s1}")
    await s1_injection_handler.message()

    logger.info(f"waiting for new server {new_server} to start")
    await new_server_start_future

#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging

import pytest
from cassandra.protocol import InvalidRequest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.topology.conftest import skip_mode

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_alter_dropped_tablets_keyspace(manager: ManagerClient) -> None:
    config = {
        'enable_tablets': 'true'
    }

    logger.info("starting a node (the leader)")
    servers = [await manager.server_add(config=config)]

    logger.info("starting a second node (the follower)")
    servers += [await manager.server_add(config=config)]

    await manager.get_cql().run_async("create keyspace ks with "
                                      "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} and "
                                      "tablets = {'enabled': true}")
    await manager.get_cql().run_async("create table ks.t (pk int primary key)")

    logger.info(f"injecting wait-after-topology-coordinator-gets-event into the leader node {servers[0]}")
    injection_handler = await inject_error_one_shot(manager.api, servers[0].ip_addr,
                                                    'wait-after-topology-coordinator-gets-event')

    async def alter_tablets_ks_without_waiting_to_complete():
        res = await manager.get_cql().run_async("select data_center from system.local")
        # ALTER tablets KS only accepts a specific DC, it rejects the generic 'replication_factor' tag
        this_dc = res[0].data_center
        await manager.get_cql().run_async("alter keyspace ks "
                                    f"with replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 1}}")

    # by creating a task this way we ensure it's immediately executed, but we won't wait until it's completed
    task = asyncio.create_task(alter_tablets_ks_without_waiting_to_complete())

    logger.info(f"waiting for the leader node {servers[0]} to start handling the keyspace-rf-change request")
    leader_log_file = await manager.server_open_log(servers[0].server_id)
    await leader_log_file.wait_for("wait-after-topology-coordinator-gets-event injection hit", timeout=10)

    logger.info(f"dropping KS from the follower node {servers[1]} so that the leader, which hangs on injected sleep, "
                f"wakes up with the drop applied")
    host = manager.get_cql().cluster.metadata.get_host(servers[1].ip_addr)
    await manager.get_cql().run_async("drop keyspace ks", host=host)

    logger.info("Waking up the leader to continue processing ALTER with KS that doesn't exist (has been just dropped)")
    await injection_handler.message()

    matches = await leader_log_file.grep("topology change coordinator fiber got error "
                                         "data_dictionary::no_such_keyspace \(Can't find a keyspace ks\)")
    assert not matches

    with pytest.raises(InvalidRequest, match="Can't ALTER keyspace ks, keyspace doesn't exist") as e:
        await task

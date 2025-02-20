#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import log_run_time, wait_until_last_generation_is_in_use, wait_until_topology_upgrade_finishes, \
        wait_for_cdc_generations_publishing, check_system_topology_and_cdc_generations_v3_consistency, \
        start_writes_to_cdc_table


@pytest.mark.asyncio
@log_run_time
async def test_topology_upgrade_basic(request, build_mode: str, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {
        'force_gossip_topology_changes': True,
        'enable_tablets': False,
        'ring_delay_ms': 15000 if build_mode == 'debug' else 5000,
    }

    servers = [await manager.server_add(config=cfg)]
    # Enable raft-based node operations for subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['force_gossip_topology_changes']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    _, stop_writes_and_verify = await start_writes_to_cdc_table(cql)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Check that triggering upgrade is idempotent")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logging.info("Booting new node")
    servers.append(await manager.server_add(config=cfg))

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the new CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    await wait_until_last_generation_is_in_use(cql)

    logging.debug("Sleeping for 1 second to make sure there are writes to the CDC table in the last generation")
    await asyncio.sleep(1)

    logging.info("Checking correctness of data in system_distributed.cdc_streams_descriptions_v2")
    await stop_writes_and_verify()

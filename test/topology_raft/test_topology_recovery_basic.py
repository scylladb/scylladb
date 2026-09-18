#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql
from test.topology.util import enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency, start_writes_to_cdc_table, wait_until_last_generation_is_in_use
from test.topology.conftest import cluster_con


@pytest.mark.asyncio
@log_run_time
async def test_topology_recovery_basic(request, mode: str, manager: ManagerClient):
    # Increase ring delay to ensure nodes learn about CDC generations before they start operating.
    ring_delay = 15000 if mode == 'debug' else 5000
    normal_cfg = {'ring_delay_ms': ring_delay}
    zero_token_cfg = {'ring_delay_ms': ring_delay, 'join_ring': False}

    servers = [await manager.server_add(config=normal_cfg),
               await manager.server_add(config=zero_token_cfg),
               await manager.server_add(config=normal_cfg)]

    # The zero-token node requires a different cql session not to be ignored by the driver because of empty tokens in
    # the system.peers table.
    # We need one cql session for both token-owning nodes to continue the write workload during the rolling restart.
    cql_normal = cluster_con([servers[0].ip_addr, servers[2].ip_addr], 9042, False, load_balancing_policy=
                             WhiteListRoundRobinPolicy([servers[0].ip_addr, servers[2].ip_addr])).connect()
    cql_zero_token = cluster_con([servers[1].ip_addr], 9042, False, load_balancing_policy=
                                 WhiteListRoundRobinPolicy([servers[1].ip_addr])).connect()
    # In the whole test, cqls[i] and hosts[i] correspond to servers[i].
    cqls = [cql_normal, cql_zero_token, cql_normal]
    hosts = [cql_normal.hosts[0] if cql_normal.hosts[0].address == servers[0].ip_addr else cql_normal.hosts[1],
             cql_zero_token.hosts[0],
             cql_normal.hosts[1] if cql_normal.hosts[0].address == servers[0].ip_addr else cql_normal.hosts[0]]

    # We don't want to use ManagerClient.rolling_restart. Waiting for CQL of the zero-token node would time out.
    async def rolling_restart():
        for idx, s in enumerate(servers):
            await manager.server_stop_gracefully(s.server_id)

            for idx2 in range(len(servers)):
                if idx2 != idx:
                    await manager.server_not_sees_other_server(servers[idx2].ip_addr, s.ip_addr)

            await manager.server_start(s.server_id)

            for idx2 in range(len(servers)):
                if idx2 != idx:
                    await manager.server_sees_other_server(servers[idx2].ip_addr, s.ip_addr)

    logging.info("Waiting until driver connects to every server")
    await asyncio.gather(*(wait_for_cql(cql, h, time.time() + 60) for cql, h in zip(cqls, hosts)))

    restart_writes, stop_writes_and_verify = await start_writes_to_cdc_table(cql_normal)

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for cql, h in zip(cqls, hosts)))

    # If we restarted nodes before the last generation was in use, some writes
    # could fail. After restart, nodes load only the last generation. If it's
    # not active yet, writes with lower timestamps would fail.
    await wait_until_last_generation_is_in_use(cql_normal)

    logging.debug("Sleeping for 1 second to make sure there are writes to the CDC table in all 3 generations")
    await asyncio.sleep(1)

    # Restart sequentially, as it tests how nodes operating in legacy mode
    # react to raft topology mode nodes and vice versa
    await rolling_restart()

    await stop_writes_and_verify()

    def reconnect_cqls():
        nonlocal cql_normal
        nonlocal cql_zero_token
        nonlocal cqls
        cql_normal.shutdown()
        cql_zero_token.shutdown()
        cql_normal = cluster_con([servers[0].ip_addr, servers[2].ip_addr], 9042, False, load_balancing_policy=
                                 WhiteListRoundRobinPolicy([servers[0].ip_addr, servers[2].ip_addr])).connect()
        cql_zero_token = cluster_con([servers[1].ip_addr], 9042, False, load_balancing_policy=
                                     WhiteListRoundRobinPolicy([servers[1].ip_addr])).connect()
        cqls = [cql_normal, cql_zero_token, cql_normal]

    reconnect_cqls()

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    await asyncio.gather(*(wait_for_cql(cql, h, time.time() + 60) for cql, h in zip(cqls, hosts)))
    logging.info(f"Driver reconnected, hosts: {hosts}")

    restart_writes(cql_normal)

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for cql, h in zip(cqls, hosts)))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for cql, h in zip(cqls, hosts)))

    logging.info(f"Restarting hosts {hosts}")
    await rolling_restart()

    # FIXME: We must reconnect the driver before performing CQL queries below, for example
    # in wait_until_schema_upgrade_finishes. Unfortunately, it forces us to stop writing to
    # a CDC table first. Reconnecting the driver would close the session used to send the
    # writes, and some writes could time out on the client.
    # Once https://github.com/scylladb/python-driver/issues/295 is fixed, we can remove
    # all calls to reconnect_driver, restart_writes and leave only the last call to
    # stop_writes_and_verify.
    await stop_writes_and_verify()

    reconnect_cqls()

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    await asyncio.gather(*(wait_for_cql(cql, h, time.time() + 60) for cql, h in zip(cqls, hosts)))

    restart_writes(cql_normal)

    logging.info("Waiting until upgrade to raft schema finishes")
    await asyncio.gather(*(wait_until_schema_upgrade_finishes(cql, h, time.time() + 60) for cql, h in zip(cqls, hosts)))

    logging.info("Checking the topology upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Waiting until all nodes see others as alive")
    await manager.servers_see_each_other(servers)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Waiting for CDC generations publishing")
    for cql, h in zip(cqls, hosts):
        await wait_for_cdc_generations_publishing(cql, [h], time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, cqls)

    logging.info("Booting new node")
    servers += [await manager.server_add(config=normal_cfg)]
    cqls += [cluster_con([servers[3].ip_addr], 9042, False,
                         load_balancing_policy=WhiteListRoundRobinPolicy([servers[3].ip_addr])).connect()]
    hosts += [cqls[3].hosts[0]]
    await asyncio.gather(*(wait_for_cql(cql, h, time.time() + 60) for cql, h in zip(cqls, hosts)))

    logging.info("Waiting for the new CDC generation publishing")
    for cql, h in zip(cqls, hosts):
        await wait_for_cdc_generations_publishing(cql, [h], time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, cqls)

    await wait_until_last_generation_is_in_use(cql_normal)

    logging.debug("Sleeping for 1 second to make sure there are writes to the CDC table in the last generation")
    await asyncio.sleep(1)

    logging.info("Checking correctness of data in system_distributed.cdc_streams_descriptions_v2")
    await stop_writes_and_verify()

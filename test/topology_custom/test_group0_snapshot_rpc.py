#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test snapshot transfer by forcing threshold and performing schema changes
"""
import asyncio
import logging
import pytest
import time

from test.topology.conftest import skip_mode
from test.topology.util import wait_for_cdc_generations_publishing
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_view


logger = logging.getLogger(__name__)


async def check_snapshot_consistency(manager: ManagerClient, servers: list[ServerInfo]):
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await wait_for_view(cql, "t_mv", len(servers))
    await asyncio.gather(*(read_barrier(manager.api, s.ip_addr) for s in servers))

    async def compare_table_contents(table: str):
        results = await asyncio.gather(*(cql.run_async(f"SELECT * FROM {table}", host=host) for host in hosts))

        for host, res in zip(hosts, results):
            logging.info(f"Dumping the state of {table} as seen by {host}:")
            for row in res:
                logging.info(f"  {row}")

        for host, res in zip(hosts, results):
            assert results[0] == res, f"Contents of {table} on {host} are not the same as on {hosts[0]}"

    await compare_table_contents("system.topology")
    await compare_table_contents("system.roles")
    await compare_table_contents("system.service_levels_v2")
    await compare_table_contents("system.view_build_status_v2")


@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_group0_snapshot_old_and_new_rpc(manager: ManagerClient):
    """
        L - raft leader
        This test checks that snapshot transfer works if:
        1. Node B sends a legacy snapshot request to L which is handled in legacy mode
        2. Node C sends a legacy snapshot request to L, first half is handled in legacy mode and second in new mode
        3. Node D sends a legacy snapshot request to L which is handled in new mode
        4. Node D sends a new snapshot request to L which is handled in new mode
    """

    async def server_add(force_legacy_request_mode: bool = False, force_stuck_between_legacy_requests: bool = False, start: bool = True) -> ServerInfo:
        injections = list()
        if force_legacy_request_mode or force_stuck_between_legacy_requests:
            injections.append("raft_pull_snapshot_sender_force_legacy")
        if force_stuck_between_legacy_requests:
            injections.append("raft_pull_snapshot_sender_pause_between_rpcs")
        return await manager.server_add(start=start, config={
            "logger_log_level": "group0_raft_sm=trace",
            "error_injections_at_startup": [
                {
                    "name": "raft_server_set_snapshot_thresholds",
                    "snapshot_threshold": "3",
                    "snapshot_trailing": "1",
                }
            ] + injections,
        })

    async def reconfigure_servers(force_legacy_handler_mode: bool):
        servers = await manager.running_servers()
        if force_legacy_handler_mode:
            await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "raft_pull_snapshot_handler_force_legacy", one_shot=False) for s in servers))
        else:
            await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "raft_pull_snapshot_handler_force_legacy") for s in servers))

    servers: list[ServerInfo] = []
    servers.append(await server_add())

    # Add some stuff so that the tables checked in check_snapshot_consistency are not empty
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.t_mv AS SELECT * FROM ks.t WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, pk)")
    await cql.run_async("CREATE SERVICE LEVEL sl WITH timeout = 500ms")
    # Auth creates the default role

    logger.info("Case 1: snapshot is requested in legacy mode, handler operates in legacy mode too")
    await reconfigure_servers(force_legacy_handler_mode=True)
    servers.append(await server_add(force_legacy_request_mode=True))
    await check_snapshot_consistency(manager, servers)

    logger.info("Case 2: snapshot is requested in legacy mode, handler changes its operation mode from legacy to new")
    await reconfigure_servers(force_legacy_handler_mode=True)
    async with asyncio.TaskGroup() as tg:
        srv = await server_add(force_stuck_between_legacy_requests=True, start=False)
        start_result = tg.create_task(manager.server_start(srv.server_id))
        log = await manager.server_open_log(srv.server_id)
        await log.wait_for("transfer snapshot: second round: requesting others")
        await reconfigure_servers(force_legacy_handler_mode=False)
        await manager.api.disable_injection(srv.ip_addr, "raft_pull_snapshot_sender_pause_between_rpcs")
        await start_result
        servers.append(srv)
    await check_snapshot_consistency(manager, servers)

    logger.info("Case 3: snapshot is requested in legacy mode, handler operates in new mode")
    await reconfigure_servers(force_legacy_handler_mode=False)
    servers.append(await server_add()) # Joining node will always request in legacy mode
    await check_snapshot_consistency(manager, servers)

    logger.info("Case 4: snapshot is requested in new, handler operates in new mode too")
    await manager.server_stop_gracefully(servers[-1].server_id)
    await reconfigure_servers(force_legacy_handler_mode=False)
    # Add more stuff to force snapshot transfer
    for i in range(5):
        await cql.run_async(f"CREATE SERVICE LEVEL sl{i} WITH TIMEOUT = 500ms")
    await manager.server_start(servers[-1].server_id)
    await check_snapshot_consistency(manager, servers)

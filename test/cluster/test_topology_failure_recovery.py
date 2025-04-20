#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
import pytest
import logging
import asyncio
import random
import sys

logger = logging.getLogger(__name__)

def init_random_seed():
    """ Initialize random seed for the test.

        The seed is logged so that the test can be reproduced if needed.
    """
    seed = random.randrange(sys.maxsize)
    random.seed(seed)
    logger.info("Random seed: %s", seed)


async def get_running_servers(manager: ManagerClient):
    """ Return the running servers in randomized order.

        This helps to avoid making any assumptions on the order of the servers,
        as in general the order of the cluster running servers can be somewhat
        random (especially when initialized in parallel).
    """
    servers = await manager.running_servers()
    random.shuffle(servers)
    return servers


async def inject_error_on(manager: ManagerClient, error_name: str, servers: list[ServerInfo]):
    """ Inject an error on the given servers. """
    errs = [manager.api.enable_injection(s.ip_addr, error_name, one_shot=True) for s in servers]
    await asyncio.gather(*errs)


async def remove_error_on(manager: ManagerClient, error_name: str, servers: list[ServerInfo]):
    """ Remove an error injection on the given servers. """
    errs = [manager.api.disable_injection(s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_drain_failure_during_decommission(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    servers = [await manager.server_add(config=cfg) for _ in range(3)]

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        await inject_error_on(manager, "stream_tablet_fail_on_drain", servers)

        await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")

        matches = [await log.grep("raft_topology - rollback.*after decommissioning failure, moving transition state to rollback to normal",
                from_mark=mark) for log, mark in zip(logs, marks)]
        assert sum(len(x) for x in matches) == 1


@pytest.mark.asyncio
@pytest.mark.prepare_3_nodes_cluster
@skip_mode('release', 'error injections are not supported in release mode')
async def test_topology_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while doing a topology operation"""
    init_random_seed()
    # decommission failure
    servers = await get_running_servers(manager)
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")
    servers = await get_running_servers(manager)
    assert len(servers) == 3
    matches = [await log.grep("raft_topology - rollback.*after decommissioning failure, moving transition state to rollback to normal",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # bootstrap failure
    marks = [await log.mark() for log in logs]
    servers = await get_running_servers(manager)
    s = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
    servers = await get_running_servers(manager)
    assert s not in servers
    matches = [await log.grep("raft_topology - rollback.*after bootstrapping failure, moving transition state to left token ring",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # bootstrap failure in raft barrier
    marks = [await log.mark() for log in logs]
    servers = await get_running_servers(manager)
    s = await manager.server_add(start=False)
    await inject_error_on(manager, "raft_topology_barrier_fail", servers)
    try:
        await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
        servers = await get_running_servers(manager)
        assert s not in servers
        matches = [await log.grep("raft_topology - rollback.*after bootstrapping failure, moving transition state to left token ring",
                                  from_mark=mark) for log, mark in zip(logs, marks)]
        assert sum(len(x) for x in matches) == 1
    finally:
        # the injection is one-shot, but it will not trigger on the coordinator
        # so it needs to be removed manually to not affect the rest of the test
        await remove_error_on(manager, "raft_topology_barrier_fail", servers)
    # rebuild failure
    marks = [await log.mark() for log in logs]
    servers = await get_running_servers(manager)
    await manager.api.enable_injection(servers[1].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.rebuild_node(servers[1].server_id, expected_error="rebuild failed:")
    # replace failure
    marks = [await log.mark() for log in logs]
    servers = await get_running_servers(manager)
    await manager.server_stop_gracefully(servers[2].server_id)
    downed_server_id = servers[2].server_id
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    s = await manager.server_add(start=False, replace_cfg=replace_cfg, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Replace failed. See earlier errors")
    servers = await get_running_servers(manager)
    assert s not in servers
    matches = [await log.grep("raft_topology - rollback.*after replacing failure, moving transition state to left token ring",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # remove failure
    marks = [await log.mark() for log in logs]
    await manager.api.enable_injection(servers[1].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.remove_node(servers[0].server_id, downed_server_id, expected_error="Removenode failed. See earlier errors")
    matches = [await log.grep("raft_topology - rollback.*after removing failure, moving transition state to rollback to normal",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
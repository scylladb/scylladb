#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time
from uuid import UUID

import pytest

from test.cluster.util import get_topology_coordinator
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-6087",
                      reason="a joining node killed after it joined group 0 fails every later start"
                             " with 'no entry for request id'")
@pytest.mark.parametrize("coordinator_finished", [False, True],
                         ids=["coordinator_still_bootstrapping", "coordinator_marked_it_normal"])
async def test_restart_joiner_killed_after_group0_join(manager: ScyllaClusterManager, coordinator_finished: bool):
    """
    Idea: a joining node that is killed after it joined group 0, and before it reached
    NORMAL, must be able to start again: it resumes its join, or the cluster rejects it
    with a clear error. It must not fail with an internal error, and a node the
    coordinator already made a normal member must come up.

    Setup: three nodes and a joiner. The joiner is held by an error injection right
    before its first read of its own join request, so it is in group 0 and in JOINING
    mode while the request is still pending. It is killed with SIGKILL at one of two
    moments, then started again:

    - coordinator_still_bootstrapping: the topology coordinator is held at the last step
      of the bootstrap (delay_node_bootstrap), so the request is pending on both sides
      when the joiner is killed. The coordinator is released once the restarted joiner
      is back in JOINING mode, so that a join that resumes can complete.
    - coordinator_marked_it_normal: the coordinator finished the bootstrap and marked the
      node normal and its request done; the joiner is killed before it reads that. The
      cluster then has a normal member that was never up.

    The second start must not log "no entry for request id". If it fails, it must be
    because the cluster rejected the node, and a node the coordinator made normal must
    come up.
    """
    servers = await manager.servers_add(3)
    cql, hosts = await manager.get_ready_cql(servers)

    coordinator_host_id = await get_topology_coordinator(manager)
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    coordinator = servers[host_ids.index(coordinator_host_id)]
    if not coordinator_finished:
        await manager.api.enable_injection(coordinator.ip_addr, 'delay_node_bootstrap', one_shot=False)

    # The joiner joins group 0, enters JOINING mode and stops right before its first
    # read of system.topology_requests.
    joiner = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['topology_request_pause_before_wait']})
    await manager.server_start(joiner.server_id, expected_server_up_state=ServerUpState.PROCESS_STARTED)
    await manager.api.wait_for_injection_enter(joiner.ip_addr, 'topology_request_pause_before_wait')
    joiner_host_id = await manager.get_host_id(joiner.server_id)

    if coordinator_finished:
        async def joiner_is_normal():
            rows = await cql.run_async("SELECT node_state FROM system.topology WHERE key = 'topology' AND host_id = %s",
                                       [UUID(joiner_host_id)], host=hosts[0])
            return (rows and rows[0].node_state == 'normal') or None
        await wait_for(joiner_is_normal, time.time() + 120, label="joiner normal in system.topology")
    else:
        await manager.api.wait_for_injection_enter(coordinator.ip_addr, 'delay_node_bootstrap')

    logger.info(f"Killing {joiner} (host id {joiner_host_id}) while its join request is pending for it")
    await manager.server_stop(joiner.server_id, convict=False)
    await manager.server_update_config(joiner.server_id, 'error_injections_at_startup', [])

    log = await manager.server_open_log(joiner.server_id)
    mark = await log.mark()
    start = asyncio.create_task(manager.server_start(joiner.server_id))
    if not coordinator_finished:
        try:
            await log.wait_for("entering JOINING mode", from_mark=mark, timeout=120)
        finally:
            await manager.api.message_injection(coordinator.ip_addr, 'delay_node_bootstrap')
    try:
        await start
        started = True
    except RuntimeError as e:
        logger.info(f"{joiner} did not start again: {e}")
        started = False

    internal_errors = [line.split(", at:")[0] for line, _ in await log.grep("no entry for request id", from_mark=mark)]
    assert not internal_errors, f"{joiner} failed to start with an internal error:\n" + "\n".join(internal_errors)
    if not started:
        assert not coordinator_finished, f"{joiner} is a normal member of the cluster but did not start again"
        rejections = await log.grep("Startup failed: .*(rejected|banned|cannot be restarted)", from_mark=mark)
        assert rejections, f"{joiner} did not start again and the cluster did not reject it"

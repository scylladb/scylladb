#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest

from test.cluster.util import get_topology_coordinator
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature

logger = logging.getLogger(__name__)

TEST_FEATURE = "TEST_ONLY_FEATURE"
PAUSE_COORDINATOR = "topology_coordinator_pause_before_processing_backlog"
BLOCK_APPLY = "group0_state_machine::block_apply"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_join_request_from_left_node_is_rejected(manager: ScyllaClusterManager) -> None:
    """
    Idea: a node that has left the cluster must not get a join request placed
    again. Normally the RPC ban stops it: every node bans the host ids it sees
    in `left` when it loads the topology state, and refuses their connections.
    A node that has not applied the `left` state yet accepts the connection,
    and join_node_request_handler must reject the request after its read
    barrier, with "The node has already been removed from the cluster". When
    that check looked the host up with topology::find, which never returns
    left nodes, the request was placed instead: the host's row went back to
    node_state=none and the coordinator spent 30 s in wait_for_ip before it
    moved the host to `left` again.

    Setup: 3 nodes with TEST_ONLY_FEATURE enabled, a 4th node D that does not
    support it, B a node other than the topology coordinator.

    D joins through B (seeds=[B]). The coordinator is paused until B has placed
    the request, then B's group0 apply is blocked and the coordinator rejects D
    on the feature check, moving it to `left`. B has not applied that, so it
    has not banned D. D starts again with seeds=[B], with the same host id;
    once B has received the request, B's apply is released, so its read
    barrier catches up to D being `left`. The test checks that B rejects the
    request instead of placing it.
    """
    servers = await manager.servers_add(3, config={'error_injections_at_startup': ['features_enable_test_feature']})
    hosts = await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 60)
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE, manager.cql, h, time.time() + 60) for h in hosts))

    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    b = next(s for s in servers if s != coordinator)
    log_b = await manager.server_open_log(b.server_id)

    # D's first join, placed by B and rejected by the coordinator after B stops applying.
    await manager.api.enable_injection(coordinator.ip_addr, PAUSE_COORDINATOR, one_shot=True)
    d = await manager.server_add(start=False)
    first_join = asyncio.create_task(manager.server_start(
        d.server_id, seeds=[b.ip_addr],
        expected_error="Feature check failed|received notification of being banned from the cluster from"))
    _, [(_, match)] = await log_b.wait_for(r"placed join request for (\S+)", timeout=60)
    d_id = match.group(1)
    await manager.api.enable_injection(b.ip_addr, BLOCK_APPLY, one_shot=False)
    await manager.api.message_injection(coordinator.ip_addr, PAUSE_COORDINATOR)
    await first_join

    # D, now left, asks B again; B catches up on its read barrier.
    mark = await log_b.mark()
    second_join = asyncio.create_task(manager.server_start(
        d.server_id, seeds=[b.ip_addr],
        expected_error="already been removed from the cluster|connection is closed|being banned"))
    await log_b.wait_for(f"received request to join from host_id: {d_id}", from_mark=mark, timeout=60)
    await manager.api.disable_injection(b.ip_addr, BLOCK_APPLY)
    _, [(line, _)] = await log_b.wait_for(
        f"placed join request for {d_id}|the node {d_id} attempted to join but it was removed",
        from_mark=mark, timeout=60)
    await second_join
    assert "attempted to join but it was removed" in line, f"B placed a join request from a left node: {line}"

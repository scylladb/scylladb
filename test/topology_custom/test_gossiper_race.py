#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#


from aiohttp import ServerDisconnectedError
import pytest

from test.topology.conftest import skip_mode
from test.topology.util import get_coordinator_host
from test.pylib.manager_client import ManagerClient


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.xfail(reason="https://github.com/scylladb/scylladb/issues/25621")
async def test_gossiper_race_on_decommission(manager: ManagerClient):
    """
    Test for gossiper race scenario (https://github.com/scylladb/scylladb/issues/25621):
    - Create a cluster with multiple nodes
    - Decommission one node while injecting delays in gossip processing
    - Check for the race condition where get_host_id() is called on a removed endpoint
    """
    cmdline = [
        '--logger-log-level=gossip=debug',
        '--logger-log-level=raft_topology=debug'
    ]

    # Create cluster with more nodes to increase gossip traffic
    servers = await manager.servers_add(3, cmdline=cmdline)

    coordinator = await get_coordinator_host(manager=manager)
    coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)
    coordinator_log_mark = await coordinator_log.mark()

    decom_node = next(s for s in servers if s.server_id != coordinator.server_id)

    # enable the delay_gossiper_apply injection
    await manager.api.enable_injection(
        node_ip=coordinator.ip_addr,
        injection="delay_gossiper_apply",
        one_shot=False,
        parameters={"delay_node": decom_node.ip_addr},
    )

    # wait for the "delay_gossiper_apply" error injection to take effect
    # - wait for multiple occurrences to be batched, so that there is a higher chance of one of them
    #   failing down in the `gossiper::do_on_change_notifications()`
    for _ in range(5):
        log_mark = await coordinator_log.mark()
        await coordinator_log.wait_for(
            "delay_gossiper_apply: suspend for node",
            from_mark=log_mark,
        )

    coordinator_log_mark = await coordinator_log.mark()

    # start the decommission task
    await manager.decommission_node(decom_node.server_id)

    # wait for the node to finish the removal
    await coordinator_log.wait_for(
        "Finished to force remove node",
        from_mark=coordinator_log_mark,
    )

    coordinator_log_mark = await coordinator_log.mark()

    try:
        # unblock the delay_gossiper_apply injection
        await manager.api.message_injection(
            node_ip=coordinator.ip_addr,
            injection="delay_gossiper_apply",
        )
    except ServerDisconnectedError:
        # the server might get disconnected in the failure case because of abort
        # - we detect that later (with more informatiove error handling), so we ignore this here
        pass

    # wait for the "delay_gossiper_apply" error injection to be unblocked
    await coordinator_log.wait_for(
        "delay_gossiper_apply: resume for node",
        from_mark=coordinator_log_mark,
    )

    # test that the coordinator node didn't abort
    empty_host_found = await coordinator_log.grep(
        "gossip - adding a state with empty host id",
        from_mark=coordinator_log_mark,
    )

    assert not empty_host_found, "Empty host ID has been found in gossiper::replicate()"

    # secondary test - ensure the coordinator node is still running
    running_servers = await manager.running_servers()
    assert coordinator.server_id in [s.server_id for s in running_servers]

#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


import asyncio
import time

from aiohttp import ServerDisconnectedError
import pytest

from test.cluster.util import get_coordinator_host
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_gossiper_race_on_decommission(manager: ScyllaClusterManager):
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

    # wait for the "delay_gossiper_apply" error injection to take effect.
    # Pending state applies are coalesced per endpoint (at most one apply
    # fiber per endpoint), so only a single suspension can be observed;
    # newer states of the node arriving while the apply is suspended are
    # coalesced into the pending slot instead of spawning more fibers.
    await coordinator_log.wait_for(
        "delay_gossiper_apply: suspend for node",
        from_mark=coordinator_log_mark,
    )

    # wait until newer states of this specific node have queued up behind the
    # suspended apply, so that the apply resumed after the decommission
    # races with the node's removal like the batched applies used to
    decom_host_id = await manager.get_host_id(decom_node.server_id)
    await coordinator_log.wait_for(
        f"queue_state_apply: coalesced a state of {decom_host_id} into the pending one",
        from_mark=coordinator_log_mark,
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

    # test that the coordinator node didn't hit the case where it would try to add a state with empty host id
    empty_host_found = await coordinator_log.grep(
        "gossip - attempting to add a state with empty host id",
        from_mark=coordinator_log_mark,
    )

    assert not empty_host_found, "Empty host ID has been found in gossiper::replicate()"

    # secondary test - ensure the coordinator node is still running
    running_servers = await manager.running_servers()
    assert coordinator.server_id in [s.server_id for s in running_servers]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_gossiper_no_resurrection_on_decommission(manager: ScyllaClusterManager):
    """
    A state carrying HOST_ID that was queued before the node was removed must
    not re-add the removed endpoint when it is finally applied: the applier
    re-checks left_nodes under the endpoint lock. Runtime: ~30s.
    """
    cmdline = [
        '--logger-log-level=gossip=debug',
        '--logger-log-level=raft_topology=debug'
    ]
    servers = await manager.servers_add(3, cmdline=cmdline)

    coordinator = await get_coordinator_host(manager=manager)
    coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)
    mark = await coordinator_log.mark()

    decom_node = next(s for s in servers if s.server_id != coordinator.server_id)

    # Suspend application of every state of the decommissioned node —
    # including the usual ones that carry HOST_ID — so one is still queued
    # when the node is removed.
    await manager.api.enable_injection(
        node_ip=coordinator.ip_addr,
        injection="delay_gossiper_apply",
        one_shot=False,
        parameters={"delay_node": decom_node.ip_addr, "any_state": "1"},
    )
    await coordinator_log.wait_for("delay_gossiper_apply: suspend for node", from_mark=mark)

    await manager.decommission_node(decom_node.server_id)
    await coordinator_log.wait_for("Finished to force remove node", from_mark=mark)

    mark = await coordinator_log.mark()
    try:
        await manager.api.message_injection(node_ip=coordinator.ip_addr, injection="delay_gossiper_apply")
    except ServerDisconnectedError:
        pass

    # The suspended apply must be discarded by the under-lock re-check ...
    await coordinator_log.wait_for(
        "do_apply_state_locally: ignoring gossip for .* because it left",
        from_mark=mark,
        timeout=60,
    )

    # ... and the removed node must not be resurrected in the endpoint map.
    eps = await manager.api.client.get_json("/failure_detector/endpoints/", host=coordinator.ip_addr)
    addrs = {e["addrs"] for e in eps}
    assert decom_node.ip_addr not in addrs, \
        f"decommissioned node {decom_node.ip_addr} was resurrected in the gossiper endpoint map"



@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_gossiper_no_resurrection_before_topology_commit(manager: ScyllaClusterManager):
    """
    A node that left is removed from the gossiper only after the topology which
    lists it in left_nodes is committed to memory. Gossip about the node that
    arrives while that topology is built, but not committed yet, must not re-add
    the endpoint after the removal.

    The topology coordinator is held before it applies the change which moves the
    decommissioned node to left_nodes, so it keeps gossiping the node's state
    while another node holds the same change between building and committing it.
    """
    cmdline = [
        '--logger-log-level=gossip=debug',
        '--logger-log-level=raft_topology=debug'
    ]
    servers = await manager.servers_add(3, cmdline=cmdline)

    coordinator = await get_coordinator_host(manager=manager)
    decom_node = next(s for s in servers if s.server_id != coordinator.server_id)
    observer = next(s for s in servers if s.server_id not in (coordinator.server_id, decom_node.server_id))
    decom_host_id = await manager.get_host_id(decom_node.server_id)

    cql = manager.get_cql()
    hosts = {s.server_id: h for s, h in zip([coordinator, observer],
                                             await wait_for_cql_and_get_hosts(cql, [coordinator, observer], time.time() + 60))}

    release = asyncio.Event()

    async def hold_left_change(server: ServerInfo, injection: str, held: asyncio.Event) -> None:
        """Releases every group 0 change paused at the injection, except the one which moves
        the decommissioned node to left_nodes, which is held until release is set."""
        log = await manager.server_open_log(server_id=server.server_id)
        mark = await log.mark()
        await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
        while True:
            mark, _ = await log.wait_for(f"{injection}: waiting for message", from_mark=mark, timeout=300)
            # The change is persisted before the injection is reached.
            rows = await cql.run_async(
                f"SELECT node_state FROM system.topology WHERE key = 'topology' AND host_id = {decom_host_id}",
                host=hosts[server.server_id])
            if rows and rows[0].node_state == 'left':
                held.set()
                await release.wait()
                # Also releases the held change.
                await manager.api.disable_injection(server.ip_addr, injection)
                return
            await manager.api.message_injection(server.ip_addr, injection)

    coordinator_held = asyncio.Event()
    observer_held = asyncio.Event()
    holders = [
        asyncio.create_task(hold_left_change(coordinator, "group0_pause_before_topology_transition", coordinator_held)),
        asyncio.create_task(hold_left_change(observer, "schema_applier_pause_before_commit", observer_held)),
    ]
    decommission = asyncio.create_task(manager.decommission_node(decom_node.server_id))

    try:
        await asyncio.wait_for(asyncio.gather(coordinator_held.wait(), observer_held.wait()), timeout=300)

        # Let the observer gossip with the coordinator, which still has the node.
        await asyncio.sleep(3)

        observer_log = await manager.server_open_log(server_id=observer.server_id)
        mark = await observer_log.mark()
        await manager.api.disable_injection(observer.ip_addr, "schema_applier_pause_before_commit")
        await observer_log.wait_for("topology_state_load: notifying nodes after token metadata replication", from_mark=mark)

        # The coordinator is still held, so the observer applies no further topology change,
        # which would remove the node from the gossiper again.
        async def node_removed():
            eps = await manager.api.client.get_json("/failure_detector/endpoints/", host=observer.ip_addr)
            if decom_node.ip_addr not in {e["addrs"] for e in eps}:
                return True
        await wait_for(node_removed, time.time() + 10, period=0.5)
    except BaseException:
        # The test failed, so it does not wait for the decommission, nor for a holder
        # still waiting for its node to pause.
        for task in [decommission, *holders]:
            task.cancel()
        await asyncio.gather(decommission, *holders, return_exceptions=True)
        raise
    finally:
        release.set()
        await asyncio.gather(*holders, return_exceptions=True)
        for server, injection in [(coordinator, "group0_pause_before_topology_transition"),
                                  (observer, "schema_applier_pause_before_commit")]:
            await manager.api.disable_injection(server.ip_addr, injection)

    await decommission

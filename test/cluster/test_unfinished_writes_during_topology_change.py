#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging

import pytest

from test.cluster.test_tablets2 import inject_error_on
from test.cluster.util import get_topology_coordinator
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_unfinished_audit_write_during_topology_change(manager: ScyllaClusterManager) -> None:
    """An audit write whose replica never answers must not block a topology change.

    The write response handler pins a token_metadata version until it completes or times
    out, and barrier_and_drain waits for stale versions. Audit writes have a five-second
    timeout; with the one-hour internal timeout the join below hangs.

    Steps:
     1. Pause the coordinator before barrier_and_drain and add a node.
     2. At the first barrier, pause write responses on the other node and run an audited
        statement on the coordinator. CL=ONE is satisfied locally, and the handler stays
        alive for the parked remote response.
     3. Release the first barrier and wait for the second one, by which time the handler's
        version is stale.
     4. Release the second barrier and check that the join completes.
    """
    cmdline = ['--logger-log-level', 'debug_error_injection=debug']
    await manager.servers_add(2, auto_rack_dc="dc1", cmdline=cmdline)

    running = await manager.running_servers()
    cql, hosts = await manager.get_ready_cql(running)

    coordinator_host_id = await get_topology_coordinator(manager)
    target_server: ServerInfo | None = None
    for s in running:
        if await manager.get_host_id(s.server_id) == coordinator_host_id:
            target_server = s
            break
    assert target_server is not None, f"No topology coordinator among {running}"
    other_server = next(s for s in running if s.server_id != target_server.server_id)
    target_host = next(h for h in hosts if h.address == str(target_server.rpc_address))

    logger.info(f"Pausing barrier_and_drain on the coordinator {target_server}")
    await manager.api.enable_injection(target_server.ip_addr, "pause_before_barrier_and_drain",
                                       one_shot=False)

    logger.info("Adding a node to trigger a topology change")
    add_node_task = asyncio.create_task(
        manager.server_add(property_file={"dc": "dc1", "rack": running[0].rack}))

    await manager.api.wait_for_injection_enter(target_server.ip_addr,
                                              "pause_before_barrier_and_drain")

    logger.info(f"Pausing write responses on {other_server}")
    await inject_error_on(manager, "storage_proxy_write_response_pause", [other_server])
    write_pause_released = False

    async def release_write_pause() -> None:
        nonlocal write_pause_released
        if not write_pause_released:
            write_pause_released = True
            await manager.api.message_injection(other_server.ip_addr,
                                                "storage_proxy_write_response_pause")

    try:
        # CREATE SERVICE LEVEL carries no keyspace in its audit_info, so it is audited under
        # the default audit configuration.
        logger.info("Running an audited statement on the coordinator")
        await cql.run_async("CREATE SERVICE LEVEL IF NOT EXISTS unfinished_write_sl WITH SHARES = 100",
                            host=target_host)

        # Make sure the audit write reached the other replica and is parked there; otherwise
        # nothing pins a version and the test passes vacuously.
        await manager.api.wait_for_injection_enter(other_server.ip_addr,
                                                  "storage_proxy_write_response_pause")

        logger.info("Releasing the first barrier_and_drain")
        await manager.api.message_injection(target_server.ip_addr, "pause_before_barrier_and_drain")

        logger.info("Waiting for the second barrier_and_drain")
        await manager.api.wait_for_injection_enter(target_server.ip_addr,
                                                  "pause_before_barrier_and_drain", threshold=2)

        logger.info("Releasing barrier_and_drain so it reaches stale_versions_in_use()")
        await manager.api.disable_injection(target_server.ip_addr, "pause_before_barrier_and_drain")

        try:
            # Generous relative to the five-second audit write timeout, tight enough that
            # the one-hour stall of the old behaviour fails the test rather than timing the
            # suite out.
            new_server = await asyncio.wait_for(add_node_task, timeout=180)
        except asyncio.TimeoutError:
            # Deadlock reproduced. Release the paused write and stop everything, including
            # the half-joined node, so teardown does not wait out its own timeout on a
            # cluster whose coordinator is wedged.
            logger.info("Topology change did not complete; killing all servers")
            await release_write_pause()
            for s in await manager.all_servers() + await manager.starting_servers():
                await manager.server_stop(s.server_id, convict=True)
            pytest.fail("Topology change did not complete within 180s: the unfinished audit "
                        "write pinned a stale token_metadata version and blocked barrier_and_drain")

        logger.info(f"Topology change completed, new node {new_server}")
    finally:
        await release_write_pause()

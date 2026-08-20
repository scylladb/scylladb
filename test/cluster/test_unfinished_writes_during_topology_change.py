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
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_unfinished_internal_write_during_topology_change(manager: ManagerClient) -> None:
    """A stuck internal write must not block a topology change on a running node.

    A write response handler holds its effective_replication_map, and thus the
    token_metadata version current when the write started, until it completes. A write
    issued with an internal client state gets its expire timer from
    infinite_timeout_config, whose one-hour timeouts are indistinguishable from forever
    for a topology operation. If such a write's MUTATION_DONE never arrives, the handler,
    and the pinned version, live for up to that hour.
    raft_topology_cmd::barrier_and_drain then blocks in stale_versions_in_use() for as
    long, and no topology operation on that node can complete meanwhile: a joining node
    does not leave the bootstrap state.

    test_unfinished_writes_during_shutdown covers the same hazard on the shutdown path,
    where storage_proxy::do_drain() -> cancel_nonlocal_write_response_handlers() releases
    the handler. This test covers the running node, where nothing did until
    local_topology_barrier() started bounding such handlers.

    The internal write used here is the audit log write, which is how this was found in the
    wild (CI 32012): audit's should_log() short-circuits on an empty keyspace, so a
    statement whose audit_info carries no keyspace - CREATE SERVICE LEVEL here, ALTER
    CLUSTER in the original failure - is audited under the default configuration. The audit
    keyspace is NetworkTopologyStrategy RF=3 and audit writes use CL=ONE, so on a two-node
    cluster the coordinator's local replica satisfies CL immediately while the second
    replica's response keeps the handler alive. Audit writes now use a bounded (5s) timeout
    config of their own, so the audit_unbounded_write_timeout injection restores the old
    one-hour internal timeout to keep driving the barrier backstop with a real write.

    Steps:
     1. Enable pause_before_barrier_and_drain on the topology coordinator.
     2. Add a node, so a topology change issues barrier_and_drain.
     3. Wait for the first barrier_and_drain to hit the injection.
     4. Pause write responses on the other node.
     5. Run an audited statement on the coordinator. Its audit write completes locally at
        CL=ONE but keeps a write handler alive for the paused replica.
     6. Release the first barrier_and_drain (the handler's version is still current), then
        wait for the second one - by now a new token_metadata version is installed, so the
        handler's ERM is stale.
     7. Release the second barrier_and_drain and let it reach stale_versions_in_use().
     8. Verify the topology change still completes, and that it completed because the
        barrier bounded the stuck handler rather than by luck.
    """
    cmdline = ['--logger-log-level', 'debug_error_injection=debug']
    config = {'error_injections_at_startup': ['audit_unbounded_write_timeout']}
    await manager.servers_add(2, auto_rack_dc="dc1", cmdline=cmdline, config=config)

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

    target_log = await manager.server_open_log(target_server.server_id)
    log_mark = await target_log.mark()

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
        # An audited statement, run on the coordinator: with the startup injection above the
        # audit write's handler is armed at the one-hour internal timeout, so it cannot
        # expire on its own within the test.
        logger.info("Running an audited statement on the coordinator")
        await cql.run_async("CREATE SERVICE LEVEL IF NOT EXISTS unfinished_write_sl WITH SHARES = 100",
                            host=target_host)

        # Confirm the audit write actually reached the other replica and is parked there;
        # otherwise nothing pins a version and the test would pass vacuously.
        await manager.api.wait_for_injection_enter(other_server.ip_addr,
                                                  "storage_proxy_write_response_pause")

        # Release the first barrier. It completes: the handler's ERM version is still current.
        logger.info("Releasing the first barrier_and_drain")
        await manager.api.message_injection(target_server.ip_addr, "pause_before_barrier_and_drain")

        # By the second barrier, topology_state_load has installed a new token_metadata
        # version, so the handler's ERM is now stale and it is what the barrier waits on.
        logger.info("Waiting for the second barrier_and_drain")
        await manager.api.wait_for_injection_enter(target_server.ip_addr,
                                                  "pause_before_barrier_and_drain", threshold=2)

        logger.info("Releasing barrier_and_drain so it reaches stale_versions_in_use()")
        await manager.api.disable_injection(target_server.ip_addr, "pause_before_barrier_and_drain")

        try:
            # Generous relative to the 30s grace period, tight enough that a genuine deadlock
            # (which lasts up to the one-hour write timeout) still fails the test rather than
            # timing the suite out.
            new_server = await asyncio.wait_for(add_node_task, timeout=180)
        except asyncio.TimeoutError:
            # Deadlock reproduced. Release the paused write and stop everything, including
            # the half-joined node, so teardown does not wait out its own timeout on a
            # cluster whose coordinator is wedged.
            logger.info("Topology change did not complete; killing all servers")
            await release_write_pause()
            for s in await manager.all_servers() + await manager.starting_servers():
                await manager.server_stop(s.server_id, convict=True)
            pytest.fail("Topology change did not complete within 180s - the stuck internal write "
                        "pinned a stale token_metadata version and deadlocked barrier_and_drain")

        # Assert the mechanism, not just the outcome: the barrier must have completed because it
        # bounded the stuck handler. Without this, the test would also pass if the write had
        # completed on its own and the pin never mattered.
        bounded = await target_log.grep(r"Bounding write handler .* on audit\.audit_log",
                                        from_mark=log_mark)
        assert bounded, ("The topology change completed without the barrier bounding the stuck "
                         "audit write, so this run did not exercise the hazard")

        logger.info(f"Topology change completed, new node {new_server}")
    finally:
        await release_write_pause()

#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_first_completed
from collections.abc import Coroutine
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

# The test kills nodes while topology requests are queued. If the cluster has
# tablets, the tablet migrations of the auto-RF system keyspaces need a global
# token metadata barrier which requires every node to be up (see the FIXME in
# service::topology_coordinator::global_tablet_token_metadata_barrier()). The
# coordinator then retries the barrier forever and never gets to process - and
# cancel - the queued requests. Keep the system keyspaces on vnodes so that the
# cluster has no tablets at all; this test is about the request queue, not about
# tablets.
CONFIG = {'error_injections_at_startup': ['auto_rf_keyspaces_use_vnodes']}


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_coordinator_queue_management(manager: ScyllaClusterManager):
    """This test creates a 5 node cluster with 2 down nodes (A and B). After that it
       creates a queue of 3 topology operation: bootstrap, removenode A and removenode B
       with ignore_nodes=A. Check that all operation manage to complete.
       Then it downs one node and creates a queue with two requests:
       bootstrap and decommission. Since none can proceed both should be canceled.
    """
    await manager.servers_add(5, config=CONFIG)
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    # Submit the removenode for servers[3] while servers[4] is still alive
    # to avoid the initiator-side liveness check. Stop servers[4] after
    # the removenode is confirmed as queued.
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)

    inj = 'topology_coordinator_pause_before_processing_backlog'
    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s3_id = await manager.get_host_id(servers[3].server_id)
    tasks = [asyncio.create_task(manager.server_add(config=CONFIG)),
             asyncio.create_task(manager.remove_node(servers[0].server_id, servers[3].server_id))]

    # Ensure the removenode is queued before stopping servers[4].
    marks[0], _ = await logs[0].wait_for("raft_topology - removenode: waiting for completion", from_mark=marks[0])

    await manager.server_stop_gracefully(servers[4].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[4].ip_addr)

    tasks += [asyncio.create_task(manager.remove_node(servers[0].server_id, servers[4].server_id, [s3_id]))]

    await wait_for_first_completed([
        l.wait_for("received request to join from host_id", from_mark=m) for l, m in zip(logs[:3], marks[:3])
    ])

    marks[0], _ = await logs[0].wait_for("raft_topology - removenode: waiting for completion", from_mark=marks[0])

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await asyncio.gather(*tasks)

    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)

    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s = await manager.server_add(start=False, config=CONFIG)

    tasks = [asyncio.create_task(manager.server_start(s.server_id, expected_error="request canceled because some required nodes are dead|received notification of being banned from the cluster from")),
             asyncio.create_task(manager.decommission_node(servers[1].server_id, expected_error="Decommission failed. See earlier errors"))]

    await wait_for_first_completed([
        l.wait_for("received request to join from host_id", from_mark=m) for l, m in zip(logs[:3], marks[:3])
    ])

    await logs[1].wait_for("raft_topology - decommission: waiting for completion", from_mark=marks[1])

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await asyncio.gather(*tasks)

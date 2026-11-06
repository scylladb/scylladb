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
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

COORDINATOR_CLOCK_BEHIND_BUG = pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-6088",
    reason="the coordinator's done=true loses to the requester's done=false when its clock is behind")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='test_clocks_offset_seconds exists only in builds with error injection')
@pytest.mark.parametrize("coordinator_clock_offset", [0, pytest.param(-600, marks=COORDINATOR_CLOCK_BEHIND_BUG)])
async def test_decommission_with_coordinator_clock_behind(manager: ScyllaClusterManager,
                                                          coordinator_clock_offset: int) -> None:
    """
    Idea: a topology request must read as done once the topology coordinator
    has completed it, whatever the wall clocks of the coordinator and of the
    node that placed it. Every write to a request's row in
    system.topology_requests carries the same timestamp, the request id's, and
    the same expiry, the id's time plus a month. Two such cells are reconciled
    by their TTL: the one computed from the later gc_clock wins. The requester
    writes done=false when it places the request and the coordinator writes
    done=true when it completes it, so with the coordinator's clock behind the
    requester's by more than the operation takes, done=true loses on every
    replica, and the requester, which polls its own row, waits forever.

    Setup: 3 nodes, no user tables. The topology coordinator's clock is moved
    by test_clocks_offset_seconds before another node is decommissioned. The
    decommission gets a bounded wait, and the test checks that it returns and
    that the request's row reads done=true on the remaining nodes. With no
    offset the decommission takes seconds, so the coordinator's write has the
    later clock, or ties on TTL and the larger value, true, wins.
    """
    servers = await manager.servers_add(3)
    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host_of = {s.server_id: next(h for h in hosts if h.address == s.ip_addr) for s in servers}

    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    leaving = next(s for s in servers if s != coordinator)
    await cql.run_async(
        f"UPDATE system.config SET value = '{coordinator_clock_offset}' WHERE name = 'test_clocks_offset_seconds'",
        host=host_of[coordinator.server_id])

    leaving_log = await manager.server_open_log(leaving.server_id)
    coordinator_log = await manager.server_open_log(coordinator.server_id)
    decommission = asyncio.create_task(manager.decommission_node(leaving.server_id))
    _, [(_, match)] = await leaving_log.wait_for(
        r"decommission: waiting for completion \(request ID: (\S+)\)", timeout=60)
    request_id = match.group(1)
    # The coordinator completes the operation either way; what differs is
    # whether the leaving node learns of it.
    await coordinator_log.wait_for("finished decommissioning node", timeout=120)

    finished, _ = await asyncio.wait({decommission}, timeout=60)
    returned = decommission in finished
    if returned:
        decommission.result()
    done = {}
    for s in servers:
        if s != leaving:
            rows = await cql.run_async(f"SELECT done FROM system.topology_requests WHERE id = {request_id}",
                                       host=host_of[s.server_id])
            done[s.ip_addr] = rows[0].done
    logger.info("decommission returned: %s, request %s done flags: %s", returned, request_id, done)
    if not returned:
        # Stopping the leaving node ends the REST call the decommission is
        # blocked in, which would otherwise outlive the test by its own
        # timeout; whatever error the interrupted call ends with is expected.
        await manager.server_stop(leaving.server_id, convict=False)
        await asyncio.gather(decommission, return_exceptions=True)
    assert returned and all(done.values()), \
        f"decommission returned: {returned}, system.topology_requests done flags: {done}"

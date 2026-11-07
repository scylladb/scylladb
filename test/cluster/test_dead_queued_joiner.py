#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import json
import logging
import uuid

import pytest

from test.cluster.util import get_coordinator_host, get_non_coordinator_host
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

PAUSE_COORDINATOR = "topology_coordinator_pause_before_processing_backlog"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-6089",
                      reason="a dead queued joiner makes every later join fail")
async def test_dead_queued_joiner_does_not_block_other_joins(manager: ScyllaClusterManager) -> None:
    """
    Idea: a node can die while its join request waits in the topology
    coordinator's queue. That must not break the next join: the dead request
    should be skipped, expired or handled first, not make a healthy joiner
    fail.

    Today, when the coordinator processes any joiner, it sends wait_for_ip to
    every node, and each node waits until it can map every host in `new_nodes`
    to an IP, the dead one included. A node that never got the dead joiner's
    gossip cannot: it was down or isolated for the dead joiner's whole life,
    and once the wiped joiner is started again on the same IP with a new host
    id, gossip digests, which are keyed by IP and carry the newest generation
    only, never bring the old host id. After 30 s wait_for_ip fails and the
    healthy joiner is rejected with "failed to obtain an IP for <dead>".

    Setup: 3 nodes, A the topology coordinator and C another node. A's
    coordinator fiber is paused before it looks at the request queue, so every
    join request placed below stays queued. C is paused (SIGSTOP) while a
    joiner starts with seeds=[A] and A places its request; the joiner is then
    killed and wiped, and started again on the same IP, the way an operator
    restarts a node that did not get in. C resumes once the last incarnation's
    request is placed, so C learns that one and never the dead ones. The
    coordinator fiber is then released.

    The queued join with the lowest host id is processed last (system.topology
    rows are read in host id order into a map that iterates its first entry
    last), so the live joiner is processed while a dead one is still queued
    only if its host id does not sort lowest; the joiner is killed and wiped
    again until that holds.

    Checks that the live joiner joins.
    """
    servers = await manager.servers_add(3)
    a = await get_coordinator_host(manager)
    c = await get_non_coordinator_host(manager)
    log_a = await manager.server_open_log(a.server_id)
    # C cannot map a dead joiner, so its wait_for_ip for one fails whenever the coordinator gets to it.
    manager.ignore_log_patterns.append(r"raft_topology_cmd wait_for_ip failed with")

    await manager.api.enable_injection(a.ip_addr, PAUSE_COORDINATOR, one_shot=True)

    joiner = await manager.server_add(start=False)
    log_joiner = await manager.server_open_log(joiner.server_id)

    async def start_and_place() -> uuid.UUID:
        """Start the joiner on its IP and return the host id A placed a join request for."""
        mark = await log_a.mark()
        await manager.server_start(joiner.server_id, seeds=[a.ip_addr], connect_driver=False,
                                   expected_server_up_state=ServerUpState.PROCESS_STARTED)
        _, [(_, match)] = await log_a.wait_for(r"placed join request for (\S+)", from_mark=mark, timeout=120)
        return uuid.UUID(match.group(1))

    await manager.server_pause(c.server_id)
    dead: list[uuid.UUID] = []
    live = await start_and_place()
    for _ in range(6):
        await manager.server_stop(joiner.server_id, convict=False)  # SIGKILL while its request is queued
        dead.append(live)
        await manager.server_wipe(joiner.server_id)
        live = await start_and_place()
        if live > min(dead):
            break
    else:
        pytest.fail(f"six host ids in a row each sorted below the earlier ones: {dead + [live]}")
    logger.info("dead queued joiners %s, live joiner %s", dead, live)

    await manager.server_unpause(c.server_id)
    await manager.server_sees_other_server(c.ip_addr, joiner.ip_addr)
    for s in servers:
        if s.server_id != c.server_id:
            await manager.server_sees_other_server(s.ip_addr, c.ip_addr)
    known = json.dumps(await manager.api.client.get_json("/failure_detector/endpoints/", host=c.ip_addr))
    assert str(live) in known and not any(str(x) in known for x in dead), \
        f"C should know {live} and none of {dead}: {known}"

    await manager.api.message_injection(a.ip_addr, PAUSE_COORDINATOR)
    _, [(line, _)] = await log_joiner.wait_for(
        r"initialization completed|rejected request to join the cluster.*", timeout=300)
    assert "initialization completed" in line, \
        f"the live joiner {live} was rejected because of the dead queued joiner(s) {dead}: {line.strip()}"

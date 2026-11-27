#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""removenode of a dead node while the topology coordinator is stuck on it (network proxy
harness, test/pylib/network_proxy.py).

The test talks CQL to the coordinator through a session pinned to it (`session`); CQL goes
through the proxy too, but the rules here act on the internode ports only.
"""

from __future__ import annotations

import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.network.util import wait_for_down
from test.cluster.util import ensure_group0_leader_on
from test.pylib.internal_types import ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The bounds below (120s for removenode) are tuned for dev builds.
pytestmark = pytest.mark.skip_mode(mode='debug', reason='timing bounds are tuned for dev builds')

# The driver-side timeout of a statement; a statement running into it has hung past every server-side bound.
CLIENT_TIMEOUT_S = 60
RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


def stmt(text: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(text, consistency_level=cl, retry_policy=FallthroughRetryPolicy())


async def session(manager: ScyllaClusterManager, server: ServerInfo):
    """A session that talks to `server` only, with no schema agreement wait after a DDL (which
    would stall on a cut-off node). A proxied node advertises its proxy's address, so the
    whitelist names that one (`get_cql_exclusive` whitelists the real address, which the driver
    never sees, and fails). CQL goes through the proxy, which the internode rules leave alone."""
    addr = server.rpc_address
    cluster = manager.con_gen([addr], manager.port, manager.use_ssl, None, WhiteListRoundRobinPolicy([addr]))
    manager.exclusive_clusters.append(cluster)
    cql = cluster.connect()
    cql.cluster.max_schema_agreement_wait = 0
    return cql


def describe(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)[:300]}"


class RemovenodeNotDone(AssertionError):
    """removenode did not complete within the test's bound."""


TOPOLOGY_CMD_NO_TIMEOUT = (
    "the topology coordinator sends raft_topology_cmd (barrier_and_drain) without a timeout to a DOWN node that "
    "accepts connections and never answers, so the global barrier and the removenode behind it hang; issue to be filed "
    "(related: scylladb/scylladb#19151)")


@pytest.mark.parametrize("fault", ["stop", "drop"])
async def test_removenode_of_a_dead_replica_after_a_timed_out_truncate(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, fault: str) -> None:
    """A replica dies (`stop`: the process is stopped; `drop`: it stays alive behind a cut, like
    a frozen node), then a client TRUNCATEs a tablet table and gets a timeout; the truncate goes
    on as the topology coordinator's current transition, retrying its barrier on every node.
    The operator does what the docs say for a dead node: removenode. The removenode must finish,
    or fail with an error, within a bound, while the node is still dead.

    Layout: racks r1, r1, r2, r3, RF=3, so the dead r1 node's tablets can go to the other r1
    node. The group0 leader (topology coordinator) is the r2 node.

    `drop` fails today: the truncate's global barrier sends raft_topology_cmd to the dead node
    with no timeout, on a connection the cut accepts and never answers, so the coordinator never
    gets to the removenode. A stopped node refuses the connection and is removed at once.
    """
    truncate_timeout_s = 10
    config = {"truncate_request_timeout_in_ms": truncate_timeout_s * 1000,
              "tablets_mode_for_new_keyspaces": "enabled",
              "failure_detector_timeout_in_ms": failure_detector_timeout}
    servers = await manager.servers_add(4, config=config, cmdline=["--logger-log-level", "raft_topology=debug"],
                                        property_file=[{"dc": "dc1", "rack": r} for r in ("r1", "r1", "r2", "r3")])
    await manager.get_ready_cql(servers)
    _, victim, coord, _ = servers
    await ensure_group0_leader_on(manager, coord)
    cql = await session(manager, coord)
    ks = "trunc_rm_ks"
    await cql.run_async(f"CREATE KEYSPACE {ks} {RF3} AND tablets = {{'initial': 4}}")
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
    for k in range(20):
        await cql.run_async(stmt(f"INSERT INTO {ks}.t (pk, v) VALUES ({k}, {k})", ConsistencyLevel.ALL))
    victim_id = await manager.get_host_id(victim.server_id)
    others = [s for s in servers if s != victim]

    if fault == "stop":
        await manager.server_stop(victim.server_id, convict=False)
    else:
        await network_proxy.isolate(victim.ip_addr, [s.ip_addr for s in others])
    for s in others:
        await wait_for_down(manager, network_proxy, s, {victim.ip_addr})

    log = await manager.server_open_log(coord.server_id)
    mark = await log.mark()
    t0 = time.monotonic()
    try:
        await cql.run_async(stmt(f"TRUNCATE {ks}.t"), timeout=CLIENT_TIMEOUT_S)
        truncate_error = None
    except Exception as e:
        truncate_error = describe(e)
    logger.info(f"TRUNCATE with {victim.ip_addr} dead ({fault}): {truncate_error or 'ok'} after {time.monotonic() - t0:.1f}s")

    t0 = time.monotonic()
    bound_s = 120
    try:
        await manager.api.remove_node(coord.ip_addr, victim_id, [], timeout=bound_s)
        removenode = "done"
    except Exception as e:
        removenode = describe(e)
    took = time.monotonic() - t0
    logger.info(f"removenode of {victim.ip_addr} ({fault}): {removenode} after {took:.1f}s")
    for line, _ in (await log.grep(r"Performing TRUNCATE|coordinator fiber got error|drain rpc failed|"
                                   r"removenode|Cannot perform|executing global topology command", from_mark=mark))[-15:]:
        logger.info(f"coordinator: {line.strip()[:250]}")
    # Read before the heal, which ends the stall: the coordinator's last barrier event since the TRUNCATE.
    barrier_events = [line.strip()[:250] for line, _ in await log.grep(
        r"executing global topology command barrier_and_drain|drain rpc failed|coordinator fiber got error", from_mark=mark)]
    if fault == "drop":
        await network_proxy.heal()
    if removenode.startswith("TimeoutError") or took >= bound_s:
        message = f"removenode of a dead node after a TRUNCATE ({truncate_error or 'ok'}): {removenode} after {took:.1f}s"
        # The bug is named only when its mechanism shows: the TRUNCATE timed out and the coordinator's last barrier
        # event is a barrier_and_drain that neither failed nor was followed by a fiber error.
        if (fault == "drop" and truncate_error and "Timeout during TRUNCATE" in truncate_error
                and barrier_events and "executing global topology command barrier_and_drain" in barrier_events[-1]):
            raise RemovenodeNotDone(f"{message}; {TOPOLOGY_CMD_NO_TIMEOUT}; coordinator stuck since: {barrier_events[-1]}")
        raise RemovenodeNotDone(f"{message}; coordinator barrier events since the TRUNCATE: {barrier_events[-5:]}")
    assert removenode == "done", f"removenode of a dead node after a timed-out TRUNCATE failed after {took:.1f}s: {removenode}"

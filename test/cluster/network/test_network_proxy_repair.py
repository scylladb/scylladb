#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tablet repair with a replica cut off mid-repair (network proxy harness, test/pylib/network_proxy.py).

The tables use tablets, RF=3 on three nodes, so every node is a replica of everything.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import pytest
from cassandra import ConsistencyLevel

from test.cluster.network.util import heal_and_wait_all_up, new_table, wait_for_down
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

# The bounds below (90s for a repair master to give up) are tuned for dev builds.
pytestmark = pytest.mark.skip_mode(mode='debug', reason='timing bounds are tuned for dev builds')


def ks_opts(tablets: int) -> str:
    return ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            f" AND tablets = {{'initial': {tablets}}}")


async def boot(manager: ScyllaClusterManager, failure_detector_timeout: int) -> tuple[list[ServerInfo], Any, list]:
    """Three nodes, one rack each, short failure detector timeout, hinted handoff off.

    Ordered so that the last one is not the group0 leader (the topology coordinator): the test
    cuts that one off, and the coordinator should keep driving the repair meanwhile.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout,
              'hinted_handoff_enabled': False,
              'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(3, config=config, cmdline=['--logger-log-level', 'repair=debug'], auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    await read_barrier(manager.api, servers[0].ip_addr)
    leader = await manager.api.get_raft_leader(servers[0].ip_addr)
    ids = [await manager.get_host_id(s.server_id) for s in servers]
    order = sorted(range(3), key=lambda i: ids[i] != leader)
    logger.info(f"group0 leader {leader}; nodes {[(servers[i].ip_addr, ids[i]) for i in order]}")
    return [servers[i] for i in order], cql, [hosts[i] for i in order]


async def isolate(manager: ScyllaClusterManager, network_proxy: NetworkProxy, servers: list[ServerInfo], victim: ServerInfo,
                  **kwargs) -> None:
    """Cut `victim` off from the other nodes, both ways, and wait until both sides see it."""
    others = [s for s in servers if s != victim]
    await network_proxy.isolate(victim.ip_addr, [s.ip_addr for s in others], **kwargs)
    await wait_for_down(manager, network_proxy, victim, {s.ip_addr for s in others})
    for s in others:
        await wait_for_down(manager, network_proxy, s, {victim.ip_addr})


async def wait_repair_task(manager: ScyllaClusterManager, server: ServerInfo, task_id: str, timeout: float) -> dict:
    """Poll the tablet repair task until it is done or failed; fail the test if it is still running after `timeout`."""
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = await manager.api.get_task_status(server.ip_addr, task_id)
        if status['state'] in ('done', 'failed'):
            return status
        await asyncio.sleep(1)
    pytest.fail(f"tablet repair task {task_id} still {status and status['state']} after {timeout}s: {status}")


async def flush_all(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str) -> None:
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)


def incompressible() -> str:
    """A 1 kB text value that sstable compression cannot shrink much (hex of random bytes)."""
    return os.urandom(512).hex()


async def _any_log_has(logs, marks, pattern: str) -> bool | None:
    for log, mark in zip(logs, marks):
        if await log.grep(pattern, from_mark=mark):
            return True
    return None


class MasterStuck(AssertionError):
    """A repair master did not end its attempt within the test's bound after its peer was marked DOWN."""


class CompactionStuck(AssertionError):
    """A major compaction did not get the compaction lock within the test's bound."""


REPAIR_MASTER_HANGS_ON_DOWN_PEER = (
    "tablet repair master hangs on a DOWN peer that still accepts connections (repair_row_level_stop and "
    "node_ops_cmd::repair_updater have no timeout); issue to be filed (see test docstring)")
COMPACTION_LOCK_HELD_BY_FAILED_REPAIR = (
    "major compaction waits for the heal: a failed incremental tablet repair keeps the compaction lock until "
    "end_repair's global barrier passes, which needs the cut-off replica; probably by design, not filed")


@pytest.mark.parametrize("mode", [
    "drop",
    pytest.param("reset", marks=pytest.mark.xfail(strict=True, raises=CompactionStuck, reason=COMPACTION_LOCK_HELD_BY_FAILED_REPAIR)),
])
async def test_repair_master_gives_up_on_a_peer_cut_off_mid_repair(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, mode: str) -> None:
    """A tablet repair is under way, streaming the data C missed over a slow link, when C is cut
    off for good. In drop mode C's peers can still open TCP connections to it, but nothing comes
    back, as with a node that is frozen or wedged; in reset mode the connections are refused,
    the control case. A and B mark C DOWN, which closes their connections to it and fails the
    row level repair in flight.

    Checks: within a bound after C is DOWN, every repair master's attempt ends (it logs the
    tablet repair as failed), rather than waiting on C forever; and a major compaction of the
    table on A, which needs the tablet's compaction lock that the repair holds, completes
    while C is still cut off.

    Both variants fail today. In drop mode the masters keep sending C RPCs that have no timeout
    (repair_row_level_stop, node_ops_cmd::repair_updater) on new connections, which C accepts and
    never answers, so they wait until the heal; a SIGSTOPped peer does the same. In reset mode
    the masters give up, but the failed incremental repair keeps the compaction lock until
    end_repair's global barrier, which needs C, so the major compaction waits for the heal.
    """
    servers, cql, hosts = await boot(manager, failure_detector_timeout)
    a, b, c = servers
    async with new_test_keyspace(manager, ks_opts(2)) as ks:
        async with new_table(manager, ks, "pk int PRIMARY KEY, v text",
                                  extra=" WITH tombstone_gc = {'mode': 'repair'}") as table:
            cf = table.split('.')[1]
            await isolate(manager, network_proxy, servers, c)
            stmt = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            stmt.consistency_level = ConsistencyLevel.QUORUM
            for start in range(0, 3000, 100):
                await asyncio.gather(*(cql.run_async(stmt, [pk, incompressible()], host=hosts[pk % 2])
                                       for pk in range(start, start + 100)))
            await flush_all(manager, servers, ks)
            await heal_and_wait_all_up(manager, network_proxy, servers)

            await network_proxy.bandwidth(c.ip_addr, 64)
            logs = [await manager.server_open_log(s.server_id) for s in servers]
            marks = [await log.mark() for log in logs]
            response = await manager.api.tablet_repair(a.ip_addr, ks, cf, "all", await_completion=False, incremental_mode='incremental')
            await wait_for(lambda: _any_log_has(logs, marks, "Repair 1 out of"), time.time() + 60, period=0.5,
                           label="row level repair to start")
            await asyncio.sleep(3)
            # The tablet repair masters are the nodes that logged the start of a tablet repair.
            masters = [i for i, (log, mark) in enumerate(zip(logs, marks)) if await log.grep("Repair 1 out of 1 tablets", from_mark=mark)]
            logger.info(f"Tablet repair masters: {[servers[i].ip_addr for i in masters]}")
            assert any(servers[i] != c for i in masters), "no tablet repair master other than C to check"
            await network_proxy.heal()
            await isolate(manager, network_proxy, servers, c, mode=mode)
            down_at = time.time()

            bound = 90
            for i in masters:
                if servers[i] == c:
                    continue
                try:
                    await logs[i].wait_for(r"Repair tablet for keyspace=\S+ status=(failed|succeeded)", from_mark=marks[i],
                                           timeout=max(1, bound - (time.time() - down_at)))
                except TimeoutError:
                    raise MasterStuck(f"the repair master {servers[i].ip_addr} is still in its tablet repair {bound}s after "
                                      f"{c.ip_addr} was marked DOWN"
                                      + (f"; {REPAIR_MASTER_HANGS_ON_DOWN_PEER}" if mode == "drop" else "")) from None
            try:
                await asyncio.wait_for(manager.api.keyspace_compaction(a.ip_addr, ks, cf), timeout=60)
            except TimeoutError:
                raise CompactionStuck(f"major compaction on {a.ip_addr} did not finish in 60s while {c.ip_addr} is cut off") from None

            await heal_and_wait_all_up(manager, network_proxy, servers)
            status = await wait_repair_task(manager, a, response['tablet_task_id'], 240)
            assert status['state'] == 'done', f"repair failed after the heal: {status}"

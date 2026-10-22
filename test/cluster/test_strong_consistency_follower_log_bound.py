#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A strongly consistent follower whose state machine stalls bounds its raft log.

A follower refuses entries beyond max_follower_log_size (max_log_size +
snapshot_trailing_size) and reports a full log, so that its leader stops
replicating to it until it has room again.  The test stalls apply() on one
follower of a strongly consistent tablet and writes until it hits the bound.

The log is shrunk with sc_raft_max_log_size, so that a hundred writes fill
it instead of the 21MB a production tablet group holds.
"""

from __future__ import annotations

import contextlib
import logging
import time
from typing import AsyncGenerator

import pytest

from test.cluster.util import FeatureConfigurations, new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

LOG_SIZE_INJECTION = "sc_raft_max_log_size"
STALL_APPLY_INJECTION = "strong_consistency_state_machine_wait_before_apply"

MAX_LOG_SIZE = 1024 * 1024
# The injection sets snapshot_trailing_size to a quarter of max_log_size.
SNAPSHOT_TRAILING_SIZE = MAX_LOG_SIZE // 4
# fsm_config::max_follower_log_size, see raft::server_impl::start().
MAX_FOLLOWER_LOG_SIZE = MAX_LOG_SIZE + SNAPSHOT_TRAILING_SIZE

# Large enough that a hundred writes fill the follower, small enough to be far
# below max_command_size.  Like real writes, the entries do not divide the
# bound exactly.
PAYLOAD_SIZE = 16 * 1024
# More than the follower can take.
OVERFILL_WRITES = 2 * MAX_FOLLOWER_LOG_SIZE // PAYLOAD_SIZE

SC_FEATURE = FeatureConfigurations.STRONG_CONSISTENCY.value
METRIC_PREFIX = "scylla_strong_consistency_raft_"


class StalledFollower:
    """A three-node tablet group whose follower `victim`, the last node, does not apply entries."""

    def __init__(self, manager: ScyllaClusterManager, cql, ks: str,
                 servers: list[ServerInfo]) -> None:
        self.manager = manager
        self.cql = cql
        self.ks = ks
        self.victim = servers[-1]
        self.others = servers[:-1]
        self.written = 0
        self.payload = bytes(PAYLOAD_SIZE)
        self.write_stmt = cql.prepare(f"UPDATE {ks}.t SET v = ? WHERE pk = ?")

    async def write(self, count: int) -> None:
        for _ in range(count):
            await self.cql.run_async(self.write_stmt, (self.payload, self.written % 100))
            self.written += 1

    async def raft_state(self, server: ServerInfo) -> dict:
        metrics = await self.manager.metrics.query(server.ip_addr)
        labels = {"ks": self.ks, "cf": "t"}

        def gauge(name: str, **extra) -> float:
            value = metrics.get(METRIC_PREFIX + name, labels | extra)
            assert value is not None, f"no {METRIC_PREFIX}{name} for {labels | extra} on {server.ip_addr}"
            return value

        return {
            "log_memory_usage": gauge("log_memory_usage"),
            "full_logs": gauge("full_logs"),
            "unapplied_entries": gauge("unapplied_entries"),
            "blocked_log_full": gauge("blocked_followers", reason="log_full"),
        }

    async def stall(self) -> None:
        await self.manager.api.enable_injection(self.victim.ip_addr, STALL_APPLY_INJECTION, one_shot=False)

    async def resume(self) -> None:
        # Disabling an injection which is not enabled is a no-op.
        await self.manager.api.disable_injection(self.victim.ip_addr, STALL_APPLY_INJECTION)


@contextlib.asynccontextmanager
async def stalled_follower(manager: ScyllaClusterManager) -> AsyncGenerator[StalledFollower, None]:
    """Start the cluster and the table, with apply() stalled on the last node.

    Resumes apply() on the way out, whatever the test did: a raft group
    cannot be torn down while its applier waits on the injection.
    """
    limits = {"name": LOG_SIZE_INJECTION, "value": str(MAX_LOG_SIZE)}
    config = SC_FEATURE.get_cluster_cfg({"error_injections_at_startup": [limits]})
    # The stalled node has to be a follower: a stalled leader is throttled by
    # its own log_limiter_semaphore, not by the follower bound.
    victim_config = SC_FEATURE.get_cluster_cfg(
        {"error_injections_at_startup": [limits, "avoid_being_raft_leader"]})

    servers = await manager.servers_add(2, config=config, auto_rack_dc="dc1")
    servers.append(await manager.server_add(config=victim_config, property_file={"dc": "dc1", "rack": "rack3"}))
    cql, _ = await manager.get_ready_cql(servers)

    # One tablet, RF=3: one raft group, and every node is a member.
    opts = SC_FEATURE.get_keyspace_opts(
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
        "AND tablets = {'initial': 1}")
    async with new_test_keyspace(manager, opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v blob)")

        group = StalledFollower(manager, cql, ks, servers)
        await group.stall()
        try:
            yield group
        finally:
            await group.resume()


async def wait_for_state(group: StalledFollower, server: ServerInfo, check, timeout: float, label: str) -> dict:
    """Poll the raft state of `server` until `check(state)` holds."""
    async def state_ok():
        state = await group.raft_state(server)
        return state if check(state) else None
    return await wait_for(state_ok, time.time() + timeout, label=label)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_full_follower_is_reported_to_the_leader(manager: ScyllaClusterManager, scale_timeout) -> None:
    """A follower which stops accepting entries says so, and its leader stops sending them."""
    async with stalled_follower(manager) as group:
        while (state := await group.raft_state(group.victim))["full_logs"] == 0:
            # Also what fails if the injection did not take effect: the
            # production bound is far above what this test writes.
            assert state["log_memory_usage"] <= MAX_FOLLOWER_LOG_SIZE, (
                f"{group.victim.ip_addr} log is past the bound {MAX_FOLLOWER_LOG_SIZE}, "
                f"is {LOG_SIZE_INJECTION} in effect? {state}")
            if group.written >= OVERFILL_WRITES:
                pytest.fail(f"{group.victim.ip_addr} did not report a full log after {group.written} writes "
                            f"of {PAYLOAD_SIZE} bytes, bound {MAX_FOLLOWER_LOG_SIZE}: {state}")
            await group.write(1)
        logger.info("%s reported a full log after %d writes: %s", group.victim.ip_addr, group.written, state)

        # The leader learns it from the follower's next append_reply.
        async def leader_blocked():
            blocked = sum([(await group.raft_state(s))["blocked_log_full"] for s in group.others])
            return True if blocked == 1 else None
        await wait_for(leader_blocked, time.time() + scale_timeout(30),
                       label="the leader stopping replication to the full follower")

        await group.resume()
        await wait_for_state(group, group.victim, lambda s: s["full_logs"] == 0, scale_timeout(60),
                             label=f"{group.victim.ip_addr} accepting entries again")

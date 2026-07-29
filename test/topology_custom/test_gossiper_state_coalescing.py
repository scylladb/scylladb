#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Coalescing pending gossip states must not lose application states.

The gossiper keeps at most one pending state per endpoint and merges every
arrival into it. Merging, rather than keeping whichever arrival has the
highest version, is what makes that safe: a node summarises everything it
knows about a peer as a single max version, and asks other nodes only for
values above it. A value dropped below that mark is therefore never
re-requested by anyone and is lost for good -- gossip has no re-delivery
safety net for it.

This test stalls state application on one node so that arrivals pile up and
coalesce, churns application states meanwhile, and then checks that the node
still converges on every peer's own view of itself.

Runtime: ~30s (cluster start ~12s, stall 8s, convergence a few seconds).
"""

import asyncio
import logging
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

STALL_INJECTION = "gossiper_stall_apply_state_locally"

NODES = 4
# Long enough for many gossip rounds from every peer to pile up behind the
# stalled applier, so arrivals actually collide in the pending slot.
STALL_SECONDS = 8
CONVERGE_TIMEOUT = 90


async def endpoint_states(manager: ScyllaClusterManager, node_ip: str) -> dict[str, dict[int, int]]:
    """What `node_ip` currently knows: {peer ip: {application state: version}}."""
    raw = await manager.api.client.get_json("/failure_detector/endpoints/", host=node_ip)
    return {
        eps["addrs"]: {int(a["application_state"]): int(a["version"])
                       for a in eps.get("application_state", [])}
        for eps in raw
    }


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_gossiper_coalescing_does_not_lose_application_states(manager: ScyllaClusterManager) -> None:
    # gossip=debug so we can confirm the coalescing path was actually taken.
    servers = await manager.servers_add(NODES, cmdline=['--logger-log-level=gossip=debug'])
    victim, peers = servers[0], servers[1:]
    victim_log = await manager.server_open_log(server_id=victim.server_id)
    log_mark = await victim_log.mark()

    cql = manager.get_cql()

    await manager.api.enable_injection(victim.ip_addr, STALL_INJECTION, one_shot=False)
    try:
        await manager.api.wait_for_injection_enter(victim.ip_addr, STALL_INJECTION)

        # Churn application states while the victim cannot apply anything, so
        # the states queued behind the stall carry real values rather than
        # bare heartbeats. A schema change bumps SCHEMA on every node.
        await cql.run_async("CREATE KEYSPACE IF NOT EXISTS coalesce_ks WITH replication = "
                            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        deadline = time.time() + STALL_SECONDS
        i = 0
        while time.time() < deadline:
            await cql.run_async(f"CREATE TABLE coalesce_ks.t{i} (pk int PRIMARY KEY, v int)")
            i += 1
            await asyncio.sleep(1)

        # The whole point of the test is the coalescing path; fail loudly
        # rather than silently passing if it was never taken.
        coalesced = await victim_log.grep("queue_state_apply: coalesced a state of",
                                          from_mark=log_mark)
        assert coalesced, ("no gossip state was coalesced on the victim; the test did not "
                           "exercise the path it is meant to cover")

        # Each peer is authoritative about itself, so its own entry is the
        # truth the victim has to converge on.
        expected: dict[str, dict[int, int]] = {}
        for peer in peers:
            expected[peer.ip_addr] = (await endpoint_states(manager, peer.ip_addr))[peer.ip_addr]
    finally:
        await manager.api.message_injection(victim.ip_addr, STALL_INJECTION)
        await manager.api.disable_injection(victim.ip_addr, STALL_INJECTION)

    async def behind() -> list[str]:
        """Application states the victim is still missing or stale on."""
        seen = await endpoint_states(manager, victim.ip_addr)
        out = []
        for peer_ip, want in expected.items():
            got = seen.get(peer_ip, {})
            for state, version in want.items():
                if got.get(state, -1) < version:
                    out.append(f"{peer_ip} state={state} want>={version} got={got.get(state, 'absent')}")
        return out

    async def converged() -> None:
        while await behind():
            await asyncio.sleep(0.5)

    try:
        await asyncio.wait_for(converged(), timeout=CONVERGE_TIMEOUT)
    except asyncio.TimeoutError:
        pytest.fail("the victim never caught up on application states that were coalesced "
                    "while its applier was stalled; a value dropped below its advertised max "
                    "version is never re-delivered (scylladb/scylladb#10967): "
                    + "; ".join(await behind()))

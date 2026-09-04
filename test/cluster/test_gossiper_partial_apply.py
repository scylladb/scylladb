#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
A gossip state must be applied in full or not at all.

apply_new_states() raises the heartbeat on its working copy before copying the
application states one by one, and every copy allocates. If a copy fails, the
partially built state must not be stored: it would advertise a max version
above application states that were never received, and a digest carries a
single max version per endpoint, so no peer ever re-sends a value below it.
The value would be lost for good.

Rethrowing before replicate() leaves our state, and therefore our digest,
untouched, so the next gossip exchange re-delivers the whole delta.

Runtime: ~10s.
"""

import asyncio
import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

APPLY_FAIL_INJECTION = "apply_new_states_fail"
# gms::application_state::SCHEMA -- the state the injected failure drops.
SCHEMA = 2

NODES = 3
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
async def test_gossiper_partially_applied_state_is_not_stored(manager: ScyllaClusterManager) -> None:
    servers = await manager.servers_add(NODES, cmdline=['--logger-log-level=gossip=debug'])
    victim, peers = servers[0], servers[1:]
    victim_log = await manager.server_open_log(server_id=victim.server_id)
    log_mark = await victim_log.mark()

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE IF NOT EXISTS partial_ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    table_seq = 0

    async def ddl() -> None:
        """Bump SCHEMA on every node."""
        nonlocal table_seq
        await cql.run_async(f"CREATE TABLE partial_ks.t{table_seq} (pk int PRIMARY KEY, v int)")
        table_seq += 1

    # Fail the next SCHEMA copy on the victim, partway through applying a delta.
    await manager.api.enable_injection(victim.ip_addr, APPLY_FAIL_INJECTION, one_shot=True)

    # Keep bumping SCHEMA until the victim actually hits the injected failure.
    async def until_injected() -> None:
        while not await victim_log.grep("injected apply failure", from_mark=log_mark):
            await ddl()
            await asyncio.sleep(1)

    await asyncio.wait_for(until_injected(), timeout=120)
    await manager.api.disable_injection(victim.ip_addr, APPLY_FAIL_INJECTION)

    # Stop issuing DDL, then snapshot each peer's own SCHEMA version. Nothing
    # bumps SCHEMA from here on, so only re-delivery can bring the victim up to
    # date -- which is exactly what a torn, already-stored state prevents.
    expected: dict[str, int] = {}
    for peer in peers:
        expected[peer.ip_addr] = (await endpoint_states(manager, peer.ip_addr))[peer.ip_addr][SCHEMA]

    async def behind() -> list[str]:
        seen = await endpoint_states(manager, victim.ip_addr)
        return [f"{peer_ip} SCHEMA want>={version} got={seen.get(peer_ip, {}).get(SCHEMA, 'absent')}"
                for peer_ip, version in expected.items()
                if seen.get(peer_ip, {}).get(SCHEMA, -1) < version]

    async def converged() -> None:
        while await behind():
            await asyncio.sleep(0.5)

    try:
        await asyncio.wait_for(converged(), timeout=CONVERGE_TIMEOUT)
    except asyncio.TimeoutError:
        pytest.fail("a partially applied gossip state was stored: the victim advertises a max "
                    "version above a SCHEMA value it never received, so no peer re-sends it: "
                    + "; ".join(await behind()))

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Reproducer for the unbounded gossiper apply backlog
(https://github.com/scylladb/scylladb/issues/10967).

Gossip state application on one node is stalled via error injection while its
peers keep gossiping: every round carries fresh heartbeat/application state
versions, and unapplied states never advance the node's digests, so peers keep
re-sending. Every received endpoint state is queued behind the apply
concurrency semaphore holding a full endpoint_state copy, so the backlog grows
without bound for as long as application cannot keep up. In the field this
takes down nodes: during a zonal outage a node that cannot keep up applying
gossip states accumulates millions of queued states and dies of OOM.

The backlog size is read from the gossiper's debug log ("gossip state-apply
backlog: N"), logged whenever received states are queued. The test asserts the
peak stays bounded. It fails on an unfixed version (the backlog grows
monotonically for as long as the stall lasts) and passes with per-endpoint
coalescing, where the backlog is bounded by the number of endpoints in the
cluster.

Runtime: ~40s (cluster start dominates); the measurement window is 15s.
"""

import asyncio
import logging
import re
import time

import pytest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

BACKLOG_RE = re.compile(r"gossip state-apply backlog: (\d+)")
STALL_INJECTION = "gossiper_stall_apply_state_locally"

# Enough nodes that the victim receives states far faster than the bound below,
# so an unbounded backlog blows past it within the measurement window: each of
# the NODES-1 peers gossips once a second carrying up to NODES endpoint states.
NODES = 5
# Coalescing bounds the backlog by the number of endpoints. Leave generous
# headroom so the assertion only trips on unbounded growth, not on jitter.
BOUND = 4 * NODES
# ~15 gossip rounds from every peer, i.e. some hundreds of received states.
MEASURE_SECONDS = 15


@pytest.mark.xfail(reason="scylladb/scylladb#10967: the gossiper apply backlog is unbounded; "
                          "fixed by per-endpoint coalescing of pending states")
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_gossiper_apply_backlog_is_bounded(manager: ManagerClient) -> None:
    servers = await manager.servers_add(NODES, cmdline=['--logger-log-level=gossip=debug'])
    victim = servers[0]
    victim_log = await manager.server_open_log(server_id=victim.server_id)
    log_mark = await victim_log.mark()

    async def peak_backlog() -> tuple[int, int]:
        """Highest backlog logged since log_mark, and the number of samples."""
        matches = await victim_log.grep(BACKLOG_RE, from_mark=log_mark)
        values = [int(m.group(1)) for _, m in matches]
        return (max(values) if values else 0), len(values)

    # Stall all gossip state application on the victim while its peers keep
    # gossiping. Everything received while application is stalled counts
    # into the backlog.
    await manager.api.enable_injection(victim.ip_addr, STALL_INJECTION, one_shot=False)
    try:
        # Make sure the stall actually engaged, so the assertion below cannot
        # pass vacuously with a disconnected injection point.
        await manager.api.wait_for_injection_enter(victim.ip_addr, STALL_INJECTION)

        # Likewise, make sure states are actually accumulating behind the
        # stalled application before measuring the bound.
        async def backlog_nonempty() -> None:
            while (await peak_backlog())[0] < 1:
                await asyncio.sleep(0.1)

        await asyncio.wait_for(backlog_nonempty(), timeout=60)

        # A bounded implementation keeps the backlog at O(number of endpoints)
        # no matter how many states arrive; an unbounded one accumulates every
        # re-delivery and crosses BOUND within a few seconds.
        deadline = time.time() + MEASURE_SECONDS
        peak = 0
        samples = 0
        while time.time() < deadline:
            peak, samples = await peak_backlog()
            assert peak <= BOUND, (
                f"gossip apply backlog reached {peak} (> {BOUND}); "
                f"the backlog is unbounded (scylladb/scylladb#10967)")
            await asyncio.sleep(0.5)
        # Guard against the log line being renamed or the logger level not
        # taking effect: a passing run must have actually observed samples.
        assert samples > 0, "no backlog samples seen in the victim's log"
        logger.info("gossip apply backlog peaked at %d over %d samples (bound %d)",
                    peak, samples, BOUND)
    finally:
        # Unstick the stalled appliers so shutdown is clean.
        await manager.api.message_injection(victim.ip_addr, STALL_INJECTION)
        await manager.api.disable_injection(victim.ip_addr, STALL_INJECTION)

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
When a replica sets the split-ready sequence number after the coordinator's own replica did,
the split must be finalized promptly, not at the next periodic tablet load stats refresh.
"""

import asyncio
import logging
import re
import time
from datetime import datetime

import pytest

from test.cluster.util import new_test_keyspace, get_coordinator_host
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)

DEFAULT_REFRESH_INTERVAL = 60
SPLIT_DEADLINE = 20
HOLD_AFTER_COORDINATOR_READY = 3
TS_RE = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})')


def log_ts(line: str) -> float:
    return datetime.strptime(TS_RE.search(line).group(1), '%Y-%m-%d %H:%M:%S,%f').timestamp()


async def wait_for_refresh_phase(manager, coordinator, phase: float, interval: int) -> float:
    """Sleep until `phase` seconds after a periodic refresh tick of the coordinator.

    The refresher triggers once when the coordinator starts and then sleeps `interval`
    between runs, so the first "Refreshed table load stats for all DC(s)" line in the
    coordinator's log marks tick 0 (drift per tick is a few ms). Returns that tick time.
    """
    log = await manager.server_open_log(coordinator.server_id)
    first = await log.grep(r'Refreshed table load stats for all DC\(s\)', max_count=1)
    assert first, "no load stats refresh found in the coordinator log"
    tick0 = log_ts(first[0][0])
    elapsed = time.time() - tick0
    target = tick0 + (int(max(0, elapsed - phase) // interval) + 1) * interval + phase
    await asyncio.sleep(max(0, target - time.time()))
    return tick0


@pytest.mark.skip_mode(mode='release', reason='debug logs and phase control need dev/debug builds')
@pytest.mark.parametrize("refresh_interval,phase", [
    pytest.param(DEFAULT_REFRESH_INTERVAL, 5, id="default_interval_phase5"),
    pytest.param(1, None, id="control_1s_interval"),
])
async def test_split_finalizes_without_waiting_for_stats_refresh(manager: ScyllaClusterManager, refresh_interval: int,
                                                                  phase: float | None):
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    config = {'tablet_load_stats_refresh_interval_in_seconds': refresh_interval}
    servers = [await manager.server_add(config=config, cmdline=cmdline,
                                        property_file={'dc': 'dc1', 'rack': f'r{i}'}) for i in range(1, 4)]

    cql = manager.get_cql()
    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int) WITH tablets = {{'min_tablet_count': 2}}")
        for i in range(100):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({i}, {i})")
        assert await get_tablet_count(manager, servers[0], ks, 't') == 2

        coordinator = await get_coordinator_host(manager)
        if phase is not None:
            await wait_for_refresh_phase(manager, coordinator, phase, refresh_interval)
        else:
            await asyncio.sleep(5)

        logs = {s.server_id: await manager.server_open_log(s.server_id) for s in servers}
        marks = {sid: await log.mark() for sid, log in logs.items()}

        # Hold the split monitor on the non-coordinator nodes so that their replicas become
        # split-ready only after the coordinator's own replica did (and its stats refresh ran).
        others = [s for s in servers if s.server_id != coordinator.server_id]
        for s in others:
            await manager.api.enable_injection(s.ip_addr, 'tablet_split_monitor_wait', False)

        t_alter = time.time()
        await cql.run_async(f"ALTER TABLE {ks}.t WITH tablets = {{'min_tablet_count': 4}}")
        await logs[coordinator.server_id].wait_for(r'Setting split ready sequence number to 1',
                                                   from_mark=marks[coordinator.server_id], timeout=30)
        await asyncio.sleep(HOLD_AFTER_COORDINATOR_READY)
        for s in others:
            await manager.api.message_injection(s.ip_addr, 'tablet_split_monitor_wait')
        deadline = t_alter + 120
        while await get_tablet_count(manager, servers[0], ks, 't') != 4:
            assert time.time() < deadline, "split did not complete within 120 s"
            await asyncio.sleep(0.2)
        elapsed = time.time() - t_alter

        split_ready, finalized, refreshes = [], [], []
        for sid, log in logs.items():
            split_ready += [log_ts(l) for l, _ in await log.grep(r'Setting split ready sequence number', from_mark=marks[sid])]
            finalized += [log_ts(l) for l, _ in await log.grep(r'Finalizing resize decision', from_mark=marks[sid])]
            refreshes += [log_ts(l) for l, _ in await log.grep(r'Refreshed table load stats for all DC', from_mark=marks[sid])]
        last_ready = max(split_ready) if split_ready else None
        first_fin = min(finalized) if finalized else None
        gap = first_fin - last_ready if last_ready and first_fin else None
        logger.info("RESULT interval=%ss phase=%s: ALTER at %.3f, last split-ready at +%s s, "
                    "finalizing at +%s s, idle gap %s s, stats refreshes at %s, split took %.1f s",
                    refresh_interval, phase, t_alter,
                    f"{last_ready - t_alter:.2f}" if last_ready else None,
                    f"{first_fin - t_alter:.2f}" if first_fin else None,
                    f"{gap:.2f}" if gap is not None else None,
                    [f"+{r - t_alter:.2f}" for r in sorted(set(refreshes))], elapsed)

        assert elapsed < SPLIT_DEADLINE, (
            f"split took {elapsed:.1f} s (limit {SPLIT_DEADLINE} s) with "
            f"tablet_load_stats_refresh_interval_in_seconds={refresh_interval}; "
            f"all replicas were split-ready at +{last_ready - t_alter if last_ready else float('nan'):.1f} s but the "
            f"resize was finalized only at +{first_fin - t_alter if first_fin else float('nan'):.1f} s")

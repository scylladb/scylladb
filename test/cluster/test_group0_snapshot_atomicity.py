#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tests for the atomicity of group0 snapshot application.

group0_state_machine::transfer_snapshot() applies a snapshot to disk and then
holds the read_apply mutex until load_snapshot() reloads the in-memory state,
so that the two form a single critical section. If the Raft FSM rejects the
snapshot as outdated (or its application fails), load_snapshot() is never
called; in that case raft_rpc::apply_snapshot() must call
abort_snapshot_transfer() to release the mutex. Otherwise the mutex would leak
and every subsequent group0 operation on the node would hang.
"""
import time
import logging

import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.util import trigger_snapshot, create_new_test_keyspace

logger = logging.getLogger(__name__)


async def _keyspace_visible_on(cql, host, keyspace: str) -> bool:
    rows = await cql.run_async(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
        parameters=(keyspace,), host=host)
    return len(rows) > 0


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_rejected_snapshot_transfer_releases_read_apply_mutex(manager: ManagerClient):
    """
    Reproducer for the read_apply mutex leak when a transferred group0 snapshot
    is rejected by the Raft FSM.

    1. Start a 3-node cluster with reduced snapshot thresholds.
    2. Stop one node D and, while it is down, create a keyspace on the other
       nodes and trigger a snapshot so D's Raft log is truncated and D will need
       a snapshot to catch up.
    3. Arm a one-shot `group0_snapshot_transfer_force_reject` injection on D so
       that the first transferred snapshot is applied to disk but then rejected,
       exercising abort_snapshot_transfer().
    4. Start D. The first snapshot transfer is rejected; the leader retries and
       the second transfer succeeds only if the read_apply mutex was released.
    5. Verify D catches up (sees the keyspace) within a bounded time and remains
       able to apply new group0 commands. Before the fix, D hangs forever holding
       the read_apply mutex.
    """
    cmdline = ['--smp=2', '--logger-log-level', 'raft=debug']
    servers = await manager.servers_add(3, cmdline=cmdline)
    cql = manager.get_cql()

    server_d = servers[2]
    others = servers[:2]

    # Reduce snapshot thresholds so a snapshot is created (and the log truncated)
    # after just a few group0 commands.
    for s in servers:
        await manager.api.enable_injection(
            s.ip_addr, "raft_server_set_snapshot_thresholds", one_shot=False,
            parameters={'snapshot_threshold': '3', 'snapshot_trailing': '1'})

    # Stop D. It will have to catch up via a snapshot when it comes back.
    await manager.server_stop_gracefully(server_d.server_id)

    # Create a keyspace while D is down, and make sure the log gets truncated so
    # D cannot catch up via plain log replication.
    ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    for s in others:
        await trigger_snapshot(manager, s)

    # Arm the one-shot force-reject injection on D before it starts, so that the
    # first *remote* snapshot transfer (the catch-up) is rejected after being
    # applied to disk. The local snapshot load during startup does not go through
    # raft_rpc::apply_snapshot, so it will not consume the one-shot injection.
    await manager.server_update_config(
        server_d.server_id, "error_injections_at_startup",
        [{"name": "group0_snapshot_transfer_force_reject", "one_shot": True}])

    await manager.server_start(server_d.server_id)
    log = await manager.server_open_log(server_d.server_id)

    # The first transfer must have been rejected and the mutex released.
    await log.wait_for("aborting pending group0 snapshot transfer .* releasing read_apply mutex")

    # If the mutex was leaked, the leader's retried snapshot transfer would block
    # forever in hold_read_apply_mutex and D would never see the keyspace.
    cql_d = await manager.get_cql_exclusive(server_d)
    hosts_d = await wait_for_cql_and_get_hosts(cql_d, [server_d], time.time() + 60)
    host_d = hosts_d[0]

    async def keyspace_present():
        return (await _keyspace_visible_on(cql_d, host_d, ks)) or None
    await wait_for(keyspace_present, time.time() + 120)

    # Also verify D can still apply *new* group0 commands (apply() takes the same
    # read_apply mutex), i.e. the mutex is genuinely free rather than momentarily
    # available.
    ks2 = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    async def keyspace2_present():
        return (await _keyspace_visible_on(cql_d, host_d, ks2)) or None
    await wait_for(keyspace2_present, time.time() + 120)

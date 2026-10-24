#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A group0 operation's timeout has to cover the group0 mutexes that
raft_group0_client::start_operation() takes, not only the read barrier between them.

The test parks a keyspace_rf_change on the topology coordinator while it holds the group0
guard, using the wait-before-committing-rf-change-event injection, and issues a CREATE
TABLE on the same node. The DDL carries group0_raft_op_timeout_in_ms, so it has to fail
with the group0 operation mutex timeout within that budget instead of waiting until the
holder lets go. No quorum loss is needed: the injection makes the holder slow on purpose.
"""

import asyncio
import logging
import time

import pytest

from test.cluster.util import get_topology_coordinator
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

GROUP0_OP_TIMEOUT_MS = 5000
# How long the holder keeps the guard: well above the timeout, and well below the
# injection's own 30s wait_for_message window, past which the node aborts.
HOLD_S = 15

INJECTION = "wait-before-committing-rf-change-event"
# What start_operation() raises when one of its mutex waits hits the deadline, see
# raft_group0_client::hold_mutex() and raft_server_with_timeouts::timeout_message().
GROUP0_TIMEOUT_ERROR = "raft operation [group0 operation mutex] timed out"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_group0_operation_timeout_covers_the_group0_mutexes(manager: ScyllaClusterManager):
    property_file = [{"dc": "dc1", "rack": f"rack{i}"} for i in range(1, 4)]
    servers = await manager.servers_add(3, property_file=property_file)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    coordinator_id = await get_topology_coordinator(manager)
    coord = await manager.find_server_by_host_id(servers, coordinator_id)
    logger.info(f"topology coordinator is {coord.ip_addr}")

    ks = "g0_timeout_repro"
    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1']} AND tablets = {'initial': 1}")
    # Baseline: the same DDL on the same node with nothing holding the guard. Run it before
    # the timeout is shortened, so that a slow debug build cannot fail it.
    coord_cql = await manager.get_cql_exclusive(coord)
    started = time.monotonic()
    await coord_cql.run_async(f"CREATE TABLE {ks}.seed (pk int PRIMARY KEY)")
    logger.info(f"baseline CREATE TABLE on the coordinator took {time.monotonic() - started:.2f}s")

    # Shorten the group0 operation timeout only now. Setting it at startup would apply it to
    # cluster formation too, where a legitimate join read-barrier can exceed it in debug
    # under load, and the test would fail in servers_add() rather than on what it asserts.
    # server_update_config only sends SIGHUP, so wait until the node has re-read the file.
    log_file = await manager.server_open_log(coord.server_id)
    mark = await log_file.mark()
    await manager.server_update_config(
            coord.server_id, 'group0_raft_op_timeout_in_ms', GROUP0_OP_TIMEOUT_MS)
    await log_file.wait_for("completed re-reading configuration file", from_mark=mark, timeout=60)

    # Park the RF change on the coordinator at the point where it holds the group0 guard
    # and is about to commit.
    await manager.api.enable_injection(coord.ip_addr, INJECTION, one_shot=False)

    logger.info("Triggering a keyspace_rf_change (will park holding the group0 guard)")
    alter_fut = cql.run_async(
        f"ALTER KEYSPACE {ks} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2']}")
    ddl_task = None
    ddl_error = None
    try:
        await manager.api.wait_for_injection_enter(coord.ip_addr, INJECTION)
        logger.info("Coordinator is parked holding the group0 guard")

        # Same node, so the same _operation_mutex. This DDL goes through
        # migration_manager::start_group0_operation(), which always passes at least the
        # default raft_timeout, i.e. group0_raft_op_timeout_in_ms.
        started = time.monotonic()
        ddl_task = asyncio.ensure_future(
            coord_cql.run_async(f"CREATE TABLE {ks}.blocked (pk int PRIMARY KEY)", timeout=120))
        logger.info(f"Issued DDL on the coordinator, giving it {HOLD_S}s "
                    f"to honour its {GROUP0_OP_TIMEOUT_MS}ms group0 timeout")

        done, _ = await asyncio.wait([ddl_task], timeout=HOLD_S)
        elapsed = time.monotonic() - started
        if done:
            ddl_error = ddl_task.exception()
            outcome = "raised" if ddl_error else "succeeded"
            logger.info(f"DDL {outcome} after {elapsed:.1f}s: {ddl_error}")
        else:
            logger.info(f"DDL still blocked after {elapsed:.1f}s")
    finally:
        # Release the holder before the injection's 30s window expires, otherwise the
        # node aborts.
        await manager.api.message_injection(coord.ip_addr, INJECTION)
        await manager.api.disable_injection(coord.ip_addr, INJECTION)
        if ddl_task is not None:
            try:
                released_at = time.monotonic()
                await asyncio.wait_for(ddl_task, timeout=60)
                logger.info(f"DDL completed {time.monotonic() - released_at:.1f}s after the "
                            "holder released the guard")
            except Exception as exc:
                logger.info(f"DDL finished with: {exc}")
        try:
            await asyncio.wait_for(asyncio.ensure_future(alter_fut), timeout=120)
        except Exception as exc:
            logger.info(f"ALTER KEYSPACE finished with: {exc}")

    assert done, (
        f"CREATE TABLE was still blocked {HOLD_S}s after it was issued, despite carrying a "
        f"{GROUP0_OP_TIMEOUT_MS}ms group0 operation timeout. It is waiting for the holder of "
        f"one of the group0 mutexes, which means a wait in start_operation() is no longer "
        f"covered by the caller's timeout.")
    # Completing is not enough. A CREATE TABLE that succeeded while the holder was parked
    # would mean it never waited for the mutex at all, and any other error says nothing
    # about the timeout. Only the group0 timeout error proves the deadline was honoured.
    assert ddl_error is not None and GROUP0_TIMEOUT_ERROR in str(ddl_error), (
        f"CREATE TABLE completed after {elapsed:.1f}s, but not with the group0 operation "
        f"timeout: {ddl_error!r}")

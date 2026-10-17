#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel, ReadTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"
PAUSE_BEFORE_READ_BARRIER = "sc_coordinator_wait_before_query_read_barrier"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_graceful_shutdown_with_read_in_flight(manager: ScyllaClusterManager):
    """Check that a node stops cleanly while a QUORUM read on a strongly consistent
    table is stuck in the raft read barrier on it.

    A QUORUM read is a raft read barrier on the leader, and a new leader can answer it
    only after it commits the first entry of its own term. The test hands leadership to
    one node while the other two drop the group's AppendEntries and may not run for
    leader themselves, so the new leader keeps its term but never commits, and a read on
    it waits in the read barrier until the shutdown aborts it. The stop must return, the
    node must log the read's abort, the read must fail on the client, and the restarted
    node must answer a QUORUM read again. The client usually sees the host go away, as
    the CQL server closes its connections before it aborts the requests still running;
    the abort turned into a read timeout has been seen to reach the client too.

    The read's own timeout would abort the wait too, so the test makes it longer than
    request_timeout_on_shutdown_in_seconds: only then the shutdown's abort is the one
    that ends the read.
    """
    config = DEFAULT_CONFIG | {'request_timeout_on_shutdown_in_seconds': 1, 'read_request_timeout_in_ms': 60000}
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'sc_coordinator=trace']
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            leader = servers[host_ids.index(await wait_for_leader(manager, servers[0], group_id))]
            i = next(i for i, s in enumerate(servers) if s is not leader)
            target, target_host, target_id = servers[i], hosts[i], host_ids[i]
            others = [s for s in servers if s is not target]
            quorum_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM)

            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)")

            try:
                # From now on only `target` can lead the group, and nothing it appends reaches the others.
                for s in others:
                    await manager.api.enable_injection(s.ip_addr, "avoid_being_raft_leader", one_shot=False)
                    await manager.api.enable_injection(s.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})
                await trigger_stepdown(manager, leader, group_id, target_host_id=target_id)
                await wait_for_leader(manager, target, group_id, expected_host_id=target_id)
                logger.info(f"group {group_id}: leadership moved from {leader.ip_addr} to {target.ip_addr}")

                # Pause the read right before it enters the barrier, so the stop is known to find it there.
                log = await manager.server_open_log(target.server_id)
                mark = await log.mark()
                await manager.api.enable_injection(target.ip_addr, PAUSE_BEFORE_READ_BARRIER, one_shot=True)
                read = cql.run_async(quorum_read, host=target_host)
                await log.wait_for(f"{PAUSE_BEFORE_READ_BARRIER}: waiting", from_mark=mark, timeout=60)
                await manager.api.message_injection(target.ip_addr, PAUSE_BEFORE_READ_BARRIER)
                assert not read.done()

                await manager.server_stop_gracefully(target.server_id)

                # raft words the abort by where the request waited: this one is the wait for the commit
                # inside the read barrier, so it also proves the read got there before the stop.
                await log.wait_for(r"query\(\): request timed out with error Aborted while waiting to apply entry", from_mark=mark, timeout=10)
                with pytest.raises((ReadTimeout, NoHostAvailable)):
                    await asyncio.wait_for(read, timeout=60)

                for s in others:
                    await manager.api.disable_injection(s.ip_addr, DROP_APPEND_ENTRIES)
                await manager.server_start(target.server_id)
                hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)
                assert [r.c for r in await cql.run_async(quorum_read, host=hosts[i])] == [1]
            finally:
                # Leave the other two able to replicate and to lead again: a DROP KEYSPACE through
                # group0 would otherwise hang if the test fails while `target` is down.
                for s in others:
                    for injection in (DROP_APPEND_ENTRIES, "avoid_being_raft_leader"):
                        await manager.api.disable_injection(s.ip_addr, injection)

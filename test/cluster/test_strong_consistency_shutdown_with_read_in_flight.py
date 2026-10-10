#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("stop", ["new_leader", "old_leader"])
async def test_graceful_shutdown_with_read_in_flight(manager: ScyllaClusterManager, stop: str):
    """Check that a node stops cleanly while a QUORUM read on a strongly consistent
    table is stuck inside raft on it.

    A QUORUM read is a raft read barrier on the leader. A new leader can answer it
    only after it commits the first entry of its own term. The test makes every node
    drop the group's AppendEntries and tells the leader to step down. The next leader
    wins the vote (votes are not dropped) but can never commit, so a read on it waits
    in the read barrier. The old leader no longer hears from any leader, so a read on
    it waits for a leader. Neither wait ends on its own; only the shutdown can end it.
    Leadership keeps moving between the nodes meanwhile, which only changes which of
    the two waits the read sits in.

    new_leader: the read and the shutdown go to the next leader. old_leader: both go
    to the node that stepped down. The stop must return, the read must fail (the driver
    does not retry a read pinned to a host), and the restarted node must answer a
    QUORUM read again.
    """
    config = DEFAULT_CONFIG | {'request_timeout_on_shutdown_in_seconds': 1, 'read_request_timeout_in_ms': 60000}
    servers = await manager.servers_add(3, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    index = {str(await manager.get_host_id(s.server_id)): i for i, s in enumerate(servers)}

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            old_leader_id = await wait_for_leader(manager, servers[0], group_id)
            old_leader = servers[index[old_leader_id]]
            quorum_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM)

            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=hosts[index[old_leader_id]])

            for s in servers:
                await manager.api.enable_injection(s.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})
            await trigger_stepdown(manager, old_leader, group_id)

            async def successor() -> str | None:
                for hid, i in index.items():
                    if hid != old_leader_id and str(await manager.api.get_raft_leader(servers[i].ip_addr, group_id)) == hid:
                        return hid
                return None
            new_leader_id = await wait_for(successor, time.time() + 60, label=f"a new leader of group {group_id}")
            i = index[new_leader_id if stop == "new_leader" else old_leader_id]
            target, target_host = servers[i], hosts[i]
            logger.info(f"group {group_id}: old leader {old_leader_id}, new leader {new_leader_id}, stopping {target.ip_addr}")

            log = await manager.server_open_log(target.server_id)
            mark = await log.mark()
            await manager.api.set_logger_level(target.ip_addr, "raft", "trace")
            read = cql.run_async(quorum_read, host=target_host)
            await log.wait_for(rf"\[sc-{group_id}\] (read_barrier leader not ready|the leader is unknown, waiting through uncertainty)",
                               from_mark=mark, timeout=60)
            assert not read.done()

            await manager.server_stop_gracefully(target.server_id)

            await asyncio.wait([read], timeout=60)
            assert read.done(), "the read outlived its node"
            assert isinstance(read.exception(), NoHostAvailable), f"the read did not die with its node: {read.exception()!r}"
            logger.info(f"the read was aborted by the shutdown: {read.exception()}")

            for s in servers:
                if s is not target:
                    await manager.api.disable_injection(s.ip_addr, DROP_APPEND_ENTRIES)
            await manager.server_start(target.server_id)
            hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)
            assert [r.c for r in await cql.run_async(quorum_read, host=hosts[i])] == [1]

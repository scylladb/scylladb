#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A strongly consistent request forwarded to a node that is not fully up.

A restarting node starts its SC raft groups before its CQL server (main.cc), and the
CQL server is what registers the RPC verbs that forward CQL requests between nodes
(transport/server.cc, cql_server::init_messaging_service). So there is a window in which
the node can lead a tablet but cannot take a forwarded request. The load tests hit it by
chance (test_majority_lost_and_restored_under_load); here it is made to order: a
node started with start_native_transport=false is a node stuck in that window
(SCYLLADB-4722).
"""

from __future__ import annotations

import time

import pytest
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ServerError

from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.test_strong_consistency import get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


@pytest.mark.asyncio
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4722",
                      reason="a request forwarded to a node whose CQL server is not up fails with SERVER_ERROR 'unknown verb'")
async def test_request_forwarded_to_node_without_cql_server(manager: ScyllaClusterManager):
    """A write coordinated by a follower and forwarded to a leader whose CQL server is not
    up must be served or refused with an error that says so, not with an internal RPC
    error. The leader is a node restarted with start_native_transport=false: raft is up
    on it and it takes the leadership, but it has not registered the forwarding verbs.

    Today the client gets SERVER_ERROR "unknown verb" (rpc::unknown_verb_error passed
    through by cql_server::forward_cql). The request was never executed.

    Reproduces SCYLLADB-4722: the same happens after `nodetool disablebinary` on a leader;
    a restart is the other way for a node to lead a tablet without a CQL server.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    async with new_test_keyspace(manager, sc_keyspace_opts(replication_factor=3, initial_tablets=1)) as ks, \
            new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
        group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
        await wait_for_leader(manager, servers[0], group_id)
        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)")

        headless, coordinator = servers[0], servers[1]
        await manager.server_stop_gracefully(headless.server_id)
        await manager.server_update_config(headless.server_id, "start_native_transport", False)
        await manager.server_start(headless.server_id, expected_server_up_state=ServerUpState.HOST_ID_QUERIED, connect_driver=False)
        try:
            # server_start returns before the REST API listens when there is no CQL to wait for.
            async def api_up() -> bool:
                await manager.api.get_host_id(headless.ip_addr)
                return True
            await wait_for(api_up, time.time() + 60, label=f"the REST API of {headless.ip_addr}")
            await ensure_raft_group_leader_on(manager, headless, group_id)
            write = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (0, 2)")
            write.retry_policy = FallthroughRetryPolicy()  # one attempt: the error must be the server's answer
            try:
                await cql.run_async(write.bind([]), host=hosts[servers.index(coordinator)])
            except ServerError as e:
                pytest.fail(f"the write forwarded to the leader without a CQL server got an internal error: {e}")
        finally:
            await manager.api.client.post("/storage_service/native_transport", host=headless.ip_addr)
            await manager.server_update_config(headless.server_id, "start_native_transport", True)
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

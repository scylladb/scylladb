#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Regression test for: cross-node CQL forwarding used to drop the memoized
non-deterministic pk function value(s) on the send side.

cql_server::process() (transport/server.cc) evaluates a strongly-consistent
statement's partition-key expression once to decide where to route it. When
the owning replica is on another node, it must forward the just-computed
cql3::computed_function_values cache (`cached_vals`) alongside the request,
so the target node reuses the same value instead of re-evaluating a
non-deterministic function (uuid(), now(), ...) itself against a different
key than the one the routing decision was based on.

Verified via the "Forwarding N cached pk function value(s) with the request"
trace event emitted right where the request is actually forwarded on the
wire (transport/server.cc), the same pattern test_strong_consistency.py uses
to assert on the CQL forwarding cache.
"""

import logging

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)

CONFIG = {'experimental_features': ['strongly-consistent-tables']}


async def test_node_bounce_forwards_cached_pk_function_value(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, auto_rack_dc='my_dc')
    cql, hosts = await manager.get_ready_cql(servers)

    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"can't find host for host_id {host_id}")

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
            "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk uuid PRIMARY KEY, v int)")

        # A single tablet covers the whole ring, so any token resolves to it.
        tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, "t", 0)
        assert len(tablet_replicas) == 2
        replica_host_ids = {str(r[0]) for r in tablet_replicas}

        non_replica_host_id = [h for h in host_ids if str(h) not in replica_host_ids][0]
        non_replica_host = host_by_host_id(non_replica_host_id)

        logger.info(f"Replicas: {replica_host_ids}, non-replica coordinator: {non_replica_host_id}")

        # The non-replica host is not a tablet replica at all, so coordinating
        # through it forces a node bounce on every single execution -- no need
        # to rely on the random pk value happening to land on a remote node.
        stmt = "INSERT INTO {}.t (pk, v) VALUES (uuid(), 1) IF NOT EXISTS".format(ks)
        traced = cql.execute(stmt, host=non_replica_host, trace=True)
        trace = traced.get_query_trace()

        forwarded_events = [e for e in trace.events
                             if "cached pk function value(s) with the request" in e.description]
        assert forwarded_events, (
            "expected a 'Forwarding N cached pk function value(s) with the request' trace event "
            "on the non-replica coordinator; the request did not take the node-bounce path as "
            f"expected. All trace events: {[e.description for e in trace.events]}"
        )

        forwarded = int(forwarded_events[0].description.split()[1])
        assert forwarded == 1, (
            f"expected the coordinator to forward exactly the 1 non-deterministic pk function "
            f"value (uuid()) it evaluated while making its routing decision, but the request "
            f"carried {forwarded}. If this is 0, cql_server::process()'s node-bounce branch is "
            f"forwarding a stale/empty cache again instead of the value it just computed, so the "
            f"target node will recompute uuid() independently against a different partition key "
            f"than the one the routing decision was based on."
        )

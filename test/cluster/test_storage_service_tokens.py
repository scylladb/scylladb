#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from collections import Counter

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas

NUM_TOKENS = 16


async def test_local_tokens(manager: ScyllaClusterManager):
    """/storage_service/tokens returns only the local node's tokens, not the whole ring (SCYLLADB-4724)."""
    servers = await manager.servers_add(3, config={'num_tokens': NUM_TOKENS})
    all_tokens = set()
    for s in servers:
        local = await manager.api.get_tokens(s.ip_addr)
        assert local == await manager.api.get_tokens(s.ip_addr, s.ip_addr)
        assert len(local) == NUM_TOKENS
        assert all_tokens.isdisjoint(local)
        all_tokens.update(local)


async def test_tablet_tokens(manager: ScyllaClusterManager):
    """keyspace+cf return last tokens of the tablets each node replicates (SCYLLADB-4725)."""
    servers = await manager.servers_add(3, config={'num_tokens': NUM_TOKENS},
                                        property_file=[{'dc': 'dc1', 'rack': r} for r in ['r1', 'r1', 'r2']])
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    rf = 2
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY) WITH tablets = {{'min_tablet_count': 8, 'max_tablet_count': 8}}")
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, 't')
        replica_count = Counter()
        for s in servers:
            host_id = await manager.get_host_id(s.server_id)
            expected = sorted(t.last_token for t in tablets if any(h == host_id for h, _ in t.replicas))
            local = await manager.api.get_tokens(s.ip_addr, keyspace=ks, table='t')
            assert sorted(map(int, local)) == expected
            assert local == await manager.api.get_tokens(servers[0].ip_addr, s.ip_addr, keyspace=ks, table='t')
            replica_count.update(map(int, local))
        assert replica_count == Counter({t.last_token: rf for t in tablets})

        with pytest.raises(HTTPError, match="Either provide both keyspace and table"):
            await manager.api.get_tokens(servers[0].ip_addr, keyspace=ks)

    # system_distributed is vnode-based; reusing it saves a keyspace round-trip.
    for s in servers:
        assert await manager.api.get_tokens(s.ip_addr, keyspace='system_distributed', table='view_build_status') == await manager.api.get_tokens(s.ip_addr)

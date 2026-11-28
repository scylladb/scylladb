#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

# Per peer: (token set, writetime of its min token, data_center, supported_features).
async def peer_writetimes(cql, observer):
    res = {}
    for r in await cql.run_async("SELECT peer, tokens FROM system.peers", host=observer):
        assert r.tokens
        token = min(r.tokens, key=int)
        w = (await cql.run_async(
            f"SELECT writetime(tokens['{token}']) AS tk, writetime(data_center) AS dc, "
            f"writetime(supported_features) AS sf FROM system.peers WHERE peer = '{r.peer}'",
            host=observer))[0]
        res[r.peer] = (frozenset(r.tokens), w.tk, w.dc, w.sf)
    return res


# SCYLLADB-6314: a raft topology reload must not rewrite unchanged rows in system.peers,
# also after partial peer writes (schema_version on DDL).
async def test_topology_reload_keeps_unchanged_peer_rows(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    observer = next(h for h in hosts if h.address == servers[0].ip_addr)

    # Warm-up: record the rows written at join time in the saved state.
    await manager.api.reload_raft_topology_state(servers[0].ip_addr)
    before = await peer_writetimes(cql, observer)
    assert len(before) == 2

    # The reload republishes tokens of every normal peer on the observer.
    await manager.api.reload_raft_topology_state(servers[0].ip_addr)
    assert await peer_writetimes(cql, observer) == before

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"):
        async def schema_synced():
            local = (await cql.run_async("SELECT schema_version FROM system.local WHERE key = 'local'", host=observer))[0].schema_version
            peers = await cql.run_async("SELECT schema_version FROM system.peers", host=observer)
            return True if all(p.schema_version == local for p in peers) else None
        await wait_for(schema_synced, time.time() + 60)

    await manager.api.reload_raft_topology_state(servers[0].ip_addr)
    assert await peer_writetimes(cql, observer) == before

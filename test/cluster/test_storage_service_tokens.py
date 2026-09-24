#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

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

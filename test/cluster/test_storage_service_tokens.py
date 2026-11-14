#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import subprocess

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

NUM_TOKENS = 16


async def test_nodetool_info_local_tokens(manager: ScyllaClusterManager):
    """nodetool info shows only the node's own tokens, not the whole ring (SCYLLADB-4724)."""
    servers = await manager.servers_add(3, config={'num_tokens': NUM_TOKENS})
    exe_path = await manager.server_get_exe(servers[0].server_id)
    all_tokens = set()
    for s in servers:
        out = subprocess.run([exe_path, "nodetool", "info", "--tokens", "-h", s.ip_addr],
                             capture_output=True, text=True, check=True).stdout
        local = [line.split(':', 1)[1].strip() for line in out.splitlines() if line.startswith('Token ')]
        assert local == await manager.api.get_tokens(s.ip_addr, s.ip_addr)
        assert len(local) == NUM_TOKENS
        assert all_tokens.isdisjoint(local)
        all_tokens.update(local)
    # scylla-operator hashes the ring-wide token list to schedule cleanup.
    assert sorted(await manager.api.get_tokens(servers[0].ip_addr), key=int) == sorted(all_tokens, key=int)


async def test_nodetool_info_gossip_disabled(manager: ScyllaClusterManager):
    """nodetool info must not fail when gossip is disabled."""
    server = await manager.server_add()
    exe_path = await manager.server_get_exe(server.server_id)
    nodetool = lambda *args: subprocess.run([exe_path, "nodetool", *args, "-h", server.ip_addr],
                                            capture_output=True, text=True, check=True).stdout
    nodetool("disablegossip")
    out = nodetool("info")
    nodetool("enablegossip")
    assert "Gossip active          : false" in out
    assert "Token                  : (gossip is not active)" in out

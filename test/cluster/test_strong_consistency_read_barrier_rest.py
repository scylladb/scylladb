#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.asyncio
async def test_read_barrier_without_timeout_on_sc_group(manager: ScyllaClusterManager):
    """Verify that a read barrier on a strongly consistent tablet group, requested through
    the REST API without the optional `timeout` parameter, succeeds instead of failing
    an internal invariant.

    Reproduces SCYLLADB-4759: api/raft.cc passes a timeout without a value, and SC groups,
    unlike group0, have no default_op_timeout_in_ms to fill it in.
    """
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    await manager.get_ready_cql([server])

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)  # the explicit form works
            await read_barrier(manager.api, server.ip_addr, group_id)

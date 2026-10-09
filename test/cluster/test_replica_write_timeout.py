# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error

from test.cluster.util import new_test_keyspace, new_test_table


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_remote_replica_write_timeout_is_reported_as_timeout(manager: ScyllaClusterManager):
    """
    A write that times out on a remote replica must reach the client as
    WriteTimeout, the same as a timeout on the coordinator's local replica,
    and not as WriteFailure. Reproduces SCYLLADB-4966.
    """
    servers = await manager.servers_add(2, config={"write_request_timeout_in_ms": 2000}, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    coordinator = hosts[0]

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as tbl:
            cf = tbl.split(".")[1]
            insert = SimpleStatement(f"INSERT INTO {tbl} (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.ALL)
            async with inject_error(manager.api, servers[1].ip_addr, "database_apply",
                                    parameters={"ks_name": ks, "cf_name": cf, "what": "timeout"}):
                with pytest.raises(WriteTimeout):
                    await cql.run_async(insert, host=coordinator)

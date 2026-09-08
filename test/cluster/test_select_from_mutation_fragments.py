#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# This file configures pytest for all tests in this directory, and also

import pytest

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.protocol import InvalidRequest  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.cluster.util import new_test_keyspace

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas


async def test_sticky_coordinator_enforced(manager: ScyllaClusterManager) -> None:
    await manager.servers_add(2, cmdline=['--logger-log-level', 'paging=trace'], auto_rack_dc="dc1")

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"create table {ks}.tbl (pk int, ck int, v int, primary key (pk, ck))")

        num_rows = 43
        expected_num_rows = num_rows + 2  # rows + partition-start + partitione-end
        for ck in range(0, num_rows):
            await cql.run_async(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, {ck}, 100)")

        unpaged_res = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.tbl) WHERE pk = 0")
        assert len(unpaged_res) == expected_num_rows

        read_stmt = SimpleStatement(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.tbl) WHERE pk = 0", fetch_size=10)

        # The default round-robin load-balancing policy will jump between the nodes.
        # This should trigger an exception.
        with pytest.raises(
                InvalidRequest,
                match="Moving between coordinators is not allowed in SELECT FROM MUTATION_FRAGMENTS\\(\\) statements.*"):
            await cql.run_async(read_stmt, all_pages=True)


async def test_strongly_consistent_table_dump_is_local(manager: ScyllaClusterManager) -> None:
    """
    SELECT FROM MUTATION_FRAGMENTS() on a strongly consistent table dumps
    the coordinator's own replica. A coordinator that is not a replica
    returns no rows and forwards nothing, unlike a regular read at ONE,
    which it forwards to a replica. Two nodes and RF=1 give one of each.
    """
    servers = await manager.servers_add(2, config={'experimental_features': ['strongly-consistent-tables']}, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int)")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, v) VALUES (0, 100)", consistency_level=ConsistencyLevel.QUORUM))
        # A write returns once committed. The linearizable read waits
        # for it to be applied on the replica, which is what the dump reads.
        await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.tbl WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM))

        (replica_host_id, _), = await get_tablet_replicas(manager, servers[0], ks, "tbl", 0)
        replica_index = host_ids.index(str(replica_host_id))
        replica_host = hosts[replica_index]
        non_replica_host = hosts[1 - replica_index]

        dump = SimpleStatement(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.tbl) WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)
        read = SimpleStatement(f"SELECT * FROM {ks}.tbl WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)

        async def forwarded_requests(host):
            metrics = await manager.metrics.query(host.address)
            return metrics.get('scylla_transport_requests_forwarded_successfully') or 0

        # partition start, clustering row, partition end
        assert len(await cql.run_async(dump, host=replica_host)) == 3

        forwarded_before = await forwarded_requests(non_replica_host)
        assert await cql.run_async(dump, host=non_replica_host) == []
        assert await forwarded_requests(non_replica_host) == forwarded_before

        assert len(await cql.run_async(read, host=non_replica_host)) == 1
        assert await forwarded_requests(non_replica_host) == forwarded_before + 1

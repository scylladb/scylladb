#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
from cassandra.query import BatchStatement, BatchType

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace


# The test asserts on 5 ms stalls, so it only runs in the optimized modes.
@pytest.mark.skip_mode(mode=['debug', 'sanitize', 'coverage'], reason='instrumented builds stall on their own')
async def test_large_logged_batch_does_not_stall_coordinator(manager: ScyllaClusterManager) -> None:
    """Reproduces SCYLLADB-5901: the coordinator of a logged batch with many
    partitions ran per-partition loops that never yielded: it copied the
    batch's mutations, froze every mutation and looked up its replicas
    (mutate_prepare), serialized all of them into the batchlog entry, and
    started the send of every partition to every replica (mutate_begin).
    Production saw 400-partition batches stall for 32-35 ms in the send;
    4000 partitions make the loops visible on a fast test machine, where
    each takes 6-26 ms, while the fixed path stays under 2 ms.

    The test counts the stall detector's reports, which measure CPU time.
    The reactor_stalls histogram measures wall-clock time, and on a test
    machine running several nodes it also counts long tasks that this test
    can't attribute to the batch."""
    servers = await manager.servers_add(3, auto_rack_dc='dc1',
                                        config={'internode_compression': 'all', 'batch_size_fail_threshold_in_kb': 10240},
                                        cmdline=['--blocked-reactor-notify-ms', '5', '--blocked-reactor-reports-per-minute', '1000'])
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.tab (pk int PRIMARY KEY, v text)')
        insert = cql.prepare(f'INSERT INTO {ks}.tab (pk, v) VALUES (?, ?)')
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for pk in range(4000):
            batch.add(insert, (pk, 'x' * 100))
        # Warm up connections and caches, so they don't count as stalls.
        await cql.run_async(batch, host=hosts[0])

        async def stalls_reported():
            return (await manager.metrics.query(servers[0].ip_addr)).get('scylla_stall_detector_reported') or 0

        before = await stalls_reported()
        for _ in range(5):
            await cql.run_async(batch, host=hosts[0])
        assert await stalls_reported() - before == 0

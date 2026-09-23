#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

MIN_TOKEN = -(2**63)


async def estimates_per_host(manager, servers, hosts, ks, cf):
    for s in servers:
        # Every node must see the final tablet map before reporting.
        await read_barrier(manager.api, s.ip_addr)
        await manager.api.keyspace_flush(s.ip_addr, ks, cf)
    res = {}
    for s, h in zip(servers, hosts):
        rows = await manager.get_cql().run_async(
            f"SELECT range_start, range_end, partitions_count FROM system.size_estimates "
            f"WHERE keyspace_name = '{ks}' AND table_name = '{cf}'", host=h)
        res[s.server_id] = [(int(r.range_start), int(r.range_end), r.partitions_count) for r in rows]
    logger.info(f"size_estimates per server: {res}")
    return res


async def check_estimates(manager, server, ks, cf, estimates, n):
    tablets = await get_all_tablet_replicas(manager, server, ks, cf)
    last_tokens = [t.last_token for t in tablets]
    expected = sorted(zip([MIN_TOKEN] + last_tokens[:-1], last_tokens))
    reported = sorted((start, end) for rows in estimates.values() for start, end, _ in rows)
    assert reported == expected
    # Same 25% slack as cqlpy's test_partitions_estimate_simple_small.
    total = sum(c for rows in estimates.values() for _, _, c in rows)
    assert n / 1.25 < total < n * 1.25


# Reproduces CUSTOMER-742: size_estimates reported vnode ranges for tablet tables.
async def test_size_estimates_report_primary_tablets(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
    ])
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    N = 100
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          "AND tablets = {'enabled': true}") as ks, \
               new_test_table(manager, ks, "pk int PRIMARY KEY", " WITH tablets = {'min_tablet_count': 8}") as table:
        cf = table.split(".")[1]
        insert = cql.prepare(f"INSERT INTO {table} (pk) VALUES (?)")
        await asyncio.gather(*[cql.run_async(insert, [k]) for k in range(N)])

        estimates = await estimates_per_host(manager, servers, hosts, ks, cf)
        await check_estimates(manager, servers[0], ks, cf, estimates, N)

        # Move an r1 replica to the other r1 node, preferring the one reporting the tablet.
        host_of = {s.server_id: h for s, h in zip(servers, host_ids)}
        reporter = {end: host_of[sid] for sid, rows in estimates.items() for _, end, _ in rows}
        other_r1 = {host_ids[0]: host_ids[1], host_ids[1]: host_ids[0]}
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, cf)
        candidates = [(t, r) for t in tablets for r in t.replicas if r[0] in other_r1]
        t, (src_host, src_shard) = next(((t, r) for t, r in candidates if reporter[t.last_token] == r[0]), candidates[0])
        await manager.api.move_tablet(servers[0].ip_addr, ks, cf, src_host, src_shard, other_r1[src_host], 0, t.last_token)

        estimates = await estimates_per_host(manager, servers, hosts, ks, cf)
        await check_estimates(manager, servers[0], ks, cf, estimates, N)

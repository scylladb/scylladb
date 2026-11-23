#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Helpers shared by the network proxy tests of this directory.

The gossiper and the failure detector report proxied nodes by their proxy addresses; the
helpers here take and return real addresses (``ServerInfo.ip_addr``) and translate.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from test.pylib.internal_types import HostID, IPAddress, ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.rest_client import ScyllaMetrics, ScyllaMetricsLine
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, unique_name, wait_for

# The register workload draws its keys from this seed, so a run replays the same choices.
SEED = 20261106


async def down_endpoints(manager: ScyllaClusterManager, network_proxy: NetworkProxy, server: ServerInfo) -> set[str]:
    """The nodes `server` reports DOWN, by their real addresses (gossip knows them by their proxies')."""
    real_of = {proxy: real for real, proxy in network_proxy.addresses().items()}
    return {real_of.get(IPAddress(ep), ep) for ep in await manager.api.get_down_endpoints(server.ip_addr)}


@asynccontextmanager
async def new_table(manager: ScyllaClusterManager, keyspace: str, schema: str, extra: str = "") -> AsyncIterator[str]:
    """Like test.cluster.util.new_test_table, but the table is never dropped: the cluster lives for one
    test, and a DROP while a node is still cut off can hang and hide the test's own failure."""
    table = f"{keyspace}.{unique_name()}"
    await manager.get_cql().run_async(f"CREATE TABLE {table} ({schema}){extra}")
    yield table


async def wait_for_down(manager: ScyllaClusterManager, network_proxy: NetworkProxy, server: ServerInfo, expected: set[str],
                        timeout: float = 60) -> float:
    """Wait until `server` reports exactly the nodes `expected` (real addresses) DOWN, by their proxy
    addresses; return how long it took, in seconds."""
    proxies = {network_proxy.address_of(ip) for ip in expected}
    start = time.time()

    async def done():
        return True if set(await manager.api.get_down_endpoints(server.ip_addr)) == proxies else None
    await wait_for(done, start + timeout, period=0.2, label=f"{server.ip_addr} to see {sorted(expected) or 'nobody'} down")
    return time.time() - start


async def wait_for_all_up(manager: ScyllaClusterManager, network_proxy: NetworkProxy, servers: list[ServerInfo],
                          timeout: float = 60) -> None:
    """Wait until none of `servers` reports any node DOWN."""
    await gather_safely(*(wait_for_down(manager, network_proxy, s, set(), timeout) for s in servers))


async def heal_and_wait_all_up(manager: ScyllaClusterManager, network_proxy: NetworkProxy, servers: list[ServerInfo],
                               timeout: float = 60) -> None:
    """Remove every proxy rule, then wait until none of `servers` reports any node DOWN."""
    await network_proxy.heal()
    await wait_for_all_up(manager, network_proxy, servers, timeout)


def exact_metric(metrics: ScyllaMetrics, name: str, labels: Optional[dict[str, str]] = None) -> float:
    """The sum of the series named exactly `name` whose labels include `labels` (all of them by
    default); 0 when there is none (skip_when_empty). ScyllaMetrics.get matches a prefix, so
    "background_writes" would take in "background_writes_failed" too."""
    total = 0.0
    for line in metrics.lines_by_prefix(name):
        parsed = ScyllaMetricsLine.from_string(line)
        if parsed and parsed.name == name and all(parsed.labels.get(k) == str(v) for k, v in (labels or {}).items()):
            total += parsed.value
    return total


async def metric(manager: ScyllaClusterManager, server: ServerInfo, name: str, labels: Optional[dict[str, str]] = None) -> float:
    """exact_metric of a fresh scrape of `server`: a counter or gauge summed over the shards, per label filter."""
    return exact_metric(await manager.metrics.query(server.ip_addr), name, labels)


async def wait_for_new_leader(manager: ScyllaClusterManager, server: ServerInfo, group_id: Optional[str], old_leader: HostID,
                              timeout: float) -> HostID:
    """Wait until `server` reports a leader other than `old_leader`; `group_id` None is group0."""
    async def new_leader():
        leader = await manager.api.get_raft_leader(server.ip_addr, group_id)
        return leader if leader is not None and leader != old_leader else None
    return await wait_for(new_leader, time.time() + timeout, period=0.25,
                          label=f"{server.ip_addr} to report a leader of {group_id or 'group0'} other than {old_leader}")

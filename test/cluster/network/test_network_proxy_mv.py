#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Materialized views under live network faults.

A base replica sends its view updates to its paired view replica; the tests cut that one link
while both nodes stay alive and serve clients, which stopping or pausing a node cannot do.

The tests use RF=1 tablets with the base tablet pinned to X and the view tablet pinned to Y, so
every view update is a remote X -> Y update, and a third node Z coordinates the client writes.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout, Unavailable, WriteFailure, WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import OverloadedErrorMessage
from cassandra.query import SimpleStatement

from test.cluster.mv.tablets.test_mv_tablets import pin_the_only_tablet
from test.cluster.network.util import (down_endpoints, heal_and_wait_all_up, metric, wait_for_down)
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_view

logger = logging.getLogger(__name__)

TABLETS_RF1 = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
               " AND tablets = {'initial': 1}")

CLIENT_ERRORS = (WriteTimeout, WriteFailure, Unavailable, NoHostAvailable, OperationTimedOut, OverloadedErrorMessage, ReadTimeout)


async def boot(manager: ScyllaClusterManager, failure_detector_timeout: int, nodes: int = 3,
               extra_config: dict | None = None, cmdline: list[str] | None = None) -> tuple[list[ServerInfo], Any, list]:
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout,
              'hinted_handoff_enabled': False} | (extra_config or {})
    servers = await manager.servers_add(nodes, config=config, cmdline=cmdline or [])
    cql, hosts = await manager.get_ready_cql(servers)
    return servers, cql, hosts


async def make_base_and_view(manager: ScyllaClusterManager, cql, ks: str, x: ServerInfo | None, y: ServerInfo | None,
                             tablets: bool) -> tuple[str, str]:
    """ks.base (pk, v, w) and ks.mv keyed by (v, pk); with tablets, base pinned to X and the view to Y."""
    await cql.run_async(f"CREATE TABLE {ks}.base (pk int PRIMARY KEY, v int, w text)")
    await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.base "
                        "WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)")
    if tablets:
        await pin_the_only_tablet(manager, ks, "base", x)
        await pin_the_only_tablet(manager, ks, "mv", y)
    return f"{ks}.base", f"{ks}.mv"


async def sum_metric(manager: ScyllaClusterManager, servers: list[ServerInfo], name: str) -> float:
    """`name` summed over `servers`."""
    return sum([await metric(manager, s, name) for s in servers])


async def wait_view_hints_delivered(manager: ScyllaClusterManager, servers: list[ServerInfo], timeout: float) -> tuple[float, float]:
    async def delivered():
        written = await sum_metric(manager, servers, "scylla_hints_for_views_manager_written")
        sent = await sum_metric(manager, servers, "scylla_hints_for_views_manager_sent_total")
        return (written, sent) if sent >= written else None
    return await wait_for(delivered, time.time() + timeout, period=1, label="view hints delivered")


class SyncViewWriteAcked(AssertionError):
    """A synchronous view write was acked although the view replica could not be updated."""


@pytest.mark.xfail(reason="scylladb/scylladb#16229: a synchronous view update to an unreachable view replica "
                          "is turned into a view hint, which satisfies CL=ANY, and the base write is acked",
                   strict=True, raises=SyncViewWriteAcked)
async def test_synchronous_view_write_is_not_acked_while_the_view_replica_is_unreachable(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A view WITH synchronous_updates = true; base on X, view on Y, clients through Z. Only the
    X <-> Y link is cut: Y stays alive and reachable from Z and from the clients.

    The documented guarantee (docs/cql/cql-extensions.md, "Synchronous materialized views";
    docs/features/materialized-views.rst): a base write succeeds only after the view was updated
    at the write's consistency level, so the new data is immediately visible in the view.

    Checks: a write that returns success through Z is visible in the view right away, read through
    Z at CL=ONE (Y answers it); a write that cannot reach the view replica fails instead.
    """
    servers, cql, hosts = await boot(manager, failure_detector_timeout)
    x, y, z = servers
    host_z = hosts[2]
    async with new_test_keyspace(manager, TABLETS_RF1) as ks:
        base, view = await make_base_and_view(manager, cql, ks, x, y, True)
        await cql.run_async(f"ALTER MATERIALIZED VIEW {view} WITH synchronous_updates = true")
        await wait_for_view(cql, "mv", 3)

        async def write(pk: int) -> tuple[str, float]:
            stmt = SimpleStatement(f"INSERT INTO {base} (pk, v, w) VALUES ({pk}, {pk}, 'w{pk}')", consistency_level=ConsistencyLevel.ONE)
            stmt.retry_policy = FallthroughRetryPolicy()
            t0 = time.monotonic()
            try:
                await cql.run_async(stmt, host=host_z)
                return "ok", time.monotonic() - t0
            except CLIENT_ERRORS as e:
                return f"{type(e).__name__}: {str(e)[:200]}", time.monotonic() - t0

        async def in_view(pk: int) -> bool:
            stmt = SimpleStatement(f"SELECT pk FROM {view} WHERE v = {pk} AND pk = {pk}", consistency_level=ConsistencyLevel.ONE)
            return bool(list(await cql.run_async(stmt, host=host_z)))

        assert (await write(1))[0] == "ok"
        assert await in_view(1), "a synchronous write is not visible in the view with every link healthy"

        logger.info(f"Cutting {x.ip_addr} <-> {y.ip_addr}; Y stays reachable from Z and the clients")
        await network_proxy.partition(x.ip_addr, y.ip_addr)
        # Write 2: Y not yet known DOWN on X. Write 3: after X marked Y DOWN.
        result2 = await write(2)
        logger.info(f"write 2 (cut, not yet detected): {result2}")
        await wait_for_down(manager, network_proxy, x, {y.ip_addr})
        result3 = await write(3)
        logger.info(f"write 3 (Y DOWN for X): {result3}")
        assert await down_endpoints(manager, network_proxy, z) == set(), "Z should still see every node UP"
        visible = {pk: await in_view(pk) for pk in (2, 3)}
        hints = await metric(manager, x, "scylla_hints_for_views_manager_written")
        logger.info(f"visible in the view through Z right after the acks: {visible}; view hints on X: {hints}")
        violations = [f"write {pk} acked ({res[1]:.1f}s) but not in the view"
                      for pk, res in ((2, result2), (3, result3)) if res[0] == "ok" and not visible[pk]]
        await heal_and_wait_all_up(manager, network_proxy, servers)
        await wait_view_hints_delivered(manager, servers, 60)
        logger.info(f"after the heal: {[(pk, await in_view(pk)) for pk in (2, 3)]}")
        if violations:
            raise SyncViewWriteAcked("; ".join(violations))

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A repair with tombstone_gc in repair mode waits for the hints written before it to be sent,
whatever table they belong to. These tests exercise repair_hints_batchlog_flush_discard_unreplayed_hints,
the option that discards those hints when the wait times out, so that the next repair does not wait for them.
"""

import logging
import time
from typing import Awaitable, Callable

import pytest
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.test_cluster_config import CLUSTER_CONFIGS_QUERY, wait_for_config_map_value_on_hosts

logger = logging.getLogger(__name__)

PAUSE_REPLAY = "hinted_handoff_pause_hint_replay"
SHORT_TIMEOUT = "repair_flush_hints_timeout_in_s"
OPTION = "repair_hints_batchlog_flush_discard_unreplayed_hints"
# Logged by the coordinator for every node whose flush failed.
FLUSH_FAILED_LOG = "Sending repair_flush_hints_batchlog to node=.* failed"
# Logged by a node that discards its hints after its flush timed out.
DISCARD_LOG = "discarding the hints written before the request"


async def start_cluster(manager: ScyllaClusterManager, nodes: int) -> list[ServerInfo]:
    # Vnodes: a tablet repair fails outright when the flush fails, a vnode repair only records no repair time.
    # A write at CL TWO must not time out on a slow replica, that would hint the replica.
    cfg = {"tablets_mode_for_new_keyspaces": "disabled", "repair_hints_batchlog_flush_cache_time_in_ms": 0,
           "write_request_timeout_in_ms": 30000}
    servers = await manager.servers_add(nodes, config=cfg, cmdline=["--smp", "1"], auto_rack_dc="dc1")
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    # The repaired table is not the hinted one, so the hints are the only way the hinted rows reach the other node.
    await cql.run_async("CREATE TABLE ks.repaired (pk int PRIMARY KEY, v int) WITH tombstone_gc = {'mode': 'repair'}")
    await cql.run_async("CREATE TABLE ks.hinted (pk int PRIMARY KEY, v int)")
    return servers


async def write_hinted(manager: ScyllaClusterManager, cql, down: ServerInfo, keys: range, both_replicas_live: frozenset[int] = frozenset()) -> None:
    """
    Writes the keys to ks.hinted with `down` stopped, so that the live nodes store hints for them.

    A write waits for every live replica, so that a slow one is not hinted on the write timeout:
    a key in `both_replicas_live` has two live replicas and is written at CL TWO, any other at CL ONE.
    """
    await manager.server_stop_gracefully(down.server_id)
    await manager.others_not_see_server(down.ip_addr)
    # v differs from pk so that a view keyed by v has a different token, and possibly different replicas, than the base row.
    for pk in keys:
        cl = ConsistencyLevel.TWO if pk in both_replicas_live else ConsistencyLevel.ONE
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.hinted (pk, v) VALUES ({pk}, {pk + 1000})", consistency_level=cl))


async def metric_sum(manager: ScyllaClusterManager, servers: list[ServerInfo], name: str) -> float:
    """Sums the metric over the servers; a counter may not be exported while it is zero."""
    values = [(await manager.metrics.query(s.ip_addr)).get(name) or 0 for s in servers]
    return sum(values)


async def wait_for_metric(manager: ScyllaClusterManager, servers: list[ServerInfo], name: str, expected: float, timeout: float = 60) -> None:
    async def check():
        value = await metric_sum(manager, servers, name)
        logger.debug(f"{name} = {value}, waiting for {expected}")
        return True if value >= expected else None
    await wait_for(check, time.time() + timeout)


async def wait_for_hints_stored(manager: ScyllaClusterManager, servers: list[ServerInfo], group: str, expected: float) -> None:
    """Waits until the manager `group` has stored `expected` hints and none is still being stored."""
    await wait_for_metric(manager, servers, f"scylla_{group}_written", expected)

    async def check():
        return True if await metric_sum(manager, servers, f"scylla_{group}_size_of_hints_in_progress") == 0 else None
    await wait_for(check, time.time() + 60)


async def wait_for_hints_sent(manager: ScyllaClusterManager, servers: list[ServerInfo], group: str, at_least: float) -> None:
    """
    Waits until every hint the servers hold is sent or discarded, then checks that the manager `group`
    sent at least `at_least` of them. A sync point covers both managers and is met once the hints
    it was created after are sent or discarded.
    """
    for server in servers:
        sync_point = await manager.api.client.post_json("/hinted_handoff/sync_point", host=server.ip_addr)

        async def check():
            status = await manager.api.client.get_json("/hinted_handoff/sync_point", host=server.ip_addr,
                                                       params={"id": sync_point, "timeout": "10"})
            return True if status == "DONE" else None
        await wait_for(check, time.time() + 120)
    assert await metric_sum(manager, servers, f"scylla_{group}_sent_total") >= at_least


async def repair_until(manager: ScyllaClusterManager, node: ServerInfo, done: Callable[[], Awaitable[bool]]) -> None:
    """
    Repairs ks.repaired from `node` until `done` holds. The coordinator skips the flush when it sees
    a node down at that moment, and a node may read the option before its cluster config cache caught up.
    """
    async def check():
        await manager.api.repair(node.ip_addr, "ks", "repaired")
        return True if await done() else None
    await wait_for(check, time.time() + 120, period=1)


async def repair_until_discarding(manager: ScyllaClusterManager, node: ServerInfo, hinted: list[ServerInfo]) -> None:
    """
    Repairs ks.repaired from `node`, for the same reasons as repair_until, until every node in `hinted`
    has discarded its regular hints at least once. One discard covers every hint written before the
    request, so once is enough; a node whose flush times out on view hints alone logs the discard too.

    The discard is logged before the flush response, so this returns before any later repair may find
    the hints gone; the discard counters are updated in the background and are waited for by the caller.
    """
    logs = [await manager.server_open_log(n.server_id) for n in hinted]
    marks = [await log.mark() for log in logs]

    async def discarded():
        await manager.api.repair(node.ip_addr, "ks", "repaired")
        found = [await log.grep(DISCARD_LOG, from_mark=mark) for log, mark in zip(logs, marks)]
        return True if all(found) else None
    await wait_for(discarded, time.time() + 120, period=1)


async def keys_replicated_on(manager: ScyllaClusterManager, asked: ServerInfo, node: ServerInfo, table: str, keys: dict[int, int]) -> set[int]:
    """Returns the keys whose row in `table` has `node` as a replica; `keys` maps a key to the row's partition key."""
    result = set()
    for key, pk in keys.items():
        if node.ip_addr in await manager.api.natural_endpoints(asked.ip_addr, "ks", table, str(pk)):
            result.add(key)
    return result


async def repair_recorded(cql, host) -> bool:
    rows = await cql.run_async("SELECT keyspace_name FROM system.repair_history", host=host)
    return any(r.keyspace_name == "ks" for r in rows)


async def set_option(cql, hosts, value: bool) -> None:
    """Sets the option cluster-wide and waits until every host has the new value."""
    await cql.run_async(f"ALTER CLUSTER WITH {OPTION} = {str(value).lower()}")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], OPTION, str(value).lower())


async def keys_on(manager: ScyllaClusterManager, server: ServerInfo, table: str) -> set[int]:
    """Returns the pk column of `table` as stored on `server`, which must be the only node up and a replica of every row."""
    cql = await manager.get_cql_exclusive(server)
    rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table}", consistency_level=ConsistencyLevel.ONE))
    return {r.pk for r in rows}


async def rows_on(manager: ScyllaClusterManager, server: ServerInfo, table: str, pk_column: str, keys: dict[int, int]) -> set[int]:
    """Returns the keys whose row is stored on `server`, the only node up; `keys` maps a key to a partition key `server` replicates."""
    cql = await manager.get_cql_exclusive(server)
    result = set()
    for key, pk in keys.items():
        rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table} WHERE {pk_column} = {pk}", consistency_level=ConsistencyLevel.ONE))
        if rows:
            result.add(key)
    return result


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_repair_discards_hints_it_cannot_wait_for(manager: ScyllaClusterManager):
    """
    With the option off, a repair whose hints flush times out records no repair time and leaves the
    hints alone. With the option on, it discards them, so the next repair records a repair time.
    Hints written after the discard are still sent, and the discarded rows never reach the other node.
    """
    n1, n2 = await start_cluster(manager, 2)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [n1, n2], time.time() + 60)
    host1 = hosts[0]

    old_keys = range(0, 50)
    new_keys = range(50, 100)

    await write_hinted(manager, cql, n2, old_keys)
    await wait_for_metric(manager, [n1], "scylla_hints_manager_written", len(old_keys))

    # Nothing is sent from now on, so every flush waits for the hints until it times out.
    await manager.api.enable_injection(n1.ip_addr, PAUSE_REPLAY, one_shot=False)
    await manager.server_start(n2.server_id)
    await manager.servers_see_each_other([n1, n2])
    await manager.api.enable_injection(n1.ip_addr, SHORT_TIMEOUT, one_shot=False, parameters={"value": "3"})

    # Option off: the repair goes on without a repair time and the hints stay.
    log1 = await manager.server_open_log(n1.server_id)
    mark = await log1.mark()
    await manager.api.repair(n1.ip_addr, "ks", "repaired")
    assert await log1.grep(FLUSH_FAILED_LOG, from_mark=mark)
    assert not await log1.grep(DISCARD_LOG, from_mark=mark)
    assert not await repair_recorded(cql, host1)
    assert await metric_sum(manager, [n1], "scylla_hints_manager_discarded_on_failed_replay") == 0

    # Option on: the repair still records nothing, but the hints are discarded.
    await set_option(cql, hosts, True)
    await repair_until_discarding(manager, n1, [n1])
    assert not await repair_recorded(cql, host1)
    await wait_for_metric(manager, [n1], "scylla_hints_manager_discarded_on_failed_replay", len(old_keys))

    # The next repair has nothing to wait for and records a repair time, with replay still paused.
    await repair_until(manager, n1, lambda: repair_recorded(cql, host1))

    # Hints written after the discard are sent as usual, the discarded ones are not.
    await manager.api.disable_injection(n1.ip_addr, PAUSE_REPLAY)
    await write_hinted(manager, cql, n2, new_keys)
    await wait_for_hints_stored(manager, [n1], "hints_manager", len(old_keys) + len(new_keys))
    await manager.server_start(n2.server_id)
    await manager.servers_see_each_other([n1, n2])
    await wait_for_hints_sent(manager, [n1], "hints_manager", len(new_keys))

    await manager.server_stop_gracefully(n1.server_id)
    await manager.others_not_see_server(n1.ip_addr)
    assert await keys_on(manager, n2, "ks.hinted") == set(new_keys)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_repair_never_discards_view_hints(manager: ScyllaClusterManager):
    """
    The flush waits for view hints too, but the option discards only regular hints: after the discard
    the repair still times out on the view hints, and once replay resumes they are sent.

    A base replica that is also a view replica applies its view updates locally and writes no view
    hint. So the view rows must have tokens different from their base rows, and with two nodes and
    RF 2 no view hint is ever written. With three nodes and RF 2 the stopped node gets exactly one
    view hint for every key it is a view replica of without being a base replica of, from the live
    base replica paired with it, and exactly one regular hint for every key it is a base replica of.
    """
    n1, n2, n3 = await start_cluster(manager, 3)
    live = [n1, n2]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [n1, n2, n3], time.time() + 60)
    host1 = hosts[0]
    await cql.run_async("CREATE MATERIALIZED VIEW ks.hinted_by_v AS SELECT * FROM ks.hinted "
                        "WHERE v IS NOT NULL PRIMARY KEY (v, pk)")
    # The base replicas write view updates only once they know the view.
    for server in (n1, n2, n3):
        await read_barrier(manager.api, server.ip_addr)

    keys = range(0, 100)
    base_on_n3 = await keys_replicated_on(manager, n1, n3, "hinted", {pk: pk for pk in keys})
    view_on_n3 = await keys_replicated_on(manager, n1, n3, "hinted_by_v", {pk: pk + 1000 for pk in keys})
    view_rows_for_n3 = view_on_n3 - base_on_n3
    assert base_on_n3 and view_rows_for_n3

    await write_hinted(manager, cql, n3, keys, both_replicas_live=frozenset(keys) - base_on_n3)
    await wait_for_hints_stored(manager, live, "hints_manager", len(base_on_n3))
    await wait_for_hints_stored(manager, live, "hints_for_views_manager", len(view_rows_for_n3))

    # Every node gets the flush request; the write coordinators hold the regular hints, the paired base replicas the view hints.
    for node in live:
        await manager.api.enable_injection(node.ip_addr, PAUSE_REPLAY, one_shot=False)
    await manager.server_start(n3.server_id)
    await manager.servers_see_each_other([n1, n2, n3])
    await set_option(cql, hosts, True)
    await manager.api.enable_injection(n1.ip_addr, SHORT_TIMEOUT, one_shot=False, parameters={"value": "3"})

    hinted = [n for n in live if await metric_sum(manager, [n], "scylla_hints_manager_written") > 0]
    await repair_until_discarding(manager, n1, hinted)
    await wait_for_metric(manager, live, "scylla_hints_manager_discarded_on_failed_replay", len(base_on_n3))
    assert await metric_sum(manager, live, "scylla_hints_for_views_manager_discarded_on_failed_replay") == 0

    # The view hints still hold the flush back.
    log1 = await manager.server_open_log(n1.server_id)
    mark = await log1.mark()
    await manager.api.repair(n1.ip_addr, "ks", "repaired")
    assert await log1.grep(FLUSH_FAILED_LOG, from_mark=mark)
    assert not await repair_recorded(cql, host1)
    assert await metric_sum(manager, live, "scylla_hints_for_views_manager_discarded_on_failed_replay") == 0

    for node in live:
        await manager.api.disable_injection(node.ip_addr, PAUSE_REPLAY)
    await wait_for_hints_sent(manager, live, "hints_for_views_manager", len(view_rows_for_n3))

    # n3 got the view rows from the view hints and nothing from the discarded base hints.
    for node in live:
        await manager.server_stop_gracefully(node.server_id)
        await manager.others_not_see_server(node.ip_addr)
    assert await rows_on(manager, n3, "ks.hinted_by_v", "v", {pk: pk + 1000 for pk in view_on_n3}) == view_rows_for_n3
    assert await rows_on(manager, n3, "ks.hinted", "pk", {pk: pk for pk in base_on_n3}) == set()

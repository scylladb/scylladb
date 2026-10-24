#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Strongly consistent requests where the server computes the partition key.

`INSERT ... VALUES (uuid(), ...)` and `SELECT ... WHERE pk = uuid()` do not say which key
they are about. The node that got the request from the client runs `uuid()`, finds the
tablet for that value and forwards the request to the leader of that tablet. Every node
and shard that handles the request after that must use the same value. That is why the
value is sent along with the request (`cached_pk_function_calls` in the query options,
`cached_fn_calls` in the forwarded request). If some hop runs `uuid()` again, the request
changes its key on the way: it was sent to the right place for one value, but it is
executed for another one, and it keeps getting sent around until a new value happens to
land on the right node and shard.

The tests look at this from the outside, with the coordinator metrics. On the node that
got the request from the client, `requests_forwarded_redirected` counts how often a
target node answered "not mine". On the target nodes, `*_node_bounces` and
`*_shard_bounces` count how often the target had to send the request somewhere else.
When the key is written in the statement, none of these counters move once the leader
cache is warm. A key computed by the server must behave the same way.

Today it does not (SCYLLADB-4809): a node forward sends the values that came with the
request, not the ones it just computed, and a strongly consistent write never saves the
computed values at all. The cases that fail because of this are skipped until it is fixed.
"""

from __future__ import annotations

import logging
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
NODE_BOUNCES = {'insert': 'scylla_strong_consistency_coordinator_write_node_bounces',
                'select': 'scylla_strong_consistency_coordinator_read_node_bounces'}
SHARD_BOUNCES = {'insert': 'scylla_strong_consistency_coordinator_write_shard_bounces',
                 'select': 'scylla_strong_consistency_coordinator_read_shard_bounces'}
SKIP_REASON = "a partition key computed by uuid() is evaluated again on every hop and the request is re-routed"

WARMUP_STATEMENTS = 200
CONTROL_STATEMENTS = 20
STATEMENTS = 30


def statement(kind: str, table: str, key: str, value: int) -> SimpleStatement:
    """Build an INSERT or a QUORUM SELECT for one key. `key` is CQL text: a value or `uuid()`."""
    if kind == 'insert':
        return SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({key}, {value})")
    return SimpleStatement(f"SELECT c FROM {table} WHERE pk = {key}", consistency_level=ConsistencyLevel.QUORUM)


async def metric(manager: ScyllaClusterManager, server: ServerInfo, name: str) -> float:
    """Read one counter of a server, added up over its shards. 0 if the server has not reported it yet."""
    return (await manager.metrics.query(server.ip_addr)).get(name) or 0


async def routing_counters(manager: ScyllaClusterManager, coordinator: ServerInfo, others: list[ServerInfo], kind: str) -> dict[str, float]:
    """Read the counters that move when a request is sent on again after the coordinator forwarded
    it: redirects seen by the coordinator, and node and shard bounces on every other node.
    One scrape of /metrics per node."""
    counters = {'redirected': await metric(manager, coordinator, REDIRECTED)}
    for s in others:
        metrics = await manager.metrics.query(s.ip_addr)
        counters[f'node_bounces@{s.ip_addr}'] = metrics.get(NODE_BOUNCES[kind]) or 0
        counters[f'shard_bounces@{s.ip_addr}'] = metrics.get(SHARD_BOUNCES[kind]) or 0
    return counters


def delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {k: after[k] - before[k] for k in before if after[k] != before[k]}


@pytest.mark.asyncio
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4809", reason=SKIP_REASON)
@pytest.mark.parametrize("kind", ['insert', 'select'])
async def test_non_pure_pk_function_is_evaluated_once_per_request(manager: ScyllaClusterManager, kind: str):
    """A request whose key comes from `uuid()` must be routed like a request with a plain key:
    `uuid()` runs once, on the node that got the request from the client, and every later hop
    uses that value.

    4 nodes, RF=3, 8 tablets. Every request goes to the same node. First the leader cache of
    that node is warmed with plain keys. Then a control run with plain keys shows what a
    request costs on a warm cache: it goes straight to the leader, nobody redirects it and
    nobody bounces it. The same requests with `uuid()` as the key must leave the counters
    just as they are.
    """
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    # A tablet that moves during the run makes the leader cache stale: a redirect that has
    # nothing to do with uuid().
    await manager.disable_tablet_balancing()
    cql, hosts = await manager.get_ready_cql(servers)
    coordinator, others = servers[0], servers[1:]
    via = hosts[0]

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 8} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk uuid PRIMARY KEY, c int") as table:
            # Warm the coordinator's leader cache on every shard for every tablet. Text
            # statements carry no routing key, so the driver spreads them over the shards.
            for i in range(WARMUP_STATEMENTS):
                await cql.run_async(statement('insert', table, str(uuid.uuid4()), i), host=via)

            async def rerouted(n: int, key: str | None = None) -> dict[int, dict[str, float]]:
                """Run n requests, each with `key` or a fresh literal; the counter deltas of the re-routed ones."""
                found = {}
                for i in range(n):
                    before = await routing_counters(manager, coordinator, others, kind)
                    rows = await cql.run_async(statement(kind, table, key or str(uuid.uuid4()), i), host=via)
                    assert not rows, f"a read of a fresh key returned rows: {rows}"
                    if d := delta(before, await routing_counters(manager, coordinator, others, kind)):
                        found[i] = d
                return found

            # Control: literal keys. This is what "one key per request" costs on a warm cache.
            if control := await rerouted(CONTROL_STATEMENTS):
                pytest.fail(f"premise: with literal keys the routing is not settled yet: {control}")

            # The same requests, key computed by the server.
            offenders = await rerouted(STATEMENTS, 'uuid()')

            logger.info(f"{kind} with uuid(): {len(offenders)} of {STATEMENTS} requests were re-routed after the coordinator: {offenders}")
            assert not offenders, (
                f"{len(offenders)} of {STATEMENTS} {kind}s with uuid() as the key were re-routed after the node the client "
                f"talked to had routed them, while {CONTROL_STATEMENTS} {kind}s with literal keys were not: {offenders}")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [
    pytest.param('insert', marks=[pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4809", reason=SKIP_REASON)]),
    'select',
])
async def test_non_pure_pk_function_shard_bounce_is_at_most_one(manager: ScyllaClusterManager, kind: str):
    """On one node, a request whose key comes from `uuid()` is sent to another shard at most
    once. The token picks the shard, and the computed value travels with the request, so the
    shard that receives it has nothing left to route.

    One node, RF=1, 8 tablets spread over its shards. A text statement lands on a random
    shard, so about half of the requests need one bounce. A second bounce for the same
    request means the receiving shard ran `uuid()` again and got a different token.
    """
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    # A tablet that moves between shards during the run is bounced twice for a reason that has
    # nothing to do with uuid().
    await manager.disable_tablet_balancing()
    cql, _ = await manager.get_ready_cql([server])

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk uuid PRIMARY KEY, c int") as table:
            table_id = await manager.get_table_id(*table.split('.'))
            shards = {r.replicas[0][1] for r in await cql.run_async(f"SELECT replicas FROM system.tablets WHERE table_id = {table_id}")}
            if len(shards) < 2:
                pytest.fail(f"premise: all tablets on one shard ({shards}), nothing to bounce between")

            bounces = SHARD_BOUNCES[kind]
            offenders = {}
            for i in range(STATEMENTS):
                before = await metric(manager, server, bounces)
                await cql.run_async(statement(kind, table, 'uuid()', i))
                if (d := await metric(manager, server, bounces) - before) > 1:
                    offenders[i] = d

            assert not offenders, (
                f"{len(offenders)} of {STATEMENTS} {kind}s with uuid() as the key were bounced between shards more "
                f"than once (bounces per request: {offenders}): the receiving shard evaluated uuid() again")

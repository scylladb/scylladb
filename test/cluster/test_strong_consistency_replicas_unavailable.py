#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest
from cassandra import ConsistencyLevel, ReadTimeout, Unavailable, WriteTimeout
from cassandra.cluster import Session
from cassandra.policies import FallthroughRetryPolicy
from cassandra.pool import Host
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import wait_for

KEYSPACE_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                 " AND tablets = {'initial': 1} AND consistency = 'global'")
NO_QUORUM_CONFIG = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 3000, 'read_request_timeout_in_ms': 3000}
WRITE_ERRORS = 'scylla_strong_consistency_coordinator_write_errors'
READ_ERRORS = 'scylla_strong_consistency_coordinator_read_errors'
READ_COUNT = 'scylla_strong_consistency_coordinator_read_latency_count'


async def get_roles(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str, table_name: str) \
        -> tuple[str, ServerInfo, list[ServerInfo], ServerInfo]:
    """Split `servers` by their role for the table's single tablet: (raft group id, leader, followers, non-replica)."""
    group_id = await get_table_raft_group_id(manager, ks, table_name)
    replica_host_ids = [str(r[0]) for r in await get_tablet_replicas(manager, servers[0], ks, table_name, 0)]
    by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}
    replicas = [by_host_id[hid] for hid in replica_host_ids]
    leader = by_host_id[str(await wait_for_leader(manager, replicas[0], group_id))]
    followers = [s for s in replicas if s != leader]
    [non_replica] = [s for hid, s in by_host_id.items() if hid not in replica_host_ids]
    return group_id, leader, followers, non_replica


async def get_metric(manager: ScyllaClusterManager, server: ServerInfo, name: str, labels: dict[str, str] = {}) -> float:
    """The metric summed over shards, 0 while the server has not exposed it yet."""
    return (await manager.metrics.query(server.ip_addr)).get(name, labels) or 0


async def attempt(cql: Session, query: str | SimpleStatement, host: Host) -> tuple[list[int] | Exception, float]:
    """Run `query` pinned to `host`; return the `c` values it read, or the error it failed with, and how long it took."""
    start = time.monotonic()
    try:
        rows = await cql.run_async(query, host=host)
        outcome = [] if rows is None else [r.c for r in rows]
    except Exception as e:
        outcome = e
    return outcome, time.monotonic() - start


async def expect_error(cql: Session, query: str | SimpleStatement, host: Host, exc_type: type[Exception]) -> tuple[Exception, float]:
    """Run `query` pinned to `host`, expecting `exc_type`; return the exception and how long the request took."""
    outcome, elapsed = await attempt(cql, query, host)
    assert isinstance(outcome, exc_type), f"expected {exc_type.__name__} via {host}, got {outcome!r} after {elapsed:.3f}s"
    return outcome, elapsed


@asynccontextmanager
async def sc_table(manager: ScyllaClusterManager, config: dict = NO_QUORUM_CONFIG) \
        -> AsyncIterator[tuple[Session, dict[ServerInfo, Host], str, tuple[str, ServerInfo, list[ServerInfo], ServerInfo]]]:
    """A 4-node cluster with a strongly consistent single-tablet table of RF=3: (cql, host by server, table, get_roles())."""
    servers = await manager.servers_add(4, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks, new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
        yield cql, dict(zip(servers, hosts)), table, await get_roles(manager, servers, ks, table.split('.')[-1])


@asynccontextmanager
async def down(manager: ScyllaClusterManager, servers: list[ServerInfo], kill: list[ServerInfo]) -> AsyncIterator[None]:
    """SIGKILL `kill` one after another, each seen as down by every live node before the next goes, and on exit
    start them again and wait for all `servers` - also when the body fails, as dropping the test table needs them back."""
    for s in kill:
        await manager.server_stop(s.server_id, convict=True)
        await manager.others_not_see_server(s.ip_addr)
    try:
        yield
    finally:
        for s in kill:
            await manager.server_start(s.server_id)
        await manager.get_ready_cql(servers)


@pytest.mark.asyncio
async def test_all_replicas_down_fails_fast(manager: ScyllaClusterManager):
    """
    Verify that a request against a strongly consistent table, coordinated by a node that
    is not a replica of the tablet, fails immediately with Unavailable when every replica
    is down, rather than waiting out the request timeout: with nobody to forward to the
    outcome cannot change, so the client should learn it at once. That holds for a write,
    a QUORUM (linearizable) read and a CL=ONE (relaxed) read alike.

    The request timeouts are raised to 10s so that "fast" and "timed out" cannot be
    confused, and the coordinator's error counters are checked to be untouched: the
    refusal happens before the coordinator starts waiting for anything. Once the replicas
    are back, the acknowledged write is still there and the refused one left nothing.
    """
    config = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 10000, 'read_request_timeout_in_ms': 10000}
    async with sc_table(manager, config) as (cql, host_of, table, (group_id, leader, followers, non_replica)):
        coordinator = host_of[non_replica]
        reads = [SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=cl)
                 for cl in (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE)]

        # Going through the non-replica also fills its leader cache, so the refusal below
        # has to see past a cached leader that is dead by then.
        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=coordinator)
        errors_before = [await get_metric(manager, non_replica, name) for name in (WRITE_ERRORS, READ_ERRORS)]

        async with down(manager, list(host_of), [leader] + followers):
            for query in (f"INSERT INTO {table} (pk, c) VALUES (0, 2)", *reads):
                e, elapsed = await expect_error(cql, query, coordinator, Unavailable)
                assert (e.required_replicas, e.alive_replicas) == (1, 0) and "All replicas" in str(e), str(e)
                assert elapsed < 5.0, f"{query} took {elapsed:.1f}s to be refused"

            errors_after = [await get_metric(manager, non_replica, name) for name in (WRITE_ERRORS, READ_ERRORS)]
            assert errors_after == errors_before, "the refusal was accounted as a coordinator error"

        await wait_for_leader(manager, leader, group_id)
        assert [r.c for r in await cql.run_async(reads[0], host=coordinator)] == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("survivor", ["leader", "follower"])
async def test_no_quorum_times_out_and_relaxed_read_answers(manager: ScyllaClusterManager, survivor: str):
    """
    Verify what a strongly consistent table does with one replica alive and no quorum:
    a write and a QUORUM (linearizable) read wait for a leader for the whole request
    timeout and fail with WriteTimeout/ReadTimeout, while a CL=ONE read answers at once
    from the survivor's local state - the documented weaker guarantee of the relaxed
    read. Both the survivor and a non-replica coordinate the requests, so the forwarding
    to the only live replica is covered too. The survivor is either the former leader or
    a follower, since the two reach "no leader" along different raft paths (a stepdown
    for lack of a heartbeat quorum vs. the failure detector giving up on the leader).
    Once the majority is back, the restarted replicas catch up on whatever value won,
    whether or not a timed-out write made it.

    This is the steady state, reached once the survivor has given up on the dead leader.
    In the first seconds after its death a surviving follower still names it, the
    coordinator redirects there, and the redirect fails at once with WriteFailure/
    ReadFailure "failed while forwarding" (SCYLLADB-4758, not a bug). That window is an
    artefact of raft's election timeout, so it is waited out below rather than asserted
    on. The driver-facing Scylla bug that clients hit around it is kept out of the way
    here and pinned by its own test.

    Open product question recorded here: today "replicas alive but no quorum" surfaces
    as a timeout after the full wait rather than as an immediate Unavailable. Should the
    product decide otherwise, the timeout assertions below flip.
    """
    async with sc_table(manager) as (cql, host_of, table, (group_id, leader, followers, non_replica)):
        alive = leader if survivor == "leader" else followers[0]
        # Retries off: SCYLLADB-4756 (see test_forwarded_read_timeout_does_not_downgrade_quorum_read).
        no_retry = FallthroughRetryPolicy()
        write = SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES (0, 2)", retry_policy=no_retry)
        quorum_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM,
                                      retry_policy=no_retry)
        relaxed_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)

        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=host_of[leader])
        assert [r.c for r in await cql.run_async(quorum_read, host=host_of[leader])] == [1]
        relaxed_reads_before = await get_metric(manager, alive, READ_COUNT, {'read_type': 'non_linearizable'})
        write_timeouts_before = await get_metric(manager, alive, WRITE_ERRORS, {'reason': 'timeout'})

        # The leader dies last: killed first, it would leave the two followers a quorum to elect
        # one of them - possibly the survivor, turning the follower case into the leader case.
        async with down(manager, list(host_of), [s for s in followers + [leader] if s != alive]):
            # A surviving follower keeps naming the dead leader until its election timeout fires, and
            # requests fail fast meanwhile (see the docstring); without a quorum nobody gets elected,
            # so "no leader" is the lasting state.
            async def no_leader() -> bool | None:
                return uuid.UUID(await manager.api.get_raft_leader(alive.ip_addr, group_id)).int == 0 or None
            await wait_for(no_leader, time.time() + 60, label=f"{alive} to report no leader of group {group_id}")

            for coordinator in (host_of[alive], host_of[non_replica]):
                _, elapsed = await expect_error(cql, write, coordinator, WriteTimeout)
                assert elapsed >= 2.5, f"the write via {coordinator} failed after {elapsed:.1f}s, before its timeout"
                _, elapsed = await expect_error(cql, quorum_read, coordinator, ReadTimeout)
                assert elapsed >= 2.5, f"the QUORUM read via {coordinator} failed after {elapsed:.1f}s, before its timeout"
                rows, elapsed = await attempt(cql, relaxed_read, coordinator)
                assert rows == [1], f"the CL=ONE read via {coordinator} returned {rows!r}"
                assert elapsed < 1.5, f"the CL=ONE read via {coordinator} took {elapsed:.1f}s"

            # Both relaxed reads and both write timeouts land on the survivor: the non-replica only
            # forwards the requests, and a forwarding timeout is not a coordinator error of its own.
            assert await get_metric(manager, alive, READ_COUNT, {'read_type': 'non_linearizable'}) >= relaxed_reads_before + 2
            assert await get_metric(manager, alive, WRITE_ERRORS, {'reason': 'timeout'}) >= write_timeouts_before + 2

        await wait_for_leader(manager, alive, group_id)
        # A timed-out write may or may not have been committed; whichever it is, the restarted replicas
        # have to catch up on it. A linearizable read is served by the leader wherever it is sent, so
        # only a CL=ONE read pinned to a replica, served from its own storage, shows that.
        expected = [r.c for r in await cql.run_async(quorum_read, host=host_of[alive])]
        assert expected in ([1], [2]), f"the leader reads {expected}"
        for replica in [leader] + followers:
            async def caught_up() -> bool | None:
                rows, _ = await attempt(cql, relaxed_read, host_of[replica])
                return rows == expected or None
            await wait_for(caught_up, time.time() + 60, label=f"{replica} to read {expected} locally")


@pytest.mark.asyncio
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4756",
                      reason="a QUORUM read is silently downgraded to CL=ONE after a forwarded request times out")
async def test_forwarded_read_timeout_does_not_downgrade_quorum_read(manager: ScyllaClusterManager):
    """
    Verify that a QUORUM (linearizable) read which a non-replica forwards to the only live
    replica, and which times out there for lack of a quorum, fails for a client with the
    driver's default settings instead of returning rows: the client asked for a
    linearizable read, and the lone replica can only offer possibly stale local state.

    The root cause is checked on the error the same read gets with driver retries off:
    it has to report fewer responses than required - that is what keeps the default
    retry policy from re-sending the read, and it is how a timeout of the same read sent
    straight to the replica is reported.

    The survivor is the former leader: it times out a request instead of forwarding it
    elsewhere from the moment the other replicas die, so no settling wait is needed.

    Reproduces SCYLLADB-4756: a forwarded request that times out is answered with
    ReadTimeout(consistency=ONE, required_responses=0, received_responses=0,
    data_retrieved=False) (transport/server.cc), so the driver's default
    RetryPolicy.on_read_timeout re-sends the QUORUM read at CL=ONE and the only live
    replica serves it as a relaxed read.
    """
    async with sc_table(manager) as (cql, host_of, table, (_, leader, followers, non_replica)):
        query = f"SELECT c FROM {table} WHERE pk = 0"
        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=host_of[leader])

        async with down(manager, list(host_of), followers):
            rows, elapsed = await attempt(cql, SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM),
                                          host_of[non_replica])
            error, _ = await attempt(cql, SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM,
                                                          retry_policy=FallthroughRetryPolicy()), host_of[non_replica])

        if not isinstance(error, ReadTimeout):
            pytest.fail(f"expected ReadTimeout with driver retries off, got {error!r}")
        assert isinstance(rows, Exception), f"the QUORUM read without a quorum returned rows={rows} after {elapsed:.1f}s"
        assert error.required_responses > error.received_responses, \
            f"the timeout of a QUORUM read is reported as {error}, which the default retry policy re-sends at {error.consistency}"

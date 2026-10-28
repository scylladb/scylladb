#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The paxos state a replica acknowledged must survive its crash.

Idea: paxos is safe because every round runs through a quorum that overlaps
every earlier one, and the replicas in the overlap remember what they
promised and what they accepted.  A replica that acknowledges a promise or
a proposal before it is durable, and then crashes, takes that memory away:
a later round can get a second value chosen on top of a chosen one, or a
proposal accepted under a ballot the replica had already promised away.
Each test builds one such interleaving with error injections and a SIGKILL,
and checks what the clients see: two compare-and-sets conditioned on the same
initial value must not both apply, and a SERIAL read must return the value
the acknowledged one wrote.

Setup, for both tests: three nodes, RF=3, one tablet or vnodes, and the
commitlog synced only on demand (period of an hour), so a paxos state table
write that is not synced explicitly is lost with the kill.  n1 is the replica
that crashes.  The vnodes flavour, whose paxos state lives in the
always-synced system.paxos, is the control.  Both tests also run with
durable_writes = false, where the tablets flavour is expected to fail: its
paxos state table is in the keyspace, inherits the setting, and never
reaches the commitlog.
"""

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# Only an explicit sync makes a commitlog write durable before a crash.
CONFIG = {"commitlog_sync_period_in_ms": 3600000, "rf_rack_valid_keyspaces": False}
CMDLINE = ["--logger-log-level", "paxos=trace"]

READ_ROUND_FALLBACKS = "scylla_storage_proxy_coordinator_cas_failed_read_round_optimization"


async def read_round_fallbacks(manager: ScyllaClusterManager, server) -> int:
    """How many paxos rounds this coordinator did not resolve from the promises alone."""
    # The metric is not exported while it is zero.
    return (await manager.metrics.query(server.ip_addr)).get(READ_ROUND_FALLBACKS) or 0


def cas(ks: str, value: int, cl: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) -> SimpleStatement:
    """Set c to value if it still holds the initial 0."""
    return SimpleStatement(f"UPDATE {ks}.t SET c = {value} WHERE pk = 1 IF c = 0",
                           consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


def serial_read(ks: str) -> SimpleStatement:
    return SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.SERIAL)


def keyspace_opts(tablets: bool, durable_writes: bool) -> str:
    tablets_opts = "{'initial': 1}" if tablets else "{'enabled': false}"
    return ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
            f"AND tablets = {tablets_opts} AND durable_writes = {str(durable_writes).lower()}")


FLAVOURS = [
    pytest.param(True, True, id="tablets"),
    pytest.param(False, True, id="vnodes"),
    pytest.param(True, False, id="tablets-not-durable",
                 marks=pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5756", strict=True)),
    pytest.param(False, False, id="vnodes-not-durable"),
]


async def create_table(manager: ScyllaClusterManager, cql, servers, ks: str) -> None:
    """One row with c = 0, durable on every replica before any injection."""
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c int)")
    await cql.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 0) IF NOT EXISTS")
    await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))


async def crash(manager: ScyllaClusterManager, cql, server) -> None:
    """SIGKILL the node and start it again: whatever it did not sync is gone."""
    await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)


@pytest.mark.parametrize("tablets, durable_writes", FLAVOURS)
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_chosen_value_survives_replica_crash(manager: ScyllaClusterManager, tablets: bool, durable_writes: bool):
    """A proposal accepted by a quorum stays chosen when an accepting replica crashes.

    The first CAS is coordinated by n2 with CL=ONE, so that one learn is
    enough for it to be acknowledged: n1 and n2 accept it (n3 fails to save
    the proposal) and n2 alone learns it (n1 and n3 fail to learn), so only
    n2 has the new value in the base table.  n1 is then crashed.  The second
    round runs on n3 with n2 refusing to promise, so its quorum is {n1, n3}:
    n3 never accepted the first value, and n1 only remembers it if its paxos
    state was synced before the kill.  A SERIAL read there must return the
    chosen value, and a CAS conditioned on the initial value must not apply.
    Both take the values prefetched with the promises (the read round
    optimization); a fallback read through n2 would see the learned value and
    hide the problem, so the test checks the coordinator's metric for that and
    aborts rather than passing by that route.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, keyspace_opts(tablets, durable_writes)) as ks:
        await create_table(manager, cql, servers, ks)

        logger.info("First CAS: accepted by n1 and n2, learned by n2 only")
        await manager.api.enable_injection(n3.ip_addr, "paxos_error_before_save_proposal", one_shot=False)
        for s in (n1, n3):
            await manager.api.enable_injection(s.ip_addr, "paxos_error_before_learn", one_shot=False)
        [row] = await cql.run_async(cas(ks, 1, cl=ConsistencyLevel.ONE), host=h2)
        assert row.applied, "scenario not reached: the first CAS was not applied"

        logger.info("Crash n1")
        await crash(manager, cql, n1)

        logger.info("Second round on n3 through the quorum {n1, n3}")
        await manager.api.disable_injection(n3.ip_addr, "paxos_error_before_save_proposal")
        await manager.api.disable_injection(n3.ip_addr, "paxos_error_before_learn")
        await manager.api.enable_injection(n2.ip_addr, "paxos_error_before_save_promise", one_shot=False)

        fallbacks = await read_round_fallbacks(manager, n3)
        [row] = await cql.run_async(serial_read(ks), host=h3)
        seen_by_read = row.c
        [row] = await cql.run_async(cas(ks, 2), host=h3)
        cas2_applied, cas2_saw = row.applied, getattr(row, "c", None)
        assert await read_round_fallbacks(manager, n3) == fallbacks, (
            "scenario not reached: n3 fell back to a regular read for the condition, "
            "which may have gone through n2")

        await manager.api.disable_injection(n2.ip_addr, "paxos_error_before_save_promise")
        [row] = await cql.run_async(serial_read(ks), host=h2)
        final = row.c

        problems = []
        if seen_by_read != 1:
            problems.append(f"a SERIAL read after the acknowledged CAS returned c={seen_by_read}, expected 1")
        if cas2_applied:
            problems.append("a second CAS conditioned on the initial value was applied on top of the first")
        elif cas2_saw != 1:
            problems.append(f"the second CAS was not applied but saw c={cas2_saw}, expected 1")
        if final != 1:
            problems.append(f"the final SERIAL read returned c={final}, expected 1")
        assert not problems, "the chosen value was lost with n1's paxos state:\n" + "\n".join(problems)


COORDINATOR_BEFORE_ACCEPT = "paxos_coordinator_before_accept"


@pytest.mark.parametrize("tablets, durable_writes", FLAVOURS)
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_promised_ballot_survives_replica_crash(manager: ScyllaClusterManager, tablets: bool, durable_writes: bool):
    """A replica keeps rejecting the ballots it promised away when it crashes.

    Two rounds are held between their prepare and their accept.  The first,
    on n2, is promised by every replica.  The second, on n3 with n2 refusing
    to promise, is then promised a higher ballot by n1 and n3.  n1 is crashed,
    and the rounds are released in order.  The first one's accept must now be
    rejected by n1 and n3, which promised the higher ballot, so it cannot get
    a quorum: it ends in a write timeout, since n2 did accept it.  The second
    one is accepted by all and applies.  If n1 forgot its promise with the
    crash, it accepts the first proposal, that one is chosen with n2 and
    applied, and then the second is accepted and applied too, with both
    clients told their CAS on the initial value went through.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, keyspace_opts(tablets, durable_writes)) as ks:
        await create_table(manager, cql, servers, ks)
        n1_log = await manager.server_open_log(n1.server_id)

        logger.info("First round on n2, held before its accept once every replica promised it")
        await manager.api.enable_injection(n2.ip_addr, COORDINATOR_BEFORE_ACCEPT, one_shot=True)
        mark = await n1_log.mark()
        cas1 = cql.run_async(cas(ks, 1), host=h2)
        await manager.api.wait_for_injection_enter(n2.ip_addr, COORDINATOR_BEFORE_ACCEPT)
        # The round proceeds on a quorum of promises; n1's may still be in flight.
        await n1_log.wait_for("Promising ballot", from_mark=mark)

        logger.info("Second round on n3, promised a higher ballot by n1 and n3, held the same way")
        await manager.api.enable_injection(n2.ip_addr, "paxos_error_before_save_promise", one_shot=False)
        await manager.api.enable_injection(n3.ip_addr, COORDINATOR_BEFORE_ACCEPT, one_shot=True)
        mark = await n1_log.mark()
        cas2 = cql.run_async(cas(ks, 2), host=h3)
        await manager.api.wait_for_injection_enter(n3.ip_addr, COORDINATOR_BEFORE_ACCEPT)
        await n1_log.wait_for("Promising ballot", from_mark=mark)
        await manager.api.disable_injection(n2.ip_addr, "paxos_error_before_save_promise")

        logger.info("Crash n1")
        await crash(manager, cql, n1)

        logger.info("Release the first round, then the second")
        await manager.api.message_injection(n2.ip_addr, COORDINATOR_BEFORE_ACCEPT)
        try:
            [row] = await cas1
            cas1_outcome = "applied" if row.applied else f"not applied, saw c={getattr(row, 'c', None)}"
        except WriteTimeout as e:
            cas1_outcome = f"indeterminate ({e})"
        await manager.api.message_injection(n3.ip_addr, COORDINATOR_BEFORE_ACCEPT)
        [row] = await cas2
        cas2_outcome = "applied" if row.applied else f"not applied, saw c={getattr(row, 'c', None)}"
        [row] = await cql.run_async(serial_read(ks), host=h2)
        final = row.c
        logger.info("First CAS: %s; second CAS: %s; final c=%s", cas1_outcome, cas2_outcome, final)

        assert not (cas1_outcome == "applied" and cas2_outcome == "applied"), (
            "both CAS conditioned on the initial value were applied: n1 accepted the first "
            "proposal although it had promised the second one's higher ballot")
        assert cas2_outcome == "applied" and final == 2, (
            f"expected the second CAS to apply on top of a rejected first one, got: "
            f"first {cas1_outcome}; second {cas2_outcome}; final c={final}")

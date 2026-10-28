#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What a coordinator carries from one paxos round into the next must not weaken the guarantees.

Idea: a CAS keeps one storage_proxy::paxos_response_handler across every
round it runs: the rounds it finishes for other coordinators, its own
retries after a rejection, and finally the round that applies the update.
The handler carries state across these rounds, and some of it belongs to
one round only: the count of replicas that learned a decision, which
decides when the paxos state is pruned; the consistency level of the
learn, lowered to ANY for the empty decision of an unmet condition; the
ballot a replica said it had promised, which the next ballot must exceed;
and the schema version under which the replicas return another
coordinator's accepted proposal.  Each test builds one interleaving with
error injections and checks what the clients see.

Setup, for all tests: three nodes, RF=3, one tablet; none of this depends
on where the paxos state table lives.  Rounds are held between their
prepare and their accept with the paxos_coordinator_before_accept
injection; a replica is kept out of a prepare quorum by making it fail to
save its promise.
"""

import asyncio
import logging
import re
import time

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, WriteFailure, WriteTimeout
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

CONFIG = {"rf_rack_valid_keyspaces": False}
CMDLINE = ["--logger-log-level", "paxos=trace"]
KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
           "AND tablets = {'initial': 1}")

BEFORE_ACCEPT = "paxos_coordinator_before_accept"
BEFORE_LEARN = "paxos_error_before_learn"
BEFORE_PROMISE = "paxos_error_before_save_promise"


def cas(query: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


def serial_read(ks: str, columns: str = "c") -> SimpleStatement:
    return SimpleStatement(f"SELECT {columns} FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.SERIAL)


async def inject(manager: ScyllaClusterManager, servers, name: str, enable: bool, one_shot: bool = False) -> None:
    for s in servers:
        if enable:
            await manager.api.enable_injection(s.ip_addr, name, one_shot=one_shot)
        else:
            await manager.api.disable_injection(s.ip_addr, name)


async def outcome(coro) -> str:
    """What the client of a CAS was told."""
    try:
        [row] = await coro
    except (WriteFailure, WriteTimeout, OperationTimedOut) as e:
        return f"failed with {type(e).__name__}"
    return "applied" if row.applied else f"not applied, saw c={getattr(row, 'c', None)}"


async def create_table(cql, ks: str, columns: str = "c int") -> None:
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, {columns})")
    await cql.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 0) IF NOT EXISTS")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5776", strict=True)
async def test_decision_is_pruned_only_once_a_quorum_learned_it(manager: ScyllaClusterManager, scale_timeout):
    """A decision learned by one replica must not be pruned on the strength of another decision's learns.

    The handler counts the learns of every decision it commits in one
    counter and prunes the paxos state once that count reaches a quorum
    (paxos_response_handler::learned).  A CAS that first finishes another
    coordinator's round and then commits its own thus prunes its own
    decision after a single learn, if the other decision was learned by one
    replica: the commit then survives, data included, on that replica
    only, while the others keep the accepted proposal.  The next rounds
    through that replica repair the others with the pruned, empty commit,
    which erases their proposal, and the update is gone from every replica
    but one.

    Setup: round A (n1, c=0 -> 1) is accepted by all and learned by none.
    Round B (n1, CL=ONE, d=2 if c=1) finishes A, whose decision only n1
    learns; n3 refuses promises, so that every prepare rests on {n1, n2},
    and n2 is let through once for the repair with A's decision.  B is
    then held before its own accept, n2 and n3 fail its own learn, and B
    is acknowledged.  A SERIAL read of c through {n1, n2} repairs n2 with
    n1's commit, and its own, empty decision erases the proposal on n3; it
    reads c, which n1 and n2 agree on, so no read repair carries d along.
    With n1 down, a SERIAL read of d and a CAS conditioned on d being unset
    go through {n2, n3}: they must see d=2 and not apply.
    """
    # Writes to the paxos state table wait for the commitlog to be synced; keep that short.
    servers = await manager.servers_add(3, config={**CONFIG, "commitlog_sync_period_in_ms": 100}, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await create_table(cql, ks, "c int, d int")
        n1_log = await manager.server_open_log(n1.server_id)

        logger.info("Round A: accepted by all, learned by none")
        await inject(manager, servers, BEFORE_LEARN, True)
        first = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h1))
        assert first.startswith("failed"), f"scenario not reached: round A {first}"

        logger.info("Round B at CL=ONE: finishes A on n1 only, then its own update, learned by n1 only")
        await manager.api.disable_injection(n1.ip_addr, BEFORE_LEARN)
        await manager.api.enable_injection(n3.ip_addr, BEFORE_PROMISE, one_shot=False)
        await manager.api.enable_injection(n1.ip_addr, BEFORE_ACCEPT, one_shot=True)
        mark = await n1_log.mark()
        round_b = cql.run_async(cas(f"UPDATE {ks}.t SET d = 2 WHERE pk = 1 IF c = 1", cl=ConsistencyLevel.ONE), host=h1)
        # A's decision reached n1 only; the repair of n2 with it fails until n2 is let through.
        await n1_log.wait_for("Failure during commit repair", from_mark=mark, timeout=scale_timeout(60))
        await manager.api.disable_injection(n2.ip_addr, BEFORE_LEARN)
        await manager.api.wait_for_injection_enter(n1.ip_addr, BEFORE_ACCEPT)
        await manager.api.enable_injection(n2.ip_addr, BEFORE_LEARN, one_shot=False)
        await manager.api.disable_injection(n3.ip_addr, BEFORE_PROMISE)
        await manager.api.message_injection(n1.ip_addr, BEFORE_ACCEPT)
        second = await outcome(round_b)
        await inject(manager, (n2, n3), BEFORE_LEARN, False)
        assert second == "applied", f"scenario not reached: round B {second}"
        # The decision of B's own update is the last one n1 committed; its prune is a "Delete paxos
        # state" for its ballot.  The deletion is visible once the commitlog is synced: give it a second.
        own = re.findall(r"committing proposal\(([0-9a-f-]+)\) with cl=ONE", n1_log.file.read_text()[mark:])[-1]
        try:
            await n1_log.wait_for(f"Delete paxos state for ballot {own}", from_mark=mark, timeout=scale_timeout(10))
            pruned = True
        except TimeoutError:
            pruned = False
        logger.info("B's own decision %s, pruned on n1 after its single learn: %s", own, pruned)
        await asyncio.sleep(scale_timeout(1))

        logger.info("A round through {n1, n2} repairs n2 with n1's commit; its own decision erases n3's proposal")
        await manager.api.enable_injection(n3.ip_addr, BEFORE_PROMISE, one_shot=False)
        await cql.run_async(serial_read(ks), host=h1)
        await manager.api.disable_injection(n3.ip_addr, BEFORE_PROMISE)

        logger.info("With n1 down, read d and update it through {n2, n3}")
        await manager.server_stop_gracefully(n1.server_id)
        [row] = await cql.run_async(serial_read(ks, "d"), host=h2)
        seen = row.d
        third = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET d = 3 WHERE pk = 1 IF d = null"), host=h2))
        await manager.server_start(n1.server_id)
        await wait_for_cql_and_get_hosts(cql, [n1], time.time() + scale_timeout(60))
        [row] = await cql.run_async(serial_read(ks, "d"), host=h1)
        final = row.d

        assert seen == 2 and third != "applied" and final == 2, (
            f"the update d=2, chosen by every replica and acknowledged, was lost: its decision was pruned on n1 "
            f"after n1 alone learned it ({pruned}); a SERIAL read through n2 and n3 then returned d={seen}, "
            f"a CAS conditioned on d being unset {third}, and the final SERIAL read returned d={final}")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5777", strict=True)
async def test_learn_consistency_survives_an_unmet_condition(manager: ScyllaClusterManager, scale_timeout):
    """A CAS whose condition was unmet on a first try is still learned at its own consistency level.

    An unmet condition makes the coordinator commit an empty decision, and
    it lowers the learn of that decision to ANY on the handler
    (storage_proxy::cas).  If the empty proposal is rejected and the retry
    finds the condition met, the client's update is learned at ANY too:
    the client is told it applied once a single replica learned it.

    Setup: X (n2, CL=QUORUM, c=1 -> 2) is held before its accept while its
    condition is unmet.  Y (n3, c=0 -> 1) applies meanwhile and takes the
    ballot from X.  n1 and n3 then fail every learn and X is released: its
    retry finds c=1, proposes c=2, and the learn succeeds on n2 only.  At
    QUORUM that is a failure, not a success.  With n2 down, a QUORUM read
    must then not contradict what X was told.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await create_table(cql, ks)
        n2_log = await manager.server_open_log(n2.server_id)

        logger.info("X on n2: condition unmet, held before the accept of its empty proposal")
        await manager.api.enable_injection(n2.ip_addr, BEFORE_ACCEPT, one_shot=True)
        mark = await n2_log.mark()
        x = cql.run_async(cas(f"UPDATE {ks}.t SET c = 2 WHERE pk = 1 IF c = 1"), host=h2)
        await manager.api.wait_for_injection_enter(n2.ip_addr, BEFORE_ACCEPT)
        await n2_log.wait_for("precondition does not match current values", from_mark=mark, timeout=scale_timeout(60))

        logger.info("Y on n3 applies c=1 and takes the ballot from X")
        y = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h3))
        assert y == "applied", f"scenario not reached: Y {y}"

        logger.info("Release X: its retry proposes c=2, learned by n2 only")
        await inject(manager, (n1, n3), BEFORE_LEARN, True)
        await manager.api.message_injection(n2.ip_addr, BEFORE_ACCEPT)
        told = await outcome(x)
        await inject(manager, (n1, n3), BEFORE_LEARN, False)
        levels = re.findall(r"committing proposal\([0-9a-f-]+\) with cl=(\w+)", n2_log.file.read_text()[mark:])
        logger.info("X was told it %s; n2 committed its decisions with cl=%s", told, levels)

        await manager.server_stop_gracefully(n2.server_id)
        quorum_read = SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.QUORUM)
        [row] = await cql.run_async(quorum_read, host=h1)
        assert told != "applied" or row.c == 2, (
            f"X asked for CL=QUORUM and was told it applied, but its update was learned at cl={levels[-1]}, "
            f"by n2 alone: with n2 down a QUORUM read returns c={row.c}")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5778", strict=True)
async def test_in_progress_proposal_keeps_its_columns_across_a_schema_change(manager: ScyllaClusterManager, scale_timeout):
    """A proposal accepted under a newer schema must not lose columns when an older coordinator finishes it.

    The replicas convert the accepted proposal they return with a promise to
    the schema version the coordinator sent with its prepare
    (paxos_state::prepare), and a column that version lacks is dropped
    (converting_mutation_partition_applier).  A coordinator still on the
    schema from before an ALTER TABLE ... ADD finishes another
    coordinator's round without the added column: the update is applied
    in part.

    Setup: X (n2, c=1 -> 2) is held before its accept while its condition
    is unmet, so that it keeps the schema from before the ALTER.  Column x
    is added; Y (n3, c=0 -> 1 and x=5) is accepted by all and learned by
    none.  X is released: its retry finishes Y's round and applies on top
    of it.  A SERIAL read must then see x=5 next to c=2.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await create_table(cql, ks)
        n2_log = await manager.server_open_log(n2.server_id)

        logger.info("X on n2: condition unmet, held before the accept of its empty proposal")
        await manager.api.enable_injection(n2.ip_addr, BEFORE_ACCEPT, one_shot=True)
        mark = await n2_log.mark()
        x = cql.run_async(cas(f"UPDATE {ks}.t SET c = 2 WHERE pk = 1 IF c = 1"), host=h2)
        await manager.api.wait_for_injection_enter(n2.ip_addr, BEFORE_ACCEPT)
        await n2_log.wait_for("precondition does not match current values", from_mark=mark, timeout=scale_timeout(60))

        logger.info("ALTER TABLE adds x; Y on n3 sets c=1, x=5: accepted by all, learned by none")
        await cql.run_async(f"ALTER TABLE {ks}.t ADD x int")
        await inject(manager, servers, BEFORE_LEARN, True)
        y = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1, x = 5 WHERE pk = 1 IF c = 0"), host=h3))
        await inject(manager, servers, BEFORE_LEARN, False)
        assert y.startswith("failed"), f"scenario not reached: Y {y}"

        logger.info("Release X: its retry finishes Y's round under the old schema, then applies c=2")
        await manager.api.message_injection(n2.ip_addr, BEFORE_ACCEPT)
        told = await outcome(x)
        assert told == "applied", f"scenario not reached: X {told}"

        [row] = await cql.run_async(serial_read(ks, "c, x"), host=h1)
        assert (row.c, row.x) == (2, 5), (
            f"Y's update, chosen as c=1, x=5, was finished by X without the column X's schema lacked: "
            f"a SERIAL read returns c={row.c}, x={row.x}")


@pytest.mark.skip_mode(mode="release", reason="test_clocks_offset_seconds exists only with error injections")
@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5779", strict=True)
async def test_ballot_rises_above_a_rejection(manager: ScyllaClusterManager, scale_timeout):
    """A coordinator whose clock lags gets its ballot promised on the retry after a rejection.

    A replica that rejects a prepare answers with the ballot it promised,
    so that the coordinator can pick a higher one next time.  The
    coordinator records that ballot only after a promised prepare
    (storage_proxy::paxos_response_handler::begin_and_repair_paxos), so
    after a rejection it keeps proposing ballots from its own clock.  A
    key last touched through a coordinator whose clock is ahead is then
    out of its reach until its own clock gets there: a CAS times out.

    Setup: n3's clocks are moved ten minutes ahead, through the
    test-only test_clocks_offset_seconds option, and n3 runs a CAS on
    the key; n2 then runs one on it, and it must apply.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await create_table(cql, ks)
        n2_log = await manager.server_open_log(n2.server_id)

        logger.info("n3, its clock ten minutes ahead, sets c=1")
        offset = "UPDATE system.config SET value = '{}' WHERE name = 'test_clocks_offset_seconds'"
        await cql.run_async(offset.format(600), host=h3)
        try:
            first = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h3))
            assert first == "applied", f"scenario not reached: the CAS on n3 {first}"

            logger.info("n2, its clock on time, sets c=2")
            mark = await n2_log.mark()
            started = time.time()
            second = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 2 WHERE pk = 1 IF c = 1"), host=h2))
            took = time.time() - started
        finally:
            await cql.run_async(offset.format(0), host=h3)
        rejections = len(re.findall("already promised a higher ballot", n2_log.file.read_text()[mark:]))
        logger.info("The CAS on n2 %s after %.1f s and %d rejections", second, took, rejections)
        assert second == "applied" and took < scale_timeout(10), (
            f"the CAS on n2 {second} after {took:.1f} s: its prepare was rejected {rejections} times, "
            f"every retry proposing a ballot from n2's clock, below the one n3 had promised")

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The round of an empty update ends at its accept.

Idea: a SERIAL read and a CAS whose condition is unmet complete their
round with an empty update.  A quorum accepting it is all the round is
for: every later round then finds it as the newest accepted proposal,
so an older proposal left on a minority is never replayed.  The
coordinator sends no learn for it, and a later round does not replay it.

Setup: three nodes, RF=3, one tablet.  A replica is kept out of a round
with the paxos_error_before_save_promise and
paxos_error_before_save_proposal injections.
"""

import logging
import re

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, WriteFailure, WriteTimeout
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {"rf_rack_valid_keyspaces": False}
CMDLINE = ["--logger-log-level", "paxos=trace"]
KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
           "AND tablets = {'initial': 1}")

BEFORE_PROMISE = "paxos_error_before_save_promise"
BEFORE_PROPOSAL = "paxos_error_before_save_proposal"

COMMITTED = r"Committing decision proposal\(([0-9a-f-]+)\)"
ACCEPT = r"Accepting proposal proposal\(([0-9a-f-]+)\)"
REPLAYED = "Finishing incomplete paxos round"


def cas(query: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


def serial_read(ks: str) -> SimpleStatement:
    return SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.SERIAL)


async def inject(manager: ScyllaClusterManager, servers, names, enable: bool) -> None:
    for s in servers:
        for name in names:
            if enable:
                await manager.api.enable_injection(s.ip_addr, name, one_shot=False)
            else:
                await manager.api.disable_injection(s.ip_addr, name)


async def outcome(coro) -> str:
    """What the client of a CAS was told."""
    try:
        [row] = await coro
    except (WriteFailure, WriteTimeout, OperationTimedOut) as e:
        return f"failed with {type(e).__name__}"
    return "applied" if row.applied else f"not applied, saw c={getattr(row, 'c', None)}"


async def create_table(cql, ks: str, logs, scale_timeout) -> str:
    """Create the table with its row and return the ballot of the insert."""
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c int)")
    await cql.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 0) IF NOT EXISTS")
    # The insert is acknowledged by a quorum; the tests read the logs from after its last learn.
    for log in logs:
        await log.wait_for("Committing decision", timeout=scale_timeout(60))
    return re.findall(COMMITTED, logs[0].file.read_text())[-1]


async def test_empty_round_is_neither_learned_nor_replayed(manager: ScyllaClusterManager, scale_timeout):
    """No replica learns an empty decision, and the round after it has nothing to finish.

    A CAS whose condition is unmet and a SERIAL read each complete a round
    with an empty update.  A CAS that applies then runs on the same key.
    The only decision any replica commits is that of the last CAS, and no
    coordinator finishes a round on behalf of another.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)
    logs = [await manager.server_open_log(s.server_id) for s in servers]

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        insert_ballot = await create_table(cql, ks, logs, scale_timeout)
        marks = [await log.mark() for log in logs]

        unmet = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 2 WHERE pk = 1 IF c = 1"), host=h2))
        assert unmet == "not applied, saw c=0", f"scenario not reached: the CAS with an unmet condition {unmet}"
        [row] = await cql.run_async(serial_read(ks), host=h1)
        assert row.c == 0

        applied = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h3))
        assert applied == "applied", f"the CAS that follows the empty rounds {applied}"
        ballot = re.findall(r"proposing client-requested updates for ([0-9a-f-]+)", logs[2].file.read_text()[marks[2]:])[-1]
        for log, mark in zip(logs, marks):
            await log.wait_for(rf"Committing decision proposal\({ballot}\)", from_mark=mark, timeout=scale_timeout(60))

        for server, log, mark in zip(servers, logs, marks):
            text = log.file.read_text()[mark:]
            # A replica logs the insert's decision before it saves it, and a prepare that gets
            # there in between makes the coordinator send that decision to the replica again.
            committed = set(re.findall(COMMITTED, text)) - {insert_ballot}
            assert committed == {ballot}, (
                f"{server.ip_addr} committed {committed}: the only decision to learn is {ballot}, "
                f"that of the CAS that applied")
            assert REPLAYED not in text, f"{server.ip_addr} finished the round of an empty update"


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_empty_round_hides_an_older_proposal(manager: ScyllaClusterManager, scale_timeout):
    """A proposal accepted by a minority stays unseen once a quorum accepted an empty update above it.

    V (n1, c=0 -> 1) is accepted by n1 alone and fails.  A SERIAL read on
    n2 then runs without n1: it returns c=0 and n2 and n3 accept its empty
    update.  A second SERIAL read runs without n3.  Its prepare gets V from
    n1 and the empty update from n2; the empty update is the newer, so the
    read replays nothing and returns c=0 again, as the first one did.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    n1_log, n2_log, n3_log = logs

    def last_accept(log, mark) -> str:
        """The ballot of the last proposal the replica got to accept; the injection fails the accept after it is logged."""
        return re.findall(ACCEPT, log.file.read_text()[mark:])[-1]

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await create_table(cql, ks, logs, scale_timeout)
        mark1, mark2, mark3 = [await log.mark() for log in logs]

        logger.info("V on n1: accepted by n1 alone")
        await inject(manager, (n2, n3), (BEFORE_PROPOSAL,), True)
        v = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h1))
        await inject(manager, (n2, n3), (BEFORE_PROPOSAL,), False)
        assert v.startswith("failed"), f"scenario not reached: V {v}"
        # V fails as soon as n2 and n3 have failed its accept, which can be before n1 gets to its own.
        await n1_log.wait_for(ACCEPT, from_mark=mark1, timeout=scale_timeout(60))
        v_ballot = last_accept(n1_log, mark1)

        logger.info("A SERIAL read on n2, without n1: n2 and n3 accept its empty update")
        await inject(manager, (n1,), (BEFORE_PROMISE, BEFORE_PROPOSAL), True)
        [row] = await cql.run_async(serial_read(ks), host=h2)
        await inject(manager, (n1,), (BEFORE_PROMISE, BEFORE_PROPOSAL), False)
        assert row.c == 0, f"scenario not reached: the first read returned c={row.c}"
        empty_ballot = last_accept(n2_log, mark2)
        assert empty_ballot != v_ballot and last_accept(n3_log, mark3) == empty_ballot, (
            f"scenario not reached: after V ({v_ballot}) n2 accepted {empty_ballot} and n3 {last_accept(n3_log, mark3)}")

        logger.info("A SERIAL read on n1, without n3: its prepare gets V from n1 and the empty update from n2")
        await inject(manager, (n3,), (BEFORE_PROMISE, BEFORE_PROPOSAL), True)
        [row] = await cql.run_async(serial_read(ks), host=h1)
        await inject(manager, (n3,), (BEFORE_PROMISE, BEFORE_PROPOSAL), False)
        assert REPLAYED not in n1_log.file.read_text()[mark1:], "the second read finished the round of another coordinator"
        assert row.c == 0, (
            f"two SERIAL reads with no write between them returned c=0 and then c={row.c}: "
            f"the second one replayed V, which the empty update of the first one had superseded")

        applied = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 5 WHERE pk = 1 IF c = 0"), host=h3))
        assert applied == "applied", f"the CAS that follows the reads {applied}"

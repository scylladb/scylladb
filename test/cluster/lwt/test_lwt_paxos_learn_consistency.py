#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The consistency level of a CAS learn must not be weakened by an earlier round of the same CAS.

Idea: a CAS keeps one storage_proxy::paxos_response_handler across every
round it runs: the rounds it finishes for other coordinators, its own
retries after a rejection, and finally the round that applies the update.
The consistency level of the learn is lowered to ANY for the empty
decision of an unmet condition, and it must not stay lowered for the
rounds that follow.  The test builds that interleaving with error
injections and checks what the client sees.

Setup: three nodes, RF=3, one tablet; none of this depends on where the
paxos state table lives.  The round is held between its prepare and its
accept with the paxos_coordinator_before_accept injection.
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

BEFORE_ACCEPT = "paxos_coordinator_before_accept"
BEFORE_LEARN = "paxos_error_before_learn"


def cas(query: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


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
async def test_learn_consistency_survives_an_unmet_condition(manager: ScyllaClusterManager, scale_timeout):
    """A CAS whose condition was unmet on a first try is still learned at its own consistency level.

    An unmet condition makes the coordinator commit an empty decision,
    learned at ANY (storage_proxy::cas).  That level is for the empty
    decision alone: if the empty proposal is rejected and the retry finds
    the condition met, the client's update must be learned at the client's
    level, not acknowledged once a single replica learned it.

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
        n1_log, n3_log = [await manager.server_open_log(s.server_id) for s in (n1, n3)]
        mark1, mark3 = await n1_log.mark(), await n3_log.mark()
        y = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h3))
        assert y == "applied", f"scenario not reached: Y {y}"
        # Y needs promises from a quorum only; until n2's is in, n2 would still accept X's older proposal.
        y_ballot = re.findall(r"proposing client-requested updates for ([0-9a-f-]+)", n3_log.file.read_text()[mark3:])[-1]
        await n2_log.wait_for(f"Promising ballot {y_ballot}", from_mark=mark, timeout=scale_timeout(60))
        # Y's learn too is acknowledged by a quorum only: one still in flight when X is released would hit
        # the injection on n1 or n3, and X's retry would then repair that replica over and over until it timed out.
        for log, from_mark in ((n1_log, mark1), (n2_log, mark), (n3_log, mark3)):
            await log.wait_for(rf"Committing decision proposal\({y_ballot}\)", from_mark=from_mark, timeout=scale_timeout(60))

        logger.info("Release X: its retry proposes c=2, learned by n2 only")
        await inject(manager, (n1, n3), BEFORE_LEARN, True)
        await manager.api.message_injection(n2.ip_addr, BEFORE_ACCEPT)
        told = await outcome(x)
        levels = re.findall(r"committing proposal\([0-9a-f-]+\) with cl=(\w+)", n2_log.file.read_text()[mark:])
        logger.info("X was told it %s; n2 committed its decisions with cl=%s", told, levels)
        # The retry did propose the client's update and learned it at the client's level; with two
        # replicas failing the learn that is a write failure, not a timeout elsewhere in the round.
        assert levels and set(levels) == {"QUORUM"} and told == "failed with WriteFailure", (
            f"X was told it {told} and n2 committed its decisions with cl={levels}: the retry must learn "
            f"the client's update at QUORUM and fail when two replicas fail the learn")

        await manager.server_stop_gracefully(n2.server_id)
        quorum_read = SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.QUORUM)
        [row] = await cql.run_async(quorum_read, host=h1)
        # Only now: a learn still in flight when X answered would otherwise land on n1 or n3.
        await inject(manager, (n1, n3), BEFORE_LEARN, False)
        assert told != "applied" or row.c == 2, (
            f"X asked for CL=QUORUM and was told it applied, but its update was learned at cl={levels[-1]}, "
            f"by n2 alone: with n2 down a QUORUM read returns c={row.c}")

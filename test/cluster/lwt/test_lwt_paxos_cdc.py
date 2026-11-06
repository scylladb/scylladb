#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What the CDC log holds for an LWT update whose paxos round another coordinator finished.

Idea: a coordinator writes the CDC log entry of a decision next to the
learn of the decision itself, as a plain write that succeeds or fails on
its own (storage_proxy::paxos_response_handler::learn_decision).  A
decision accepted by every replica whose learn then fails everywhere
leaves its log entry behind while the client is told the write failed;
the next coordinator to touch the key finishes the round and learns the
same decision again, running the CDC augmentation anew: a fresh random
cdc$time for the same timestamp (cdc::generate_timeuuid), and a preimage
read at that moment.  The test shows what the log contains after such a
round.

Setup: three nodes, RF=3, one tablet; a table with cdc = {'enabled':
true, 'preimage': true}, and a row created by an INSERT ... IF NOT
EXISTS.
"""

import logging

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

BEFORE_LEARN = "paxos_error_before_learn"

# cdc$operation codes, from cdc::operation
PRE_IMAGE, UPDATE, INSERT = 0, 1, 2
OPERATION = {PRE_IMAGE: "preimage", UPDATE: "UPDATE", INSERT: "INSERT"}


def cas(query: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


def serial_read(ks: str) -> SimpleStatement:
    return SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.SERIAL)


async def inject(manager: ScyllaClusterManager, servers, name: str, enable: bool) -> None:
    for s in servers:
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


@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5813", strict=True)
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_update_is_logged_once_when_another_coordinator_finishes_its_round(manager: ScyllaClusterManager):
    """An LWT update is in the CDC log once, however many coordinators learned its decision.

    The update c=0 -> 1 through n1 is accepted by every replica and learned
    by none, so n1 reports a failure; a SERIAL read through n2 then
    finishes the round and must return c=1.  The log is read at ALL and
    every row of pk=1 is logged with its cdc$time, the time field of that
    uuid, its operation and its c.  The assertion checks that the log
    holds exactly one UPDATE row and one preimage row for the update.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c int) "
                            "WITH cdc = {'enabled': true, 'preimage': true}")
        await cql.run_async(cas(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 0) IF NOT EXISTS"), host=h1)

        logger.info("The update through n1: accepted by all, learned by none")
        await inject(manager, servers, BEFORE_LEARN, True)
        told = await outcome(cql.run_async(cas(f"UPDATE {ks}.t SET c = 1 WHERE pk = 1 IF c = 0"), host=h1))
        await inject(manager, servers, BEFORE_LEARN, False)
        assert told.startswith("failed"), f"scenario not reached: the update {told}"

        logger.info("The update %s; a SERIAL read through n2 finishes the round", told)
        [row] = await cql.run_async(serial_read(ks), host=h2)
        assert row.c == 1, f"scenario not reached: the SERIAL read returned c={row.c}"

        log = SimpleStatement(f'SELECT "cdc$time" AS tuuid, "cdc$operation" AS op, "cdc$batch_seq_no" AS seq, pk, c '
                              f"FROM {ks}.t_scylla_cdc_log", consistency_level=ConsistencyLevel.ALL)
        rows = [r for r in await cql.run_async(log) if r.pk == 1]
        for r in sorted(rows, key=lambda r: (r.tuuid.time, r.tuuid.int, r.seq)):
            logger.info("cdc$time=%s (time field %d) %s seq=%d c=%s", r.tuuid, r.tuuid.time, OPERATION.get(r.op, r.op), r.seq, r.c)
        updates = sorted((r.tuuid for r in rows if r.op == UPDATE), key=lambda u: (u.time, u.int))
        preimages = [r.c for r in rows if r.op == PRE_IMAGE and r.tuuid in updates]
        assert len(updates) == 1 and len(preimages) == 1, (
            f"the update c=0 -> 1 is in the CDC log {len(updates)} times, under cdc$time {', '.join(map(str, updates))} "
            f"(time fields {[u.time for u in updates]}), with preimages c={preimages}: n1 wrote its log entry although "
            f"its learn of the decision failed, and n2 wrote it again when it finished the round")

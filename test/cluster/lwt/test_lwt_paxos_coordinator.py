#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The ballot a coordinator proposes after a rejected prepare.

Idea: a replica that rejects a prepare answers with the ballot it
promised.  The coordinator must propose a higher one on the retry.
A coordinator whose clock is behind cannot do that from its own
clock: every ballot it makes is below the promised one, and the CAS
times out.

Setup: three nodes, RF=3, one tablet.  One node's clock is moved
ahead with the test-only test_clocks_offset_seconds option.
"""

import logging
import re
import time

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


def cas(query: str, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> SimpleStatement:
    return SimpleStatement(query, consistency_level=cl, serial_consistency_level=ConsistencyLevel.SERIAL)


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


@pytest.mark.skip_mode(mode="release", reason="test_clocks_offset_seconds exists only with error injections")
async def test_ballot_rises_above_a_rejection(manager: ScyllaClusterManager, scale_timeout):
    """A coordinator whose clock is behind gets its ballot promised on the retry.

    n3's clock is moved ten minutes ahead and n3 runs a CAS on the key.
    Every replica now holds a promise ten minutes in the future.  n2,
    whose clock is on time, then runs a CAS on the same key.  It must
    apply within seconds: the first prepare is rejected, and the retry
    must propose a ballot above the one the replica reported.
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
        assert rejections >= 1, f"scenario not reached: n2's prepare was never rejected; the CAS on n2 {second}"
        assert second == "applied" and took < scale_timeout(10), (
            f"the CAS on n2 {second} after {took:.1f} s: its prepare was rejected {rejections} times, "
            f"every retry proposing a ballot from n2's clock, below the one n3 had promised")

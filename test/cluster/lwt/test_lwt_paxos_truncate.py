#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""An unfinished paxos round must not bring back what TRUNCATE removed.

Idea: a replica learns a decision only if its ballot is newer than the table's
last truncation (paxos_state::learn), so that a decision delayed from before a
TRUNCATE cannot resurrect the data.  A proposal that was accepted but never
learned is finished by the next round on its key, which re-proposes it under
its own, newer ballot (storage_proxy::paxos_response_handler::begin_and_repair_paxos);
the guard then lets it through, although the data it carries, cell timestamps
included, predates the truncation.

Setup: three nodes, RF=3, tablets or vnodes.  The first CAS is accepted by
every replica and learned by none, since every learn fails, so its client gets
an error; the table is truncated; a SERIAL read of the key then finishes the
round.
"""

import logging

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {"rf_rack_valid_keyspaces": False}
CMDLINE = ["--logger-log-level", "paxos=trace"]


def keyspace_opts(tablets: bool) -> str:
    tablets_opts = "{'initial': 1}" if tablets else "{'enabled': false}"
    return ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
            f"AND tablets = {tablets_opts}")


@pytest.mark.parametrize("tablets", [True, False], ids=["tablets", "vnodes"])
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.xfail(reason="https://scylladb.atlassian.net/browse/SCYLLADB-5758", strict=True)
async def test_unfinished_round_after_truncate(manager: ScyllaClusterManager, tablets: bool):
    """A row from a round that was accepted before a TRUNCATE must not reappear after it."""
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, keyspace_opts(tablets)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c int)")
        # With tablets the first LWT on a table creates its paxos state table.
        await cql.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (0, 0) IF NOT EXISTS")

        logger.info("First CAS: accepted by every replica, learned by none")
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "paxos_error_before_learn", one_shot=False)
        insert = SimpleStatement(f"INSERT INTO {ks}.t (pk, c) VALUES (1, 1) IF NOT EXISTS",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 serial_consistency_level=ConsistencyLevel.SERIAL)
        try:
            [row] = await cql.run_async(insert, host=h2)
            first = "applied" if row.applied else f"not applied, saw c={getattr(row, 'c', None)}"
        except Exception as e:
            first = f"failed with {type(e).__name__}"
        logger.info("First CAS: %s", first)
        for s in servers:
            await manager.api.disable_injection(s.ip_addr, "paxos_error_before_learn")

        logger.info("TRUNCATE, then finish the round with a SERIAL read")
        await cql.run_async(f"TRUNCATE {ks}.t")
        quorum_read = SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.QUORUM)
        assert not await cql.run_async(quorum_read, host=h3), "scenario not reached: the row is there right after TRUNCATE"
        serial_read = SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 1", consistency_level=ConsistencyLevel.SERIAL)
        rows = await cql.run_async(serial_read, host=h3)
        after = await cql.run_async(quorum_read, host=h1)
        assert not rows and not after, (
            f"the round the first CAS left behind ({first}) brought its row back after TRUNCATE: "
            f"SERIAL read {[r.c for r in rows]}, QUORUM read afterwards {[r.c for r in after]}")

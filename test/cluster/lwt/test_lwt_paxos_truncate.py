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
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.lwt.lwt_common import get_token_for_pk
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts

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


def insert(ks: str, pk: int) -> SimpleStatement:
    return SimpleStatement(f"INSERT INTO {ks}.t (pk, c) VALUES ({pk}, 1) IF NOT EXISTS",
                           consistency_level=ConsistencyLevel.QUORUM,
                           serial_consistency_level=ConsistencyLevel.SERIAL)


def read(ks: str, pk: int, cl: ConsistencyLevel) -> SimpleStatement:
    return SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = {pk}", consistency_level=cl)


@pytest.mark.parametrize("tablets", [True, False], ids=["tablets", "vnodes"])
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_truncated_value_returns_through_a_new_replica(manager: ScyllaClusterManager, scale_timeout, tablets: bool):
    """A value learned and then truncated must not come back through a replica that joined after the TRUNCATE.

    Truncation times are node-local: a node that joins after the TRUNCATE has
    none for the table, so the guard in paxos_state::learn holds nothing back
    on it.  A round on a key sends the most recent commit, under its original
    ballot, to every replica that lacks it, and a replica that joined after
    the fact lacks it: with vnodes system.paxos is not streamed to a new node,
    with tablets the key's tablet is migrated from the replica that never
    learned the decision.  Setup: the decision is learned by n1 and n2 only
    (n3 fails to learn), the table is truncated, n4 is added and made a
    replica of the key next to n1 and n2.  A prepare stops at a quorum of
    answers, so n2 is stopped while a SERIAL read of the key runs through n4:
    the answers are n1's, with the commit, and n4's, without it.

    What keeps the row away today is the pruning of paxos state: once a
    decision is learned by a quorum its data is deleted from the state and
    only its ballot stays, so the commit n4 receives is empty.
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, (h1, h2, h3) = await manager.get_ready_cql(servers)
    if tablets:
        await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, keyspace_opts(tablets)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c int)")
        # With vnodes the new node replicates only some keys: write enough to find one.
        keys = [1] if tablets else list(range(1, 33))

        logger.info("Learned by n1 and n2 only")
        await manager.api.enable_injection(n3.ip_addr, "paxos_error_before_learn", one_shot=False)
        for pk in keys:
            [row] = await cql.run_async(insert(ks, pk), host=h1)
            assert row.applied, "scenario not reached: the insert was not applied"
        await manager.api.disable_injection(n3.ip_addr, "paxos_error_before_learn")
        token = await get_token_for_pk(cql, ks, "t", keys[0])

        logger.info("TRUNCATE, then add n4")
        await cql.run_async(f"TRUNCATE {ks}.t")
        n4 = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
        [h4] = await wait_for_cql_and_get_hosts(cql, [n4], time.time() + scale_timeout(60))
        host_id = {s.server_id: await manager.get_host_id(s.server_id) for s in (n1, n2, n3, n4)}

        if tablets:
            pk = 1
            [shard] = [s for h, s in await get_tablet_replicas(manager, n1, ks, "t", token) if h == host_id[n3.server_id]]
            logger.info("Migrate the key's tablet from n3, which never learned, to n4")
            await manager.api.move_tablet(n1.ip_addr, ks, "t", host_id[n3.server_id], shard, host_id[n4.server_id], 0,
                                          token, timeout=scale_timeout(120))
        else:
            wanted = {n1.ip_addr, n2.ip_addr, n4.ip_addr}
            for pk in keys:
                if set(await manager.api.natural_endpoints(n1.ip_addr, ks, "t", str(pk))) == wanted:
                    break
            else:
                pytest.fail("scenario not reached: no key is replicated on exactly n1, n2 and n4")
            logger.info("Key %d is replicated on n1, n2 and n4", pk)

        assert not await cql.run_async(read(ks, pk, ConsistencyLevel.QUORUM), host=h1), (
            "scenario not reached: the row is there before any paxos round")
        # The SERIAL read repairs what the answering replicas lack; the data it
        # returns was prefetched before that, so read again afterwards, and
        # from every replica.
        await manager.server_stop_gracefully(n2.server_id)
        rows = await cql.run_async(read(ks, pk, ConsistencyLevel.SERIAL), host=h4)
        await manager.server_start(n2.server_id)
        await wait_for_cql_and_get_hosts(cql, [n2], time.time() + scale_timeout(60))
        again = await cql.run_async(read(ks, pk, ConsistencyLevel.SERIAL), host=h1)
        everywhere = await cql.run_async(read(ks, pk, ConsistencyLevel.ALL), host=h1)
        assert not rows and not again and not everywhere, (
            f"the truncated row of key {pk} came back through n4, which joined after the TRUNCATE: "
            f"SERIAL reads {[r.c for r in rows]} then {[r.c for r in again]}, "
            f"read at ALL {[r.c for r in everywhere]}")

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

import pytest
from cassandra import WriteFailure, WriteTimeout
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts


def keyspace_opts(rf: int, tablets: bool) -> str:
    tablets_opts = "{'initial': 1}" if tablets else "{'enabled': false}"
    return (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} "
            f"AND tablets = {tablets_opts}")


@pytest.mark.parametrize("tablets", [True, False], ids=["tablets", "vnodes"])
async def test_serial_read_in_reversed_order(manager: ScyllaClusterManager, tablets: bool):
    """A SERIAL read whose ORDER BY is the reverse of the clustering order.

    Reproduces SCYLLADB-6044: select_statement hands its reversed query schema
    to the paxos round, so the read's own decision is learned on the reversed
    schema, and the coordinator applies it with a schema that is not synced:
    database::apply() calls on_internal_error, which aborts the node under
    test.py's --abort-on-internal-error.
    """
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1, tablets)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        for ck in range(3):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (0, {ck}, {ck}) IF NOT EXISTS")

        rows = await cql.run_async(SimpleStatement(f"SELECT ck, v FROM {ks}.t WHERE pk = 0 ORDER BY ck DESC",
                                                   consistency_level=ConsistencyLevel.SERIAL))
        assert [(r.ck, r.v) for r in rows] == [(2, 2), (1, 1), (0, 0)]


@pytest.mark.parametrize("tablets", [True, False], ids=["tablets", "vnodes"])
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_serial_read_in_reversed_order_finishes_unfinished_round(manager: ScyllaClusterManager, tablets: bool):
    """A reversed SERIAL read that has to finish a write's paxos round.

    The write is accepted by every replica but learned by none, so the read
    finds it in progress and proposes it again. The paxos round runs on the
    table schema: replicas hand the accepted proposal over without converting
    it to the reversed schema, and the read returns the write in reverse order.
    """
    servers = await manager.servers_add(3, cmdline=["--logger-log-level", "paxos=debug"], auto_rack_dc="dc1")
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    async with new_test_keyspace(manager, keyspace_opts(3, tablets)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        for ck in range(3):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (0, {ck}, {ck}) IF NOT EXISTS")

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]

        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "paxos_error_before_learn", one_shot=False)
        writer = await manager.get_cql_exclusive(servers[0])
        with pytest.raises((WriteFailure, WriteTimeout)):
            await writer.run_async(f"UPDATE {ks}.t SET v = 10 WHERE pk = 0 AND ck = 1 IF v = 1")
        for s in servers:
            await manager.api.disable_injection(s.ip_addr, "paxos_error_before_learn")

        reader = await manager.get_cql_exclusive(servers[1])
        rows = await reader.run_async(SimpleStatement(f"SELECT ck, v FROM {ks}.t WHERE pk = 0 ORDER BY ck DESC",
                                                      consistency_level=ConsistencyLevel.SERIAL))
        assert [(r.ck, r.v) for r in rows] == [(2, 2), (1, 10), (0, 0)]

        replays = sum([len(await log.grep("Finishing incomplete paxos round", from_mark=mark)) for log, mark in zip(logs, marks)])
        assert replays > 0, "the read did not find the write in progress"
        conversions = sum([len(await log.grep("Stored mutation references outdated schema version", from_mark=mark))
                           for log, mark in zip(logs, marks)])
        assert conversions == 0, "a replica converted a proposal to another schema"

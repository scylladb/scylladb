# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import logging
import pathlib
import time

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_cluster import ScyllaVersionDescription
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# Key shapes hitting the components() serializer: full key, partial clustering prefix,
# partition-key-only, and the empty clustering key of a static-only row.
CREATE_TABLE = """
    CREATE TABLE {ks}.t (
        pk1 int, pk2 text,
        ck1 int, ck2 text, ck3 int,
        v int, s int static,
        PRIMARY KEY ((pk1, pk2), ck1, ck2, ck3)
    )
"""


READ_REPAIRS_METRIC = "scylla_storage_proxy_coordinator_foreground_read_repairs"


async def run(cql, stmt: str, host=None, cl=ConsistencyLevel.ALL) -> None:
    await cql.run_async(SimpleStatement(stmt, consistency_level=cl), host=host)


async def total_read_repairs(manager: ScyllaClusterManager, servers) -> int:
    return sum([(await manager.metrics.query(s.ip_addr)).get(READ_REPAIRS_METRIC) or 0 for s in servers])


async def write_all_mutation_kinds(cql, ks: str, hosts: list) -> None:
    """hosts: driver Hosts round-robined as coordinator, so every node (whatever its version)
       coordinates and RPCs mutations to differently-versioned peers."""
    t = f"{ks}.t"
    stmts = [
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 1, 'x', 1, 10)",   # plain insert
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 2, 'y', 2, 20)",
        f"UPDATE {t} SET v = 21 WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 2 AND ck2 = 'y' AND ck3 = 2",  # update
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 3, 'z', 3, 30)",
        f"DELETE v FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 3 AND ck2 = 'z' AND ck3 = 3",       # single-cell delete
        f"UPDATE {t} SET s = 111 WHERE pk1 = 3 AND pk2 = 's1'",                                       # static write
        f"UPDATE {t} SET s = 222 WHERE pk1 = 4 AND pk2 = 's2'",
        f"DELETE s FROM {t} WHERE pk1 = 4 AND pk2 = 's2'",                                            # static delete
    ]
    for i, stmt in enumerate(stmts):
        await run(cql, stmt, host=hosts[i % len(hosts)])
    # whole-row, partition and clustering-range (partial CK) deletes through every coordinator,
    # so both version pairings decode them
    for j, host in enumerate(hosts):
        ck1 = 4 + 10 * j
        await run(cql, f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', {ck1}, 'w', 4, 40)", host=host)
        await run(cql, f"DELETE FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = {ck1} AND ck2 = 'w' AND ck3 = 4", host=host)
        await run(cql, f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (9, 'del{j}', 1, 'r', 1, 90)", host=host)
        await run(cql, f"DELETE FROM {t} WHERE pk1 = 9 AND pk2 = 'del{j}'", host=host)
        ck = 5 + 10 * j
        await run(cql, f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', {ck}, 'p', 1, 51)", host=host)
        await run(cql, f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', {ck}, 'q', 2, 52)", host=host)
        await run(cql, f"DELETE FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = {ck}", host=host)


async def verify_all_mutation_kinds(manager: ScyllaClusterManager, servers, cql, ks: str, hosts: list,
                                    missed: list[tuple[int, str, int, int]] = ()) -> None:
    """missed: (ck1, ck2, ck3, v) rows that only repair could have delivered to a restarted node."""
    # CL=ALL reads reconcile replicas; a read repair would hide a replica that lost data
    repairs_before = await total_read_repairs(manager, servers)
    # every read through every coordinator, so each one's bounds reach both old and new replicas
    for host in hosts:
        await verify_through(cql, ks, host, len(hosts))
    if missed:
        rows = await cql.run_async(SimpleStatement(
            f"SELECT ck1, ck2, ck3, v FROM {ks}.t WHERE pk1 = 7 AND pk2 = 'missed' BYPASS CACHE",
            consistency_level=ConsistencyLevel.ALL), host=hosts[0])
        got = {(r.ck1, r.ck2, r.ck3): r.v for r in rows}
        for ck1, ck2, ck3, v in missed:
            assert got.get((ck1, ck2, ck3)) == v, f"{ks}: repair didn't deliver the missed row {(ck1, ck2, ck3)}"
    assert await total_read_repairs(manager, servers) == repairs_before, f"{ks}: replica disagreed: CL=ALL read was repaired"


async def verify_through(cql, ks: str, host, num_hosts: int) -> None:
    t = f"{ks}.t"

    async def select(where: str):
        return await cql.run_async(SimpleStatement(
            f"SELECT ck1, ck2, ck3, v FROM {t} WHERE {where} BYPASS CACHE",
            consistency_level=ConsistencyLevel.ALL), host=host)

    rows = {(r.ck1, r.ck2, r.ck3): r.v for r in await select("pk1 = 1 AND pk2 = 'a'")}
    assert rows.get((1, 'x', 1)) == 10, f"{ks}: plain insert lost"
    assert rows.get((2, 'y', 2)) == 21, f"{ks}: update lost"
    assert (3, 'z', 3) in rows and rows[(3, 'z', 3)] is None, f"{ks}: single-cell delete didn't take"
    assert not any(ck1 % 10 == 4 for ck1, _, _ in rows), f"{ks}: whole-row delete didn't take"
    assert not any(ck1 % 10 == 5 for ck1, _, _ in rows), f"{ks}: clustering-range tombstone didn't take"

    for j in range(num_hosts):
        assert len(await select(f"pk1 = 9 AND pk2 = 'del{j}'")) == 0, f"{ks}: partition delete didn't take"

    # a clustering-key range bound (ck1 >= 1 AND ck1 <= 2) serializes a
    # *partial* clustering_key_prefix as the read command's slice bound,
    # a components() call site the whole-partition selects above don't hit
    range_rows = {(r.ck1, r.ck2, r.ck3): r.v for r in await select("pk1 = 1 AND pk2 = 'a' AND ck1 >= 1 AND ck1 <= 2")}
    assert range_rows == {(1, 'x', 1): 10, (2, 'y', 2): 21}, f"{ks}: clustering-range read wrong: {range_rows}"

    static1 = await cql.run_async(SimpleStatement(
        f"SELECT s FROM {t} WHERE pk1 = 3 AND pk2 = 's1'", consistency_level=ConsistencyLevel.ALL),
        host=host)
    assert len(static1) == 1 and static1[0].s == 111, f"{ks}: static write lost"

    # after the static delete, that partition has no live cells at all (no
    # regular rows, static cell tombstoned) so the partition is gone
    static2 = await cql.run_async(SimpleStatement(
        f"SELECT s FROM {t} WHERE pk1 = 4 AND pk2 = 's2'", consistency_level=ConsistencyLevel.ALL),
        host=host)
    assert len(static2) == 0, f"{ks}: static delete didn't take"


async def repair_keyspaces(manager: ScyllaClusterManager, servers, keyspaces: list) -> None:
    """Repair streams the rows a restarted node missed while down, another
       components() call site distinct from normal read/write RPCs."""
    for i, ks in enumerate(keyspaces):
        await manager.api.repair(servers[i % len(servers)].ip_addr, ks, "t")


async def test_key_serializer_rolling_upgrade(
        manager: ScyllaClusterManager, scylla_2026_1: ScyllaVersionDescription, scylla_binary: pathlib.Path):
    """Key serializer across a 3-node rolling upgrade: every coordinator/replica version pairing,
    plus repair of rows a downed node missed."""
    logger.info("Bootstrapping 3-node cluster on scylla_2026_1")
    servers = await manager.servers_add(3, config={'rf_rack_valid_keyspaces': False, 'hinted_handoff_enabled': False}, version=scylla_2026_1)
    cql = manager.get_cql()

    async def get_hosts() -> list:
        hosts_by_ip = {h.address: h for h in await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)}
        return [hosts_by_ip[str(s.rpc_address)] for s in servers]

    hosts = await get_hosts()

    async def new_keyspace(name: str) -> None:
        await run(cql, f"""
            CREATE KEYSPACE {name}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}
        """)
        await run(cql, CREATE_TABLE.format(ks=name))

    async def write_and_verify(name: str) -> None:
        await write_all_mutation_kinds(cql, name, hosts)
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, name) for s in servers])
        await verify_all_mutation_kinds(manager, servers, cql, name, hosts)

    async def exercise(name: str) -> None:
        await new_keyspace(name)
        await write_and_verify(name)

    async def wait_all_up() -> None:
        # repair and CL=ALL reads need every node to see the restarted one as alive
        await asyncio.gather(*[manager.server_sees_others(s.server_id, len(servers) - 1, interval=60.0) for s in servers])
        nonlocal hosts
        hosts = await get_hosts()

    async def write_while_down(down: int, ck2: str, base: int, step: int) -> None:
        # rows the stopped node misses; only repair can deliver them (no hints)
        live_hosts = [h for j, h in enumerate(hosts) if j != down]
        for k, ks in enumerate(keyspaces):
            await run(cql, f"INSERT INTO {ks}.t (pk1, pk2, ck1, ck2, ck3, v) VALUES (7, 'missed', {k}, '{ck2}', {step + 1}, {base + k})",
                      host=live_hosts[k % len(live_hosts)], cl=ConsistencyLevel.QUORUM)

    keyspaces = ["keyspace_x"]
    all_missed = []
    logger.info(f"{keyspaces[0]}: write and verify on the fully-old cluster")
    await exercise(keyspaces[0])

    for i, server in enumerate(servers):
        fully_upgraded = i == len(servers) - 1
        suffix = " (cluster fully upgraded)" if fully_upgraded else ""
        logger.info(f"Upgrading server {i} to the tree under test{suffix}")
        others = [s for s in servers if s.server_id != server.server_id]
        await manager.server_stop_gracefully(server.server_id, seen_by=others)
        await write_while_down(i, 'm', 1000, i)
        await manager.server_switch_executable(server.server_id, scylla_binary)
        await manager.server_start(server.server_id, seen_by=others)
        await wait_all_up()
        missed = [(k, 'm', i + 1, 1000 + k) for k in range(len(keyspaces))]

        if not fully_upgraded:
            # new -> old direction: bounce a still-old node on the same binary
            old = servers[-1]
            others_of_old = [s for s in servers if s.server_id != old.server_id]
            await manager.server_stop_gracefully(old.server_id, seen_by=others_of_old)
            await write_while_down(len(servers) - 1, 'o', 2000, i)
            await manager.server_start(old.server_id, seen_by=others_of_old)
            await wait_all_up()
            missed += [(k, 'o', i + 1, 2000 + k) for k in range(len(keyspaces))]

        all_missed += missed
        logger.info(f"Repairing {keyspaces} with {i + 1}/{len(servers)} nodes upgraded")
        await repair_keyspaces(manager, servers, keyspaces)
        logger.info(f"Re-verifying {keyspaces} with {i + 1}/{len(servers)} nodes upgraded")
        for k, ks in enumerate(keyspaces):
            await verify_all_mutation_kinds(manager, servers, cql, ks, hosts, missed=[m for m in all_missed if m[0] == k])

        new_ks = f"keyspace_x_{i + 1}"
        logger.info(f"{new_ks}: write and verify with {i + 1}/{len(servers)} nodes upgraded")
        await exercise(new_ks)
        keyspaces.append(new_ks)

    logger.info("Final pass: re-verifying all keyspaces on the fully-upgraded cluster")
    for k, ks in enumerate(keyspaces):
        await verify_all_mutation_kinds(manager, servers, cql, ks, hosts, missed=[m for m in all_missed if m[0] == k])

# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import logging
import pathlib
import platform
import time

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_cluster import ScyllaVersionDescription
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.version_fetch_utils import fetch_and_install_scylla_version

logger = logging.getLogger(__name__)

# 2026.1 is the oldest version this test upgrades from; scylla_binary (the
# tree under test, i.e. anything from PR #31392 onward) is the new side.
async def _get_scylla_2026_1_description(build_mode: str) -> ScyllaVersionDescription:
    is_debug = build_mode in ('debug', 'sanitize')
    path = fetch_and_install_scylla_version(2026, 1, arch=platform.machine(), pack="debug" if is_debug else "")
    return ScyllaVersionDescription(path=str(path), config={}, argv=[])


@pytest.fixture(scope="function")
async def scylla_2026_1(build_mode, internet_dependency_enabled) -> ScyllaVersionDescription:
    return await _get_scylla_2026_1_description(build_mode)


# Exercises the key shapes touched by partition_key/clustering_key_prefix's
# components() serializer (idl-compiler.py's range_of<T> support): full key,
# a partial (shorter) clustering-key prefix, partition-key-only, and the
# empty-clustering-key case a static-only row uses.
CREATE_TABLE = """
    CREATE TABLE {ks}.t (
        pk1 int, pk2 text,
        ck1 int, ck2 text, ck3 int,
        v int, s int static,
        PRIMARY KEY ((pk1, pk2), ck1, ck2, ck3)
    )
"""


async def run(cql, stmt: str, host=None) -> None:
    await cql.run_async(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ALL), host=host)


async def write_all_mutation_kinds(cql, ks: str, hosts: list) -> None:
    """hosts: the driver Hosts to round-robin as coordinator, one per statement,
       so every node in the cluster (whatever its version at this point) both
       coordinates and RPCs these mutations to its (possibly differently
       versioned) peers."""
    t = f"{ks}.t"
    stmts = [
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 1, 'x', 1, 10)",   # plain insert
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 2, 'y', 2, 20)",
        f"UPDATE {t} SET v = 21 WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 2 AND ck2 = 'y' AND ck3 = 2",  # update
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 3, 'z', 3, 30)",
        f"DELETE v FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 3 AND ck2 = 'z' AND ck3 = 3",       # single-cell delete
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 4, 'w', 4, 40)",
        f"DELETE FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 4 AND ck2 = 'w' AND ck3 = 4",         # whole-row delete
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 5, 'p', 1, 51)",
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (1, 'a', 5, 'q', 2, 52)",
        f"DELETE FROM {t} WHERE pk1 = 1 AND pk2 = 'a' AND ck1 = 5",                                   # clustering-range tombstone (partial CK)
        f"INSERT INTO {t} (pk1, pk2, ck1, ck2, ck3, v) VALUES (9, 'del', 1, 'r', 1, 90)",
        f"DELETE FROM {t} WHERE pk1 = 9 AND pk2 = 'del'",                                             # partition delete
        f"UPDATE {t} SET s = 111 WHERE pk1 = 3 AND pk2 = 's1'",                                       # static write
        f"UPDATE {t} SET s = 222 WHERE pk1 = 4 AND pk2 = 's2'",
        f"DELETE s FROM {t} WHERE pk1 = 4 AND pk2 = 's2'",                                            # static delete
    ]
    for i, stmt in enumerate(stmts):
        await run(cql, stmt, host=hosts[i % len(hosts)])


async def verify_all_mutation_kinds(cql, ks: str, hosts: list) -> None:
    t = f"{ks}.t"
    coordinators = iter(hosts[i % len(hosts)] for i in range(5))

    async def select(where: str):
        return await cql.run_async(SimpleStatement(
            f"SELECT ck1, ck2, ck3, v FROM {t} WHERE {where} BYPASS CACHE",
            consistency_level=ConsistencyLevel.ALL), host=next(coordinators))

    rows = {(r.ck1, r.ck2, r.ck3): r.v for r in await select("pk1 = 1 AND pk2 = 'a'")}
    assert rows.get((1, 'x', 1)) == 10, f"{ks}: plain insert lost"
    assert rows.get((2, 'y', 2)) == 21, f"{ks}: update lost"
    assert (3, 'z', 3) in rows and rows[(3, 'z', 3)] is None, f"{ks}: single-cell delete didn't take"
    assert (4, 'w', 4) not in rows, f"{ks}: whole-row delete didn't take"
    assert not any(ck1 == 5 for ck1, _, _ in rows), f"{ks}: clustering-range tombstone didn't take"

    assert len(await select("pk1 = 9 AND pk2 = 'del'")) == 0, f"{ks}: partition delete didn't take"

    # a clustering-key range bound (ck1 >= 1 AND ck1 <= 2) serializes a
    # *partial* clustering_key_prefix as the read command's slice bound,
    # a components() call site the whole-partition selects above don't hit
    range_rows = {(r.ck1, r.ck2, r.ck3): r.v for r in await select("pk1 = 1 AND pk2 = 'a' AND ck1 >= 1 AND ck1 <= 2")}
    assert range_rows == {(1, 'x', 1): 10, (2, 'y', 2): 21}, f"{ks}: clustering-range read wrong: {range_rows}"

    static1 = await cql.run_async(SimpleStatement(
        f"SELECT s FROM {t} WHERE pk1 = 3 AND pk2 = 's1'", consistency_level=ConsistencyLevel.ALL),
        host=next(coordinators))
    assert len(static1) == 1 and static1[0].s == 111, f"{ks}: static write lost"

    # after the static delete, that partition has no live cells at all (no
    # regular rows, static cell tombstoned) so the partition is gone
    static2 = await cql.run_async(SimpleStatement(
        f"SELECT s FROM {t} WHERE pk1 = 4 AND pk2 = 's2'", consistency_level=ConsistencyLevel.ALL),
        host=next(coordinators))
    assert len(static2) == 0, f"{ks}: static delete didn't take"


async def repair_keyspaces(manager: ScyllaClusterManager, servers, keyspaces: list) -> None:
    """Repair streams the merged mutation between replicas, another
       components() call site distinct from normal read/write RPCs."""
    for i, ks in enumerate(keyspaces):
        await manager.api.repair(servers[i % len(servers)].ip_addr, ks, "t")


async def test_key_serializer_rolling_upgrade(
        manager: ScyllaClusterManager, scylla_2026_1: ScyllaVersionDescription, scylla_binary: pathlib.Path):
    """
    Mixed-version rolling-upgrade coverage for PR #31392's range_of<T> IDL
    serializer of partition_key/clustering_key_prefix::components(): a
    3-node cluster starts on the old serializer, is upgraded one node at a
    time, and at each step both older keyspaces (written by a fully-old or
    partially-upgraded cluster) and a brand-new keyspace are read and
    written across the mixed-version topology. Every write and read is
    explicitly coordinated through a specific, rotating node (not left to
    the driver's default host picking) so both "old coordinator, new
    replica" and "new coordinator, old replica" pairings actually happen.
    """
    logger.info("Bootstrapping 3-node cluster on scylla_2026_1")
    servers = await manager.servers_add(3, config={'rf_rack_valid_keyspaces': False}, version=scylla_2026_1)
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
        await verify_all_mutation_kinds(cql, name, hosts)

    async def exercise(name: str) -> None:
        await new_keyspace(name)
        await write_and_verify(name)

    keyspaces = ["keyspace_x"]
    logger.info(f"{keyspaces[0]}: write and verify on the fully-old cluster")
    await exercise(keyspaces[0])

    for i, server in enumerate(servers):
        fully_upgraded = i == len(servers) - 1
        suffix = " (cluster fully upgraded)" if fully_upgraded else ""
        logger.info(f"Upgrading server {i} to the tree under test{suffix}")
        await manager.server_change_version(server.server_id, scylla_binary)
        hosts = await get_hosts()

        # the fully-upgraded cluster needs no repair: all replicas already agree
        if not fully_upgraded:
            logger.info(f"Repairing {keyspaces} with {i + 1}/{len(servers)} nodes upgraded")
            await repair_keyspaces(manager, servers, keyspaces)
            logger.info(f"Re-verifying {keyspaces} with {i + 1}/{len(servers)} nodes upgraded")
            for ks in keyspaces:
                await verify_all_mutation_kinds(cql, ks, hosts)

        new_ks = f"keyspace_x_{i + 1}"
        logger.info(f"{new_ks}: write and verify with {i + 1}/{len(servers)} nodes upgraded")
        await exercise(new_ks)
        keyspaces.append(new_ks)

    logger.info("Final pass: re-verifying all keyspaces on the fully-upgraded cluster")
    for ks in keyspaces:
        await verify_all_mutation_kinds(cql, ks, hosts)

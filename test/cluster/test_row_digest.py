#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_feature
from test.cluster.util import new_test_keyspace


logger = logging.getLogger(__name__)

BIG_BLOB = bytes(range(256)) * 8
BIG_MAP_SIZE = 300
SHORT_TTL = 2


@pytest.mark.skip_mode(mode=DEBUG_MODES, reason='dev mode is enough for this test')
async def test_row_digest_no_mismatch(manager: ScyllaClusterManager) -> None:
    """CL=ALL reads with per-row digests (DIGEST_ROW_HASH) must not see digest
    mismatches when replicas hold the same data in different states (memtable,
    sstable, cache) and cached row/cell hashes are reused or invalidated."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    for host in hosts:
        await wait_for_feature("DIGEST_ROW_HASH", cql, host, time.time() + 60)

    async def write(query: str, params: tuple = ()) -> None:
        stmt = cql.prepare(query)
        stmt.consistency_level = ConsistencyLevel.ALL
        await cql.run_async(stmt, params)

    async def read(query: str) -> list:
        return list(await cql.run_async(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)))

    async def read_repairs() -> int:
        total = 0
        for s in servers:
            metrics = await manager.metrics.query(s.ip_addr)
            for name in ("scylla_storage_proxy_coordinator_foreground_read_repairs",
                         "scylla_storage_proxy_coordinator_background_read_repairs",
                         "scylla_storage_proxy_coordinator_read_repairs_diff_found_total"):
                total += int(metrics.get(name) or 0)
        return total

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TYPE {ks}.ut (a int, b text)")
        await cql.run_async(f"""CREATE TABLE {ks}.t (pk int, ck int, s int static, v int, txt text, b blob,
                                m map<int, blob>, st set<text>, fl frozen<list<int>>, u ut,
                                PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'""")
        await cql.run_async(f"CREATE TABLE {ks}.c (pk int, ck int, c1 counter, c2 counter, PRIMARY KEY (pk, ck)) "
                            "WITH speculative_retry = 'NONE'")

        insert_full = f"INSERT INTO {ks}.t (pk, ck, s, v, txt, b, m, st, fl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

        def full_row(pk: int, ck: int, big_blob: bool = False, big_map: bool = False) -> tuple:
            m = {i: bytes([i % 256]) * 4 for i in range(BIG_MAP_SIZE if big_map else 3)}
            return (pk, ck, pk, ck, f"text{ck}", BIG_BLOB if big_blob else b"\x01\x02", m,
                    {"a", "b", f"x{ck}"}, [ck, ck + 1])

        # pk=0: full rows, static, a large blob and a large map.
        for ck in range(10):
            await write(insert_full, full_row(0, ck, big_blob=(ck == 0), big_map=(ck == 1)))
        await write(f"UPDATE {ks}.t SET u = {{a: 1, b: 'one'}} WHERE pk = 0 AND ck = 2")

        # pk=1: partially set rows.
        await write(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (1, 0, 10)")
        await write(f"UPDATE {ks}.t SET m = m + ? WHERE pk = 1 AND ck = 1", ({7: b"seven"},))
        await write(f"INSERT INTO {ks}.t (pk, ck, b) VALUES (1, 2, ?)", (BIG_BLOB,))

        # pk=2: cell, collection element and range deletions.
        for ck in range(6):
            await write(insert_full, full_row(2, ck, big_blob=(ck == 0), big_map=(ck == 1)))
        await write(f"DELETE v, b FROM {ks}.t WHERE pk = 2 AND ck = 0")
        await write(f"DELETE m[3] FROM {ks}.t WHERE pk = 2 AND ck = 1")
        await write(f"UPDATE {ks}.t SET st = st - {{'a'}} WHERE pk = 2 AND ck = 1")
        await write(f"UPDATE {ks}.t SET u.a = 5 WHERE pk = 2 AND ck = 1")
        await write(f"DELETE FROM {ks}.t WHERE pk = 2 AND ck >= 2 AND ck < 4")

        # pk=3: short and long TTLs, on whole rows and on single cells.
        await write(f"INSERT INTO {ks}.t (pk, ck, v, txt) VALUES (3, 0, 30, 'short') USING TTL {SHORT_TTL}")
        await write(f"INSERT INTO {ks}.t (pk, ck, v, txt) VALUES (3, 1, 31, 'long') USING TTL 3600")
        await write(f"INSERT INTO {ks}.t (pk, ck, txt) VALUES (3, 2, 'keep')")
        await write(f"UPDATE {ks}.t USING TTL {SHORT_TTL} SET v = 32 WHERE pk = 3 AND ck = 2")
        await write(f"INSERT INTO {ks}.t (pk, ck, b) VALUES (3, 3, ?) USING TTL {SHORT_TTL}", (BIG_BLOB,))

        # pk=4: static-only partition.
        await write(f"INSERT INTO {ks}.t (pk, s) VALUES (4, 44)")

        for _ in range(3):
            await write(f"UPDATE {ks}.c SET c1 = c1 + 1, c2 = c2 - 2 WHERE pk = 0 AND ck = 0")
            await write(f"UPDATE {ks}.c SET c1 = c1 + 5 WHERE pk = 0 AND ck = 1")

        # Replicas now differ: one serves from sstables, two from memtables.
        await manager.api.keyspace_flush(servers[0].ip_addr, ks)

        reads = [
            f"SELECT * FROM {ks}.t",
            *[f"SELECT * FROM {ks}.t WHERE pk = {pk}" for pk in range(5)],
            f"SELECT pk, ck, v, b FROM {ks}.t WHERE pk = 0",
            f"SELECT m FROM {ks}.t WHERE pk = 0 AND ck = 1",
            f"SELECT txt, st FROM {ks}.t WHERE pk = 2",
            f"SELECT * FROM {ks}.t WHERE pk = 0 AND ck >= 2 AND ck < 7",
            f"SELECT * FROM {ks}.t WHERE pk = 0 ORDER BY ck DESC",
            f"SELECT * FROM {ks}.t WHERE pk = 2 AND ck >= 0 AND ck < 5 ORDER BY ck DESC",
            f"SELECT DISTINCT pk, s FROM {ks}.t",
            f"SELECT s FROM {ks}.t WHERE pk = 4",
            f"SELECT * FROM {ks}.c",
            f"SELECT c1 FROM {ks}.c WHERE pk = 0 ORDER BY ck DESC",
        ]

        async def read_all(times: int) -> None:
            # The first pass populates the caches, later ones reuse cached hashes.
            for _ in range(times):
                for q in reads:
                    await read(q)

        repairs_before = await read_repairs()

        await read_all(3)
        # pk=3 is left out: its short TTL may already have expired.
        assert sum([len(await read(f"SELECT * FROM {ks}.t WHERE pk = {pk}")) for pk in (0, 1, 2, 4)]) == 18
        rows = await read(f"SELECT b, m FROM {ks}.t WHERE pk = 0 AND ck IN (0, 1)")
        assert rows[0].b == BIG_BLOB and len(rows[1].m) == BIG_MAP_SIZE
        assert [r.ck for r in await read(f"SELECT ck FROM {ks}.t WHERE pk = 2")] == [0, 1, 4, 5]
        assert [(r.ck, r.s) for r in await read(f"SELECT * FROM {ks}.t WHERE pk = 4")] == [(None, 44)]
        assert [(r.c1, r.c2) for r in await read(f"SELECT * FROM {ks}.c WHERE pk = 0")] == [(3, -6), (15, None)]

        await asyncio.sleep(SHORT_TTL + 1)
        await read_all(2)
        assert [(r.ck, r.v) for r in await read(f"SELECT ck, v FROM {ks}.t WHERE pk = 3")] == [(1, 31), (2, None)]
        assert len(await read(f"SELECT * FROM {ks}.t")) == 20

        # Overwrites must invalidate cached row and cell hashes.
        await write(f"UPDATE {ks}.t SET v = 100, b = ? WHERE pk = 0 AND ck = 0", (BIG_BLOB[::-1],))
        await write(f"UPDATE {ks}.t SET m = m + ? WHERE pk = 0 AND ck = 1", ({1000: b"new"},))
        await write(f"UPDATE {ks}.t SET m = m - {{5}} WHERE pk = 0 AND ck = 1")
        await write(f"UPDATE {ks}.t SET b = 0x00 WHERE pk = 1 AND ck = 2")
        await write(f"UPDATE {ks}.t SET st = st + {{'zz'}}, u.b = 'changed' WHERE pk = 2 AND ck = 1")
        await write(f"DELETE m FROM {ks}.t WHERE pk = 2 AND ck = 4")
        await write(f"UPDATE {ks}.t SET s = 1 WHERE pk = 0")
        await write(f"UPDATE {ks}.c SET c1 = c1 + 1 WHERE pk = 0 AND ck = 0")

        await read_all(2)
        rows = await read(f"SELECT v, b, m FROM {ks}.t WHERE pk = 0 AND ck IN (0, 1)")
        assert rows[0].v == 100 and rows[0].b == BIG_BLOB[::-1]
        assert len(rows[1].m) == BIG_MAP_SIZE and 1000 in rows[1].m and 5 not in rows[1].m
        assert (await read(f"SELECT c1 FROM {ks}.c WHERE pk = 0 AND ck = 0"))[0].c1 == 4

        # A column added later: some rows use it, others leave it unset.
        await cql.run_async(f"ALTER TABLE {ks}.t ADD extra text")
        for ck in range(3):
            await write(f"UPDATE {ks}.t SET extra = ? WHERE pk = 0 AND ck = ?", (f"e{ck}", ck))
        await write(f"INSERT INTO {ks}.t (pk, ck, v, extra) VALUES (5, 0, 50, 'x')")
        await write(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (5, 1, 51)")
        await read_all(2)
        assert [r.extra for r in await read(f"SELECT extra FROM {ks}.t WHERE pk = 0 AND ck < 4")] == ["e0", "e1", "e2", None]

        assert await read_repairs() == repairs_before, "digest mismatch between replicas"

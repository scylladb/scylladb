#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests of CDC with full preimage and postimage on multi-megabyte values, so
that a single write puts about three times its size into the CDC log.
"""

import asyncio
import logging
from collections import defaultdict

import pytest
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

MB = 1024 * 1024

# CDC log operations (cdc$operation)
PREIMAGE, UPDATE, INSERT, POSTIMAGE = 0, 1, 2, 9

# Reads which run concurrently with the large writes may fail with these
# errors, which are expected under such memory pressure (#12600).
EXPECTED_ERRORS = [
    r"[Ee]xception when communicating with \S+, to read from \S+: std::bad_alloc",
    r"exception during mutation write to \S+: std::bad_alloc",
    r"exception during mutation write to \S+: logalloc::bad_alloc",
    r"[Ee]xception when communicating with \S+, to read from \S+: utils::memory_limit_reached",
]

KEYS = [(pk, ck) for pk in range(2) for ck in range(2)]
NUM_CELLS = 5


def single_blob():
    """One 7 MB blob per row, updated with 4 MB blobs"""
    columns = "v blob"
    insert = ("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)", [{"v": b"1" * 7 * MB}])
    updates = ("UPDATE {table} SET v = ? WHERE pk = ? AND ck = ?", [{"v": b"2" * 4 * MB}])
    return columns, insert, updates


def several_blobs():
    """A row with five 1 MB blob columns, all updated together"""
    names = [f"v_{i}" for i in range(NUM_CELLS)]
    columns = ", ".join(f"{n} blob" for n in names)
    insert = (f"INSERT INTO {{table}} (pk, ck, {', '.join(names)}) VALUES (?, ?, {', '.join('?' * NUM_CELLS)})",
              [{n: b"1" * MB for n in names}])
    updates = (f"UPDATE {{table}} SET {', '.join(f'{n} = ?' for n in names)} WHERE pk = ? AND ck = ?",
               [{n: b"2" * MB for n in names}])
    return columns, insert, updates


def blob_map():
    """A map<text, blob> with a 1 MB element, to which five more 1 MB elements are added one by one"""
    columns = "v map<text, blob>"
    insert = ("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)", [{"v": {"key": b"1" * MB}}])
    updates = ("UPDATE {table} SET v = v + ? WHERE pk = ? AND ck = ?",
               [{"v": {f"key{k}": b"2" * MB}} for k in range(5)])
    return columns, insert, updates


def to_plain(value):
    return dict(value) if hasattr(value, "items") else value


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("prepared", [True, False], ids=["prepared", "unprepared"])
@pytest.mark.parametrize("shape", [single_blob, several_blobs, blob_map])
async def test_cdc_large_values(manager: ScyllaClusterManager, shape, prepared: bool):
    """
    Write multi-megabyte values (one big blob, several big blobs, or a map of
    big blobs) to a table with CDC preimage 'full' and postimage, while reading
    the base table and the CDC log concurrently. All writes must succeed, the
    base table and the CDC log must hold the written values, and - for
    prepared statements - no oversized allocations may happen. Unprepared
    statements carry the blobs as hex literals in the query text, whose
    parsing makes oversized allocations, so that check is skipped for them.
    """
    config = {
        "max_memory_for_unlimited_query_soft_limit": 20 * MB,
        "max_memory_for_unlimited_query_hard_limit": 20 * MB,
    }
    # The default 512 MB per shard is not enough for these values (#27539).
    server = await manager.server_add(config={**config, "tablets_mode_for_new_keyspaces": "disabled"}, cmdline=["-m", "2G"])
    cql = manager.get_cql()
    log = await manager.server_open_log(server.server_id)

    columns, (insert_cql, insert_values), (update_cql, update_values) = shape()
    value_columns = list(insert_values[0].keys())

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk bigint, ck bigint, {columns}, PRIMARY KEY (pk, ck)) "
                            "WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true}")

        def statement(query):
            """Return the statement, and a function building its arguments from the values and the key"""
            query = query.format(table=table)
            if query.startswith("UPDATE"):
                args = lambda values, pk, ck: [*values.values(), pk, ck]
            else:
                args = lambda values, pk, ck: [pk, ck, *values.values()]
            if prepared:
                return cql.prepare(query), args
            # The driver inlines the values into the query text.
            return SimpleStatement(query.replace("?", "%s")), args

        insert, insert_args = statement(insert_cql)
        update, update_args = statement(update_cql)

        mark = await log.mark()
        for pk, ck in KEYS:
            await cql.run_async(insert, insert_args(insert_values[0], pk, ck))

        failed_reads = 0
        async def read_loop(query, done):
            nonlocal failed_reads
            while not done.is_set():
                try:
                    await cql.run_async(query)
                except Exception as e:
                    logger.info(f"Concurrent read failed: {str(e)[:200]}")
                    failed_reads += 1

        done = asyncio.Event()
        readers = [asyncio.create_task(read_loop(SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0 AND ck = 0"), done)),
                   asyncio.create_task(read_loop(SimpleStatement(f"SELECT * FROM {table}_scylla_cdc_log LIMIT 10"), done))]
        try:
            for values in update_values:
                await asyncio.gather(*[cql.run_async(update, update_args(values, pk, ck)) for pk, ck in KEYS])
        finally:
            done.set()
            await asyncio.gather(*readers)
        logger.info(f"{failed_reads} concurrent reads failed")

        # Expected final value of each value column
        final = {}
        for values in [insert_values[0], *update_values]:
            for c, v in values.items():
                final[c] = {**final[c], **v} if isinstance(v, dict) and c in final else v
        for pk, ck in KEYS:
            row = (await cql.run_async(f"SELECT * FROM {table} WHERE pk = {pk} AND ck = {ck}"))[0]
            assert {c: to_plain(getattr(row, c)) for c in value_columns} == final

        # The CDC log of each row: the INSERT (with no preimage, as the row
        # did not exist) and its postimage, then preimage, delta and postimage
        # of each UPDATE; the last postimage is the final row.
        log_rows = defaultdict(list)
        for r in await cql.run_async(f'SELECT pk, ck, "cdc$operation", {", ".join(value_columns)} FROM {table}_scylla_cdc_log'):
            log_rows[(r.pk, r.ck)].append(r)
        for key in KEYS:
            rows = log_rows[key]
            assert [r[2] for r in rows] == [INSERT, POSTIMAGE] + [PREIMAGE, UPDATE, POSTIMAGE] * len(update_values)
            assert {c: to_plain(getattr(rows[0], c)) for c in value_columns} == insert_values[0]
            assert {c: to_plain(getattr(rows[-1], c)) for c in value_columns} == final

    if prepared:
        oversized = await log.grep("oversized allocation", from_mark=mark)
        assert not oversized, f"oversized allocations: {[line for line, _ in oversized]}"
    errors = await log.grep(r"^ERROR\b", filter_expr="|".join(EXPECTED_ERRORS), from_mark=mark)
    assert not errors, f"unexpected errors: {[line for line, _ in errors]}"

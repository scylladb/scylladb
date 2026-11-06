#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import glob
import json
import os
import pytest
import logging
import time
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import create_new_test_keyspace, new_test_keyspace, reconnect_driver
from test.pylib.random_tables import Column, TextType
from cassandra.cluster import NoHostAvailable, Session

logger = logging.getLogger(__name__)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_reboot(request, manager: ScyllaClusterManager):
    # Check that commitlog provides durability in case of a node reboot.
    #
    # 1. start a node with commitlog_sync: batch
    # 2. create a test table t
    # 3. write some data to the table and truncate it, now a truncation record should be created
    # 4. cleanly restart a node with enabled injection decrease_schema_commitlog_base_segment_id,
    #    this allows to set new segment id to zero, which imitates machine reboot
    # 5. write some data into t
    # 6. save its current content in local variable
    # 7. kill -9 the node and restart it
    # 8. check the table content is preserved

    server_info = await manager.server_add(config={
        'commitlog_sync': 'batch'
    })
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logger.info("Node started")

    random_tables = RandomTables(request.node.name, manager, "ks", 1)
    await random_tables.add_table(name='t', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])
    logger.info("Test table created")

    async def load_table() -> list:
        result = await cql.run_async("select * from ks.t")
        result.sort()
        return result
    def save_table(k: str, v: str):
        return cql.run_async("insert into ks.t(key, value) values (%s, %s)",
                             parameters = [k, v])

    test_rows_count = 10
    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'key_{i}', f'value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some data is written into test table")

    await cql.run_async("truncate table ks.t")
    logger.info("Test table is truncated")

    await manager.server_stop_gracefully(server_info.server_id)
    await manager.server_update_config(server_info.server_id,
                                       'error_injections_at_startup',
                                       ['decrease_commitlog_base_segment_id'])
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is restarted with decrease_schema_commitlog_base_segment_id injection")

    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'new_key_{i}', f'new_value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some new data is written into test table")

    table_content_before_crash = await load_table()
    await manager.server_stop(server_info.server_id, convict=False)
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is killed and restarted")

    table_content_after_crash = await load_table()
    logging.info(f"table content before crash [{table_content_before_crash}], "
                 f"after crash [{table_content_after_crash}]")
    assert table_content_before_crash == table_content_after_crash


async def kill_and_restart(manager: ScyllaClusterManager, server) -> Session:
    """Kill the server (no flush, no clean shutdown), start it again so it
    replays its commitlog, and return a CQL session ready to query it."""
    await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    await reconnect_driver(manager)
    cql, _ = await manager.get_ready_cql([server])
    return cql


async def test_commitlog_replay_with_alter_table(manager: ScyllaClusterManager):
    """Commitlog replay must apply mutations written under older schema
    versions: the commitlog holds writes made before and after ADD, DROP
    (of a regular and of a collection column) and re-ADD of a dropped column
    with another type, and after kill -9 every row must read back as before."""
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (pk1 int, ck1 int, r2 int, r3 text, r5 set<int>, PRIMARY KEY (pk1, ck1))")

        n_partitions = 5
        for key in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r2, r3, r5) VALUES ({key}, 9, 8, 'seven', {{6, 5}})")
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r3) VALUES ({key}, 8, 'eight')")
        # These rows are read back from sstables, everything below from the commitlog.
        await manager.api.keyspace_flush(server.ip_addr, ks)

        for key in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r2, r3, r5) VALUES ({key}, 0, 1, 'two', {{3, 4}})")
        await cql.run_async(f"ALTER TABLE {ks}.cf ADD r1 int")
        for key in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r1, r2, r3, r5) VALUES ({key}, 1, 2, 3, 'four', {{5, 6, 7}})")
        await cql.run_async(f"ALTER TABLE {ks}.cf DROP r2")
        for key in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r1, r3) VALUES ({key}, 2, 3, 'four')")
        await cql.run_async(f"ALTER TABLE {ks}.cf DROP r5")
        await cql.run_async(f"ALTER TABLE {ks}.cf ADD r2 varint")
        await cql.run_async(f"ALTER TABLE {ks}.cf ADD r4 int")
        for key in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk1, ck1, r2, r4) VALUES ({key}, 0, 99, 999)")

        async def check_data(cql):
            for key in range(n_partitions):
                rows = await cql.run_async(f"SELECT pk1, ck1, r1, r2, r3, r4 FROM {ks}.cf WHERE pk1 = {key}")
                assert sorted(tuple(r) for r in rows) == [
                    (key, 0, None, 99, 'two', 999),
                    (key, 1, 2, None, 'four', None),
                    (key, 2, 3, None, 'four', None),
                    (key, 8, None, None, 'eight', None),
                    (key, 9, None, None, 'seven', None),
                ]

        await check_data(cql)
        cql = await kill_and_restart(manager, server)
        await check_data(cql)


@pytest.mark.parametrize("durable_first", [True, False])
async def test_alter_keyspace_durable_writes(manager: ScyllaClusterManager, durable_first: bool):
    """ALTER KEYSPACE ... WITH durable_writes takes effect for the writes that
    follow it: writes made while durable_writes is false bypass the commitlog
    and are lost on kill -9, writes made while it is true are replayed."""
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (id int PRIMARY KEY)")
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH durable_writes = {str(durable_first).lower()}")
        for id in (1, 2):
            await cql.run_async(f"INSERT INTO {ks}.cf (id) VALUES ({id})")
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH durable_writes = {str(not durable_first).lower()}")
        for id in (3, 4):
            await cql.run_async(f"INSERT INTO {ks}.cf (id) VALUES ({id})")

        cql = await kill_and_restart(manager, server)
        rows = await cql.run_async(f"SELECT id FROM {ks}.cf")
        assert sorted(r.id for r in rows) == ([1, 2] if durable_first else [3, 4])


async def write_oversized_mutation(manager: ScyllaClusterManager, server, ks: str) -> tuple[str, list]:
    """Write one row of 20 columns of 1MB each in a single mutation. It is
    larger than the commitlog's per-entry limit (about half of the default
    32MB segment), so the commitlog stores it as a multi-fragment entry.
    Return a SELECT reading the row back and the row."""
    cql = manager.get_cql()
    columns = 20
    await cql.run_async(f"CREATE TABLE {ks}.big (pk int PRIMARY KEY, {', '.join(f'c{i} text' for i in range(columns))})")
    names = ', '.join(f'c{i}' for i in range(columns))
    row = [1, *(f"value_{i}_" + 'x' * 1024 * 1024 for i in range(columns))]
    insert = cql.prepare(f"INSERT INTO {ks}.big (pk, {names}) VALUES ({', '.join('?' * (columns + 1))})")
    await cql.run_async(insert, row)
    select = f"SELECT pk, {names} FROM {ks}.big"
    assert [list(r) for r in await cql.run_async(select)] == [row]
    return select, row


async def test_oversized_mutation_replay(manager: ScyllaClusterManager):
    """A single mutation bigger than the commitlog entry limit is written as
    several fragments; replay after kill -9 must reassemble and apply it."""
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        select, row = await write_oversized_mutation(manager, server, ks)
        cql = await kill_and_restart(manager, server)
        assert [list(r) for r in await cql.run_async(select)] == [row]


async def test_oversized_mutation_corrupted_commitlog(manager: ScyllaClusterManager):
    """If the commitlog segments holding an oversized mutation are corrupted
    while the node is down, the node must still start, report the corruption
    and not apply the mutation."""
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    manager.ignore_log_patterns.append("commitlog_replayer - Corrupted file")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        select, _ = await write_oversized_mutation(manager, server, ks)
        commitlog_dir = json.loads((await manager.get_cql().run_async(
            "SELECT value FROM system.config WHERE name = 'commitlog_directory'"))[0].value)
        await manager.server_stop(server.server_id, convict=False)

        segments = glob.glob(os.path.join(commitlog_dir, "CommitLog-*.log"))
        assert segments
        for segment in segments:
            with open(segment, "r+b") as f:
                f.seek(18)
                f.write(os.urandom(474))

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql([server])
        assert await log.grep("commitlog_replayer - Corrupted file", from_mark=mark)
        assert not await cql.run_async(select)


async def test_commitlog_replay_with_counters(manager: ScyllaClusterManager):
    """The commitlog stores the resulting counter values, not the deltas, so
    replay after kill -9 must not apply the increments again."""
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (pk int, ck int, cnt counter, PRIMARY KEY (pk, ck))")
        for i in range(1, 10):
            await cql.run_async(f"UPDATE {ks}.cf SET cnt = cnt + {i} WHERE pk = 5 AND ck = 6")
        await cql.run_async(f"UPDATE {ks}.cf SET cnt = cnt - 1 WHERE pk = 5 AND ck = 6")
        await cql.run_async(f"UPDATE {ks}.cf SET cnt = cnt + 10 WHERE pk = 7 AND ck = 8")

        async def check_counters(cql):
            rows = await cql.run_async(f"SELECT pk, ck, cnt FROM {ks}.cf")
            assert sorted(tuple(r) for r in rows) == [(5, 6, 44), (7, 8, 10)]

        await check_counters(cql)
        cql = await kill_and_restart(manager, server)
        await check_counters(cql)


# With the default periodic commitlog sync, a write is acknowledged before it
# reaches the disk. The period is long enough that no periodic sync happens
# during the LWT tests below, so what survives a kill is decided only by the
# syncs that LWT writes force.
LWT_TEST_CONFIG = {'commitlog_sync': 'periodic', 'commitlog_sync_period_in_ms': 600000}


async def test_lwt_writes_are_durable_in_periodic_mode(manager: ScyllaClusterManager):
    """Even with periodic commitlog sync, an LWT write is synced to the
    commitlog before it is acknowledged, so all acknowledged LWT writes
    survive kill -9."""
    server = await manager.server_add(config=LWT_TEST_CONFIG)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        for i in range(100):
            await cql.run_async(f"UPDATE {ks}.cf SET v = {i} WHERE pk = {i} AND ck = {i} IF v = NULL")
        expected = [(i, i, i) for i in range(100)]
        assert sorted(tuple(r) for r in await cql.run_async(f"SELECT * FROM {ks}.cf")) == expected

        cql = await kill_and_restart(manager, server)
        assert sorted(tuple(r) for r in await cql.run_async(f"SELECT * FROM {ks}.cf")) == expected


@pytest.mark.parametrize("scenario", ["two_partitions", "same_partition", "same_partition_delete"])
async def test_lwt_write_syncs_preceding_writes(manager: ScyllaClusterManager, scenario: str):
    """In periodic commitlog mode, the sync forced by an LWT write also makes
    durable the non-LWT writes that reached the same commitlog (shard) before
    it; the non-LWT writes after the last LWT write may be lost on kill -9
    (unless something else synced the commitlog meanwhile).
    The node has one shard, so all writes go to the same commitlog.
    The scenarios: LWT and non-LWT writes in different partitions, in the same
    partition, and in the same partition with the last LWT being a DELETE."""
    server = await manager.server_add(config=LWT_TEST_CONFIG, cmdline=['--smp', '1'])
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        rows = set()

        async def insert(pk, ck):
            await cql.run_async(f"INSERT INTO {ks}.cf (pk, ck, v) VALUES ({pk}, {ck}, {ck})")
            rows.add((pk, ck, ck))

        async def insert_lwt(pk, ck):
            res = await cql.run_async(f"INSERT INTO {ks}.cf (pk, ck, v) VALUES ({pk}, {ck}, {ck}) IF NOT EXISTS")
            assert res[0].applied
            rows.add((pk, ck, ck))

        # Interleave LWT and non-LWT writes, then end with a last LWT write
        # (or LWT DELETE) in the middle of a stream of non-LWT writes.
        # Everything written up to that last LWT write is expected to survive.
        if scenario == "two_partitions":
            for i in range(100):
                await insert_lwt(1, i)
                await insert(2, i)
            for i in range(100, 150):
                await insert(2, i)
            await insert_lwt(1, 150)
        elif scenario == "same_partition":
            for i in range(100):
                await insert_lwt(1, i)
            for i in range(100, 250):
                await insert(1, i)
            await insert_lwt(1, 250)
        else:
            for i in range(100):
                await insert(1, i)
            for i in range(100, 200):
                await insert_lwt(1, i)
            for i in range(200, 250):
                await insert(1, i)
            res = await cql.run_async(f"DELETE FROM {ks}.cf WHERE pk = 1 AND ck = 150 IF EXISTS")
            assert res[0].applied
            rows.remove((1, 150, 150))
        expected = set(rows)
        # Not synced by any LWT write: these may or may not survive kill -9.
        for i in range(251, 300):
            await insert(2 if scenario == "two_partitions" else 1, i)
        unsynced = rows - expected

        cql = await kill_and_restart(manager, server)
        survived = {tuple(r) for r in await cql.run_async(f"SELECT * FROM {ks}.cf")}
        assert expected <= survived
        assert survived - expected <= unsynced


async def test_commitlog_total_space_limit(manager: ScyllaClusterManager):
    """With commitlog_use_hard_size_limit, the commitlog must stay within
    commitlog_total_space_in_mb (plus the one extra segment Scylla allows)
    under sustained writes, relying on memtable flushes triggered at
    commitlog_flush_threshold_in_mb to free segments, and all acknowledged
    writes must survive kill -9.
    See https://github.com/scylladb/scylladb/pull/6368"""
    segment_size_mb = 5
    total_space_mb = 30
    flush_threshold_mb = 15
    server = await manager.server_add(cmdline=['--smp', '1'], config={
        'commitlog_sync': 'batch',
        'commitlog_segment_size_in_mb': segment_size_mb,
        'commitlog_total_space_in_mb': total_space_mb,
        'commitlog_flush_threshold_in_mb': flush_threshold_mb,
        'commitlog_reuse_segments': True,
        'commitlog_use_hard_size_limit': True,
    })
    cql = manager.get_cql()
    commitlog_dir = json.loads((await cql.run_async(
        "SELECT value FROM system.config WHERE name = 'commitlog_directory'"))[0].value)

    def commitlog_size_mb():
        size = 0
        # Includes recycled segments, which keep their disk space.
        for f in glob.glob(os.path.join(commitlog_dir, "*CommitLog-*")):
            try:
                size += os.stat(f).st_size
            except FileNotFoundError:
                pass  # deleted while we were looking
        return size / (1024 * 1024)

    # A segment may exceed the segment size by up to 1MB.
    size_limit_mb = total_space_mb + segment_size_mb + 1
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.cf (pk int PRIMARY KEY, v text)")
        insert = cql.prepare(f"INSERT INTO {ks}.cf (pk, v) VALUES (?, ?)")
        value = 'x' * 10 * 1024
        # About 4 times the total space limit.
        rows = 12000
        max_size_mb = 0
        sem = asyncio.Semaphore(32)

        async def write(pk):
            async with sem:
                await cql.run_async(insert, [pk, value])

        writes = asyncio.gather(*(write(pk) for pk in range(rows)))
        while not writes.done():
            max_size_mb = max(max_size_mb, commitlog_size_mb())
            assert max_size_mb <= size_limit_mb
            await asyncio.sleep(0.05)
        await writes
        logger.info(f"Maximal commitlog size seen: {max_size_mb:.1f}MB")
        # The writes filled the commitlog up to the flush threshold, so the
        # limit was actually exercised.
        assert max_size_mb > flush_threshold_mb

        await manager.server_stop(server.server_id, convict=False)
        assert commitlog_size_mb() <= size_limit_mb
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql([server])
        assert (await cql.run_async(f"SELECT count(*) FROM {ks}.cf"))[0].count == rows


async def test_commitlog_io_error_isolates_node(manager: ScyllaClusterManager):
    """When the commitlog cannot create new segments because of an I/O error
    (here: the commitlog directory is not writable), the node must stay up
    but stop serving requests ("Shutting down communications due to I/O
    errors until operator intervention")."""
    # Small segments, so that the writes below need new ones soon.
    server = await manager.server_add(config={'commitlog_segment_size_in_mb': 1})
    manager.ignore_log_patterns += [
        "Shutting down communications due to I/O errors until operator intervention",
        r"commitlog - Exception in segment reservation: storage_io_error",
        r"commitlog - Could not (delete|recycle) segment",
    ]
    cql = manager.get_cql()
    commitlog_dir = json.loads((await cql.run_async(
        "SELECT value FROM system.config WHERE name = 'commitlog_directory'"))[0].value)
    # Not new_test_keyspace(): the keyspace cannot be dropped once the node
    # stopped serving requests.
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async(f"CREATE TABLE {ks}.cf (pk int PRIMARY KEY, v text)")
    await cql.run_async(f"INSERT INTO {ks}.cf (pk, v) VALUES (0, 'a')")
    assert list((await cql.run_async(f"SELECT * FROM {ks}.cf WHERE pk = 0"))[0]) == [0, 'a']

    log = await manager.server_open_log(server.server_id)
    mode = os.stat(commitlog_dir).st_mode
    os.chmod(commitlog_dir, 0)
    try:
        # Root, or a process with CAP_DAC_OVERRIDE (as in some containers),
        # ignores directory permissions.
        try:
            open(os.path.join(commitlog_dir, 'probe'), 'x').close()
        except OSError:
            pass
        else:
            os.remove(os.path.join(commitlog_dir, 'probe'))
            pytest.skip("directory permissions are not enforced for this user")
        insert = cql.prepare(f"INSERT INTO {ks}.cf (pk, v) VALUES (?, ?)")
        value = 'x' * 1024
        # Each write adds ~1KB to the commitlog; 100MB is far more than the
        # segments the commitlog had reserved before the chmod.
        with pytest.raises(Exception):
            for pk in range(1, 100000):
                await cql.run_async(insert, [pk, value])
        await log.wait_for("Shutting down communications due to I/O errors until operator intervention", timeout=60)
        assert await manager.server_is_alive(server.server_id)
        with pytest.raises(NoHostAvailable):
            await cql.run_async(f"SELECT * FROM {ks}.cf WHERE pk = 0")
    finally:
        os.chmod(commitlog_dir, mode)

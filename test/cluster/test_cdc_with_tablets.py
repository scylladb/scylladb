#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

# Tests for CDC tables in tablets enabled keyspaces

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for
from test.pylib.tablets import get_tablet_count, get_base_table, get_tablet_replicas
from test.cluster.util import new_test_keyspace
from test.cluster.conftest import skip_mode

import asyncio
import logging
import threading
import time
import pytest
from enum import IntEnum


logger = logging.getLogger(__name__)

# follows cdc::stream_state
class CdcStreamState(IntEnum):
    CURRENT = 0
    CLOSED = 1
    OPENED = 2

# Basic test creating a table with CDC enabled, using either CREATE or ALTER to enable CDC, and
# verifying that CDC streams for the table are created. Then we write to the table and verify
# the CDC log entries are created in the table's streams.
@pytest.mark.parametrize("with_alter", [pytest.param(False, id="create"), pytest.param(True, id="alter")])
@pytest.mark.asyncio
async def test_create_cdc_with_tablets(manager: ManagerClient, with_alter: bool):
    servers = await manager.servers_add(1)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        if with_alter:
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)")
            await cql.run_async(f"ALTER TABLE {ks}.test WITH cdc={{'enabled': true}}")
        else:
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 2

        log_table_id = await manager.get_table_id(ks, "test_scylla_cdc_log")
        for r in rows:
            assert r.table_id == log_table_id
        streams = [r.stream_id for r in rows]

        row_count=1000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test(pk, v) VALUES({i},{i+1})") for i in range(row_count)])

        rows = await cql.run_async(f"SELECT * FROM {ks}.test_scylla_cdc_log")
        assert len(rows) == row_count

        total_log_rows = 0
        for s in streams:
            rows = await cql.run_async(f"SELECT * FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\"=0x{s.hex()}")
            total_log_rows += len(rows)
        assert total_log_rows == row_count

        # Verify the cdc log table is created as a co-located table.
        base_id = await manager.get_table_id(ks, 'test')
        cdc_id = await manager.get_table_id(ks, 'test_scylla_cdc_log')
        assert base_id == (await get_base_table(manager, cdc_id))

# Create tables with CDC and verify the CDC streams are removed from the
# system tables when the tables or keyspaces are dropped.
@pytest.mark.asyncio
async def test_drop_table_and_drop_keyspace_removes_cdc_streams(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks1:
        await cql.run_async(f"CREATE TABLE {ks1}.test1 (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 2

        await cql.run_async(f"CREATE TABLE {ks1}.test2 (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 4

        await cql.run_async(f"DROP TABLE {ks1}.test2")
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 2

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks2:
            await cql.run_async(f"CREATE TABLE {ks2}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")
            rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
            assert len(rows) == 4

        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 2

    rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
    assert len(rows) == 0

# Read CDC stream information from the virtual tables system.cdc_timestamps and system.cdc_streams
@pytest.mark.asyncio
async def test_cdc_virtual_table(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        rows = await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert len(rows) == 1

        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        rows = await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert len(rows) == 2

        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) as ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test1'")
        assert len(rows) == 1
        ts1 = rows[0].ts

        rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test1' AND timestamp={ts1} AND stream_state={CdcStreamState.CURRENT}")
        assert len(rows) == 2

        await cql.run_async(f"DROP TABLE {ks}.test1")
        rows = await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert len(rows) == 1

        await cql.run_async(f"DROP TABLE {ks}.test2")
        rows = await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert len(rows) == 0

        rows = await cql.run_async("SELECT * FROM system.cdc_streams")
        assert len(rows) == 0

# Split the tablets of a CDC table and wait for the CDC streams to split and become synchronized.
# Then read the sequence of stream sets in two ways - by reading the current stream set for each
# timestamp, and by applying the differences (closed / opened) in each timestamp, and verify we
# get the same result.
# Then trigger tablet merge and do the same, verifying streams are merged.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_stream_split_and_merge_basic(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        assert len(rows) == 1
        ts = rows[0].ts

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
        rows = await cql.run_async(f"SELECT stream_id FROM system.cdc_streams WHERE keyspace_name = '{ks}' AND table_name = 'test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
        assert len(rows) == tablet_count

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()
        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': 4}};")
        await log.wait_for("Detected tablet split", from_mark=mark, timeout=60)

        async def streams_are_synchronized_with_tablets():
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
            rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
            ts = rows[0].ts # most recent
            rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
            logger.info(rows)
            if len(rows) == tablet_count:
                return True
        await wait_for(streams_are_synchronized_with_tablets, time.time() + 60)

        # Reconstruct the stream set in each timestamp in two ways:
        # * read the streams with kind=current
        # * apply diffs
        # and verify we get the same result
        async def validate_stream_history():
            rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' ORDER BY timestamp ASC")
            timestamps = [r.ts for r in rows]
            current_streams = set()
            for ts in timestamps:
                rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state >= 1 and stream_state <= 2")
                closed_streams = set([r.stream_id for r in rows if r.stream_state == CdcStreamState.CLOSED])
                opened_streams = set([r.stream_id for r in rows if r.stream_state == CdcStreamState.OPENED])

                assert closed_streams.issubset(current_streams)

                # Construct the stream set by applying the difference in each timestamp, and verify it's the same
                # as reading the current stream set.
                current_streams = (current_streams - closed_streams) | opened_streams

                rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
                current_streams2 = set([r.stream_id for r in rows])
                assert current_streams2 == current_streams

        await validate_stream_history()

        mark = await log.mark()
        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': 1}};")
        await log.wait_for("Detected tablet merge", from_mark=mark, timeout=60)
        await wait_for(streams_are_synchronized_with_tablets, time.time() + 60)

        await validate_stream_history()

# Write to a table with CDC while streams are split and merged. Then read the log data from all streams
# and verify we get the exact data we expect.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_write_during_split_and_merge(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        assert len(rows) == 1
        ts = rows[0].ts

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
        rows = await cql.run_async(f"SELECT stream_id FROM system.cdc_streams WHERE keyspace_name = '{ks}' AND table_name = 'test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
        assert len(rows) == tablet_count

        # insert rows in a new thread while the streams are split or merged
        stop_writer = threading.Event()
        def do_writes():
            i=0
            stmt = cql.prepare(f"INSERT INTO {ks}.test(pk, v) VALUES(?, ?)")
            while not stop_writer.is_set():
                cql.execute(stmt, [i, i])
                i += 1
        writer_thread = threading.Thread(target=do_writes)
        writer_thread.start()

        for tablet_count in [4, 16, 1]:
            log = await manager.server_open_log(servers[0].server_id)
            mark = await log.mark()
            await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': {tablet_count}}};")
            await log.wait_for("Detected tablet (split|merge)", from_mark=mark, timeout=60)

            async def streams_are_synchronized_with_tablets():
                tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
                rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
                ts = rows[0].ts
                rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
                logger.info(rows)
                if len(rows) == tablet_count:
                    return True
            await wait_for(streams_are_synchronized_with_tablets, time.time() + 60)
            await asyncio.sleep(3)

        stop_writer.set()
        writer_thread.join()

        # disable tablet balancing to quiesce stream changes during the verification step
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        table_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test")
        log_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test_scylla_cdc_log")
        assert table_rows[0].count <= log_rows[0].count
        total_table_rows = table_rows[0].count

        # for each timestamp range, read the log rows of streams in this timestamp.
        # we should get exactly one log row for each inserted table row.
        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' ORDER BY timestamp ASC")
        timestamps = [r.ts for r in rows]
        total_read_log_rows = 0
        for ts, next_ts in zip(timestamps, timestamps[1:] + [None]):
            rows = await cql.run_async(f"SELECT stream_id, stream_state FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts}")
            current_streams = set([r.stream_id for r in rows if r.stream_state == CdcStreamState.CURRENT])
            closed_streams = set([r.stream_id for r in rows if r.stream_state == CdcStreamState.CLOSED])

            # for a new opened stream we read it starting from the timestamp
            # where it is introduced, and for a closed stream we stop reading
            # it at the timestamp where it is closed. otherwise we would see
            # duplicate log writes.
            for sid in current_streams:
                query = f"SELECT COUNT(*) FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} AND \"cdc$time\" >= minTimeuuid({ts})"
                if next_ts:
                    query += f" AND \"cdc$time\" < minTimeuuid({next_ts})"
                rows = await cql.run_async(query)
                total_read_log_rows += rows[0].count

            for sid in closed_streams:
                rows = await cql.run_async(f"SELECT toUnixTimestamp(\"cdc$closed_time\") AS closed_time FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} LIMIT 1")
                assert rows[0].closed_time == ts
        assert total_read_log_rows == total_table_rows

        # Read each stream in another way - from its opened timestamp to the closed
        # timestamp according to the "cdc$closed_time" column.
        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts, stream_id, stream_state FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")
        streams = []
        for r in rows:
            if r.stream_state == CdcStreamState.OPENED:
                streams.append((r.stream_id, r.ts))
        total_read_log_rows = 0
        for (sid, opened_ts) in streams:
                rows = await cql.run_async(f"SELECT toUnixTimestamp(\"cdc$closed_time\") AS closed_time FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} LIMIT 1")
                closed_ts = rows[0].closed_time if len(rows) > 0 else None
                query = f"SELECT COUNT(*) FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} AND \"cdc$time\" >= minTimeuuid({opened_ts})"
                if closed_ts:
                    query += f" AND \"cdc$time\" < minTimeuuid({closed_ts})"
                rows = await cql.run_async(query)
                total_read_log_rows += rows[0].count
        assert total_read_log_rows == total_table_rows

# Similar to the previous test, but now we have two writer threads writing to the same partitions
# and doing overwrites. We write to the table while the streams are changed.
# Finally we read the values from the log and store the latest value for each partition, and we
# verify it is equal to the value we get by reading the table.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_write_during_split_and_merge_with_overwrites(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        # start multiple writer threads that write to the same partitions but with different v values
        stop_writer = threading.Event()
        def do_writes(v):
            i=0
            stmt = cql.prepare(f"INSERT INTO {ks}.test(pk, v) VALUES(?, ?)")
            while not stop_writer.is_set():
                cql.execute(stmt, [i, v])
                i += 1
        writers = [threading.Thread(target=do_writes, args=(v,)) for v in [1, 2]]

        for w in writers:
            w.start()

        for tablet_count in [4, 16, 1]:
            log = await manager.server_open_log(servers[0].server_id)
            mark = await log.mark()
            await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': {tablet_count}}};")
            await log.wait_for("Detected tablet (split|merge)", from_mark=mark, timeout=60)

            async def streams_are_synchronized_with_tablets():
                tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
                rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
                ts = rows[0].ts
                rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
                logger.info(rows)
                if len(rows) == tablet_count:
                    return True
            await wait_for(streams_are_synchronized_with_tablets, time.time() + 60)
            await asyncio.sleep(3)

        stop_writer.set()
        for w in writers:
            w.join()

        # disable tablet balancing to quiesce stream changes during the verification step
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        # get all streams by their creation order.
        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts, stream_id, stream_state FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")
        streams = []
        for r in rows:
            if r.stream_state == CdcStreamState.OPENED:
                streams.append((r.stream_id, r.ts))

        # read the log entries for each stream by their creation order, and for each partition store the latest v value in the log.
        latest = {}
        for (sid, opened_ts) in streams:
            rows = await cql.run_async(f"SELECT toUnixTimestamp(\"cdc$closed_time\") AS closed_time FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} LIMIT 1")
            closed_ts = rows[0].closed_time if len(rows) > 0 else None
            query = f"SELECT * FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{sid.hex()} AND \"cdc$time\" >= minTimeuuid({opened_ts})"
            if closed_ts:
                query += f" AND \"cdc$time\" < minTimeuuid({closed_ts})"
            rows = await cql.run_async(query)

            for r in rows:
                latest[r.pk] = r.v

        # verify the latest v value in the log is equal to the value in the table.
        table_rows = await cql.run_async(f"SELECT * FROM {ks}.test")
        for r in table_rows:
            assert latest[r.pk] == r.v

# Test compaction of CDC streams.
# Create a CDC table, then split the tablets to create a new stream set.
# Force compaction of the CDC streams by setting a short compaction age, and wait
# until we have only a single stream set again.
# Verify the streams are equal to the most recent stream set.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_stream_compaction(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1, 'error_injections_at_startup': ['short_cdc_stream_compaction_refresh_interval' ] }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true, 'ttl': 5}}")

        rows = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        assert len(rows) == 1

        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': 4}};")

        async def new_stream_timestamp_created():
            rows = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
            if len(rows) > 1:
                return True
        await wait_for(new_stream_timestamp_created, time.time() + 60)

        ts_before = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        streams_before = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")

        async def streams_compacted():
            rows = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
            if len(rows) == 1:
                return True
        await wait_for(streams_compacted, time.time() + 60)

        ts_after = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        streams_after = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")

        # now we have a single timestamp, and it is the most recent one
        assert len(ts_after) == 1
        assert ts_after[0].timestamp == ts_before[0].timestamp

        # now there is a single stream set, and it is the same as the stream set for the most recent timestamp
        assert set([r.stream_id for r in streams_after if r.stream_state == CdcStreamState.CURRENT]) == \
                set([r.stream_id for r in streams_before if r.timestamp == ts_before[0].timestamp and r.stream_state == CdcStreamState.CURRENT])

        # the cdc_streams_history is empty. we have only a base stream set
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_history")
        assert len(rows) == 0

# Test that base table writes and their corresponding CDC log writes are co-located on the same replica.
# We create a 2-node cluster with RF=1, stop one node, and verify that the partitions available
# on the alive node work correctly for both base table and CDC log.
@pytest.mark.asyncio
async def test_cdc_colocation(manager: ManagerClient):
    # Create 2-node cluster to test co-location
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 16}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        # Insert diverse test data to ensure distribution across both nodes
        test_data = {}
        for i in range(100):
            test_data[i] = i * 10
            await cql.run_async(f"INSERT INTO {ks}.test(pk, v) VALUES({i}, {i * 10})")

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        # create map that maps each stream_id to a list of all partitions it contains
        rows = await cql.run_async(f"SELECT \"cdc$stream_id\" as sid, pk FROM {ks}.test_scylla_cdc_log")
        stream_partitions = {}
        pk_to_stream = {}
        for r in rows:
            stream_id = r.sid
            if r.pk in pk_to_stream:
                assert pk_to_stream[r.pk] == stream_id, f"Partition {r.pk} is in multiple streams: {pk_to_stream[r.pk]} and {stream_id}"
            if stream_id not in stream_partitions:
                stream_partitions[stream_id] = []
            stream_partitions[stream_id].append(r.pk)

        pk_to_host = {}
        rows = await cql.run_async(f"SELECT pk, token(pk) AS tok FROM {ks}.test")
        for r in rows:
            replicas = await get_tablet_replicas(manager, servers[0], ks, 'test', r.tok)
            assert len(replicas) == 1, f"Partition {r.pk} has multiple replicas: {replicas}"
            pk_to_host[r.pk] = replicas[0][0]

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        # Stop first node and test partitions available on second node
        await manager.server_stop(servers[0].server_id)

        accessible_partitions = set([pk for pk, host_id in pk_to_host.items() if host_id == s1_host_id])
        inaccessible_partitions = set([pk for pk, host_id in pk_to_host.items() if host_id == s0_host_id])

        for stream_id, partitions in stream_partitions.items():
            partitions_set = set(partitions)
            if partitions_set.issubset(accessible_partitions):
                # Verify that the CDC log entries for this stream are accessible
                rows = await cql.run_async(f"SELECT * FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{stream_id.hex()}")
                assert len(rows) > 0, f"No CDC log entries found for stream {stream_id.hex()}"
            else:
                assert partitions_set.issubset(inaccessible_partitions), \
                    f"Stream {stream_id} partitions {partitions} are not fully contained in inaccessible partitions"

        await manager.server_start(servers[0].server_id)

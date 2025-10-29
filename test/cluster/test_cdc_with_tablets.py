#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

# Tests for CDC tables in tablets enabled keyspaces

from collections import defaultdict
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

        # Verify the cdc log table is created as a colocated table.
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
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_history")
        assert len(rows) == 0

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
    rows = await cql.run_async("SELECT * FROM system.cdc_streams_history")
    assert len(rows) == 0

# Create a table with CDC, then disable CDC and drop the CDC log table, and create CDC again.
# Verify streams are created and cleaned up correctly.
@pytest.mark.asyncio
async def test_drop_and_recreate_cdc(manager: ManagerClient):
    await manager.servers_add(1)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")
        await cql.run_async(f"ALTER TABLE {ks}.test1 WITH cdc={{'enabled': false}}")

        # The CDC table still exists and streams are still present
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) > 0

        await cql.run_async(f"DROP TABLE {ks}.test1_scylla_cdc_log")

        # the streams should be removed now
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) == 0

        await cql.run_async(f"ALTER TABLE {ks}.test1 WITH cdc={{'enabled': true}}")

        # new streams are created
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
        assert len(rows) > 0

    # after dropping the keyspace, the streams should be removed
    rows = await cql.run_async("SELECT * FROM system.cdc_streams_state")
    assert len(rows) == 0

async def validate_virtual_tables(manager, servers, cql, log_table_id, ks, table):
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    # read all streams from the internal tables and verify against the virtual tables
    base_rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts, stream_id FROM system.cdc_streams_state WHERE table_id={log_table_id}")
    history_rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts, stream_id, stream_state FROM system.cdc_streams_history WHERE table_id={log_table_id} ORDER BY timestamp ASC")

    all_streams = defaultdict(list)
    for r in base_rows:
        all_streams[r.ts].append((r.stream_id, CdcStreamState.OPENED))
    for r in history_rows:
        all_streams[r.ts].append((r.stream_id, r.stream_state))

    # verify the timestamps in the internal table match those in the virtual table
    virtual_ts_rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='{table}' ORDER BY timestamp ASC")
    virtual_ts = [r.ts for r in virtual_ts_rows]

    assert list(all_streams.keys()) == virtual_ts, f"Timestamps mismatch: internal {all_streams.keys()}, virtual {virtual_ts}"

    # verify the current stream set for each timestamp in the virtual table matches the stream set
    # we get from the internal tables by starting from the base and applying the diffs
    current_streams = set()
    for ts, streams in all_streams.items():
        closed_streams = set([s for s, state in streams if state == CdcStreamState.CLOSED])
        opened_streams = set([s for s, state in streams if state == CdcStreamState.OPENED])

        assert closed_streams.issubset(current_streams)

        current_streams = (current_streams - closed_streams) | opened_streams

        rows = await cql.run_async(f"SELECT stream_id FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='{table}' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
        current_streams_virtual = set([r.stream_id for r in rows])
        assert current_streams_virtual == current_streams

# Read CDC stream information from the virtual tables system.cdc_timestamps and system.cdc_streams
# and verify it against the internal CDC tables.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_virtual_tables(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:

        assert [] == await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert [] == await cql.run_async("SELECT * FROM system.cdc_streams")

        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")
        log_table_id = await manager.get_table_id(ks, "test_scylla_cdc_log")

        await validate_virtual_tables(manager, servers, cql, log_table_id, ks, 'test')

        # trigger multiple tablet splits to create new cdc timestamps and streams and validate the
        # virtual tables after each one.
        for _ in range(3):
            prev_history_count = (await cql.run_async(f"SELECT COUNT(*) AS cnt FROM system.cdc_streams_history WHERE table_id={log_table_id}"))[0].cnt
            prev_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
            await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': {prev_tablet_count * 2}}};")

            async def tablet_count_is(expected_tablet_count):
                new_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
                if new_tablet_count == expected_tablet_count:
                    return True
            await wait_for(lambda: tablet_count_is(prev_tablet_count * 2), time.time() + 60)
            new_history_count = (await cql.run_async(f"SELECT COUNT(*) AS cnt FROM system.cdc_streams_history WHERE table_id={log_table_id}"))[0].cnt
            assert new_history_count > prev_history_count

            await validate_virtual_tables(manager, servers, cql, log_table_id, ks, 'test')

        # drop the table and verify the virtual tables are cleared
        await cql.run_async(f"DROP TABLE {ks}.test")

        assert [] == await cql.run_async("SELECT * FROM system.cdc_timestamps")
        assert [] == await cql.run_async("SELECT * FROM system.cdc_streams")
        await validate_virtual_tables(manager, servers, cql, log_table_id, ks, 'test')

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_virtual_tables_with_multiple_cdc_tables(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    N = 3
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        for i in range(N):
            await cql.run_async(f"CREATE TABLE {ks}.test{i} (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        log_table_id = [await manager.get_table_id(ks, f"test{i}_scylla_cdc_log") for i in range(N)]

        virt_tables = await cql.run_async(f"SELECT table_name FROM system.cdc_timestamps WHERE keyspace_name='{ks}' ALLOW FILTERING")
        assert set([r.table_name for r in virt_tables]) == set([f'test{i}' for i in range(N)])

        for i in range(N):
            await validate_virtual_tables(manager, servers, cql, log_table_id[i], ks, f'test{i}')

        # drop one table and verify it's removed from the virtual tables and the others remain valid.
        await cql.run_async(f"DROP TABLE {ks}.test{N-1}")
        log_table_id = log_table_id[:-1]
        N = N - 1

        virt_tables = await cql.run_async(f"SELECT table_name FROM system.cdc_timestamps WHERE keyspace_name='{ks}' ALLOW FILTERING")
        assert set([r.table_name for r in virt_tables]) == set([f'test{i}' for i in range(N)])

        for i in range(N):
            await validate_virtual_tables(manager, servers, cql, log_table_id[i], ks, f'test{i}')

    assert [] == await cql.run_async("SELECT * FROM system.cdc_timestamps")
    assert [] == await cql.run_async("SELECT * FROM system.cdc_streams")


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
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}} AND tablets = {{'min_tablet_count': 2}}")

        async def assert_streams_are_synchronized_with_tablets():
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
            rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' ORDER BY timestamp DESC LIMIT 1")
            ts = rows[0].ts # most recent
            rows = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp = {ts} AND stream_state = {CdcStreamState.CURRENT}")
            assert len(rows) == tablet_count

        await assert_streams_are_synchronized_with_tablets()

        prev_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': {prev_tablet_count * 2}}};")

        async def tablet_count_is(expected_tablet_count):
            new_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
            if new_tablet_count == expected_tablet_count:
                return True

        await wait_for(lambda: tablet_count_is(prev_tablet_count * 2), time.time() + 60)
        await assert_streams_are_synchronized_with_tablets()

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

        prev_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test_scylla_cdc_log')
        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': {prev_tablet_count // 2}}};")
        await wait_for(lambda: tablet_count_is(prev_tablet_count // 2), time.time() + 60)
        await assert_streams_are_synchronized_with_tablets()

        await validate_stream_history()

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

# Test garbage collection of CDC streams.
# Create a CDC table with short TTL, then split the tablets to create a new stream set,
# and wait until the old streams are garbage collected and we have a single stream set again.
# Verify the remaining stream set is equal to the most recent stream set.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_stream_garbage_collection(manager: ManagerClient):
    cfg = { 'tablet_load_stats_refresh_interval_in_seconds': 1, 'error_injections_at_startup': ['short_cdc_streams_gc_refresh_interval' ] }
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true, 'ttl': 3600}}")

        rows = await cql.run_async(f"SELECT toUnixTimestamp(timestamp) as ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
        assert len(rows) == 1
        ts1 = rows[0].ts

        await cql.run_async(f"ALTER TABLE {ks}.test_scylla_cdc_log WITH tablets = {{'min_tablet_count': 4}};")

        async def new_stream_timestamp_created():
            rows = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' AND timestamp > {ts1}")
            if len(rows) > 0:
                return True
        await wait_for(new_stream_timestamp_created, time.time() + 60)

        ts_before = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' ORDER BY timestamp DESC")
        streams_before = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")
        assert len(ts_before) == 2

        # set short TTL to trigger garbage collection
        await cql.run_async(f"ALTER TABLE {ks}.test WITH cdc = {{'enabled': true, 'ttl': 5}}")

        async def streams_garbage_collected():
            rows = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test'")
            if len(rows) == 1:
                return True
        await wait_for(streams_garbage_collected, time.time() + 60)

        ts_after = await cql.run_async(f"SELECT * FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='test' ORDER BY timestamp DESC")
        streams_after = await cql.run_async(f"SELECT * FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='test'")

        # now we have a single timestamp, and it is the most recent one
        assert len(ts_after) == 1
        assert ts_after[0].timestamp == ts_before[0].timestamp

        # now there is a single stream set, and it is the same as the stream set for the most recent timestamp
        assert set([r.stream_id for r in streams_after if r.stream_state == CdcStreamState.CURRENT]) == \
                set([r.stream_id for r in streams_before if r.timestamp == ts_before[0].timestamp and r.stream_state == CdcStreamState.CURRENT])

        # cdc_streams_history is empty. we have only a base stream set
        rows = await cql.run_async("SELECT * FROM system.cdc_streams_history")
        assert len(rows) == 0

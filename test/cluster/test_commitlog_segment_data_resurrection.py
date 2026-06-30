#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace

import pytest
import os
import logging
import glob
import json
import asyncio
import time
from functools import partial
from datetime import datetime, timezone
from test.cluster.util import new_test_keyspace
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def test_pinned_cl_segment_doesnt_resurrect_data(manager: ManagerClient):
    """
        The tested scenario is as follows:
        * Two tables, ks1.tbl1 and ks2.tbl2.
        * A commitlog segment contains writes for both tables.
        * ks1.tbl1 has very low write traffic, memtables are very rarely
          flushed, this results in ks1.tbl1 "pinning" any commitlog segment
          which has writes, that are still in the memtable.
        * ks2.tbl2 deletes the data, some of which is still in the segment
          pinned by ks1.tbl1.
        * ks2.tbl2 flushes the memtable, data gets to sstables, commitlog
          segments containing writes for both tables are still pinned.
        * ks2.tbl2 compacts and purges away both the data and the tombstone.
        * The node has an unclean restart, the pinned segments are replayed and
          data is resurrected, as the tombstone is already purged.

        This test uses gc_grace_seconds=0 to make the test fast.

        See https://github.com/scylladb/scylladb/issues/14870
        """
    server = await manager.server_add(config={
        "commitlog_sync": "batch",
        "commitlog_segment_size_in_mb": 1,
        "enable_cache": False
    })
    cql = manager.cql

    def commitlog_path():
        return json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'commitlog_directory'").one().value)

    async def get_segments_num():
        metrics_res = await manager.metrics.query(server.ip_addr)
        return int(metrics_res.get("scylla_commitlog_segments"))

    def get_cl_segments():
        return {os.path.basename(s) for s in glob.glob(os.path.join(cl_path, "CommitLog-*"))}

    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks1, \
               new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks2:
        tbl1 = f"{ks1}.tbl1"
        tbl2 = f"{ks2}.tbl2"
        await cql.run_async(f"create table {tbl1} (pk int, ck int, primary key(pk, ck))")
        await cql.run_async(f"create table {tbl2} (pk int, ck int, v text, primary key(pk, ck)) WITH gc_grace_seconds = 0")

        cl_path = commitlog_path()
        segments_before_writes = await get_segments_num()
        segments_after_writes = segments_before_writes

        logger.debug(f"Have {segments_after_writes} segments before writing data")

        insert_id_tbl1 = cql.prepare(f"INSERT INTO {tbl1} (pk, ck) VALUES (?, ?)")
        insert_id_tbl2 = cql.prepare(f"INSERT INTO {tbl2} (pk, ck, v) VALUES (?, ?, ?)")
        pk1 = 0
        pk2 = 1
        ck = 0
        value = "v" * 1024

        logger.debug(f"Filling segment with mixed data from {tbl1} and {tbl2}")

        # Ensure at least one segment with writes from both tables
        while segments_after_writes < segments_before_writes + 1:
            cql.execute(insert_id_tbl1, (pk1, ck))
            cql.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_num()

        logger.debug(f"Filling segment(s) with {tbl2} only")

        while segments_after_writes < segments_before_writes + 3:
            cql.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_num()

        cql.execute(f"DELETE FROM {tbl2} WHERE pk = {pk1}")

        # We need to make sure the segment in which the above delete landed in
        # is full, otherwise the memtable flush will not be able to destroy it.
        logger.debug(f"Filling another segment with {tbl2} (pk={pk2})")

        while segments_after_writes < segments_before_writes + 4:
            cql.execute(insert_id_tbl2, (pk2, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_num()

        segments_before = get_cl_segments()
        logger.debug(f"Wrote {ck} rows, now have {segments_after_writes} segments ({segments_before}")

        logger.debug(f"Flush {tbl2}")
        await manager.api.keyspace_flush(node_ip=server.ip_addr, keyspace=ks2, table="tbl2")
        await manager.api.keyspace_compaction(node_ip=server.ip_addr, keyspace=ks2, table="tbl2")

        segments_after = get_cl_segments()
        logger.debug(f"After flush+compact, now have {await get_segments_num()} segments ({segments_after})")

        assert len(list(cql.execute(f"SELECT * FROM {tbl1} WHERE pk = {pk1}"))) > 0
        assert len(list(cql.execute(f"SELECT * FROM {tbl2} WHERE pk = {pk1}"))) == 0

        # Need to ensure at least one segment was freed.
        # We assume the last segment, containing the tombstone, was among the freed ones.
        logger.debug(f"before seg {segments_before}, after seg {segments_after}")
        removed_segments = segments_before - segments_after
        assert len(removed_segments) > 0

        logger.debug(f"The following segments were removed: {removed_segments}")

        logger.debug("Kill + restart the node")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)

        manager.driver_close()
        await manager.driver_connect()
        cql = manager.cql

        assert len(list(cql.execute(f"SELECT * FROM {tbl2} WHERE pk = {pk1}"))) == 0


@pytest.mark.asyncio
@pytest.mark.slow
async def test_pinned_cl_segment_doesnt_resurrect_data_but_repair_ensures_tombstone_gc(manager: ManagerClient):
    """
    """
    cfg = {
        "commitlog_sync": "batch",
        "commitlog_segment_size_in_mb": 1,
        "enable_cache": False,
        "hinted_handoff_enabled": False,
        "repair_hints_batchlog_flush_cache_time_in_ms": 0,
    }
    servers = await manager.servers_add(3, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}]
    )

    cql = manager.cql

    hosts = [(await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]
             for s in servers]

    async def get_segments_num(server):
        metrics_res = await manager.metrics.query(server.ip_addr)
        return int(metrics_res.get("scylla_commitlog_segments"))

    async def get_segments_nums():
        return [await get_segments_num(s) for s in servers]

    def less_than_by(after, before, off = 0):
        return all(x < (y + off) for x, y in zip(before, after))

    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks1, \
               new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks2:
        tbl1 = f"{ks1}.tbl1"
        tbl2 = f"{ks2}.tbl2"
        await cql.run_async(f"create table {tbl1} (pk int, ck int, primary key(pk, ck)) WITH tombstone_gc = {{'mode': 'repair' }}")
        await cql.run_async(f"create table {tbl2} (pk int, ck int, v text, primary key(pk, ck)) WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}")

        insert_id_tbl1 = cql.prepare(f"INSERT INTO {tbl1} (pk, ck) VALUES (?, ?)")
        insert_id_tbl2 = cql.prepare(f"INSERT INTO {tbl2} (pk, ck, v) VALUES (?, ?, ?)")
        pk1 = 0
        pk2 = 1
        ck = 0
        value = "v" * 1024

        segments_before_writes = await get_segments_nums()
        segments_after_writes = segments_before_writes

        logger.debug("Have %s segments before writing data", segments_after_writes)

        logger.debug("Filling segment with mixed data from %s and %s", tbl2, tbl2)

        # Ensure at least one segment with writes from both tables
        while less_than_by(segments_before_writes, segments_after_writes, 1):
            cql.execute(insert_id_tbl1, (pk1, ck))
            cql.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_nums()

        logger.debug("Filling segment(s) with %s only", tbl2)

        while less_than_by(segments_before_writes, segments_after_writes, 3):
            cql.execute(insert_id_tbl2, (pk1, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_nums()

        cql.execute(f"DELETE FROM {tbl2} WHERE pk = {pk1}")

        # We need to make sure the segment in which the above delete landed in
        # is full, otherwise the memtable flush will not be able to destroy it.
        logger.debug("Filling another segment with %s (pk=%s)", tbl2, pk2)

        while less_than_by(segments_before_writes, segments_after_writes, 4):
            cql.execute(insert_id_tbl2, (pk2, ck, value))
            ck = ck + 1
            segments_after_writes = await get_segments_nums()

        logger.debug("Wrote %s rows, now have %s segments", ck, segments_after_writes)
        logger.debug("Flush %s", tbl2)

        async def flush_ks(server):
            await manager.api.keyspace_flush(node_ip=server.ip_addr, keyspace=ks2, table="tbl2")

        async def compact_ks(server):
            await manager.api.keyspace_compaction(node_ip=server.ip_addr, keyspace=ks2, table="tbl2")

        await asyncio.gather(*[flush_ks(s) for s in servers])

        segments_after = await get_segments_nums()
        logger.debug("After flush, now have %s segments", segments_after)

        assert len(list(cql.execute(f"SELECT * FROM {tbl1} WHERE pk = {pk1}"))) > 0
        assert len(list(cql.execute(f"SELECT * FROM {tbl2} WHERE pk = {pk1}"))) == 0

        tombstone_mark = datetime.now(timezone.utc)

        def get_tombstone(row):
            if row.metadata is None:
                return None
            metadata = json.loads(row.metadata)
            return metadata.get("tombstone")

        async def list_tombstones(tombstone_mark, host):
            res = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({tbl2})", host=host))
            tombstones = []
            for row in res:
                tombstone = get_tombstone(row)
                if tombstone and datetime.fromtimestamp(float(tombstone["timestamp"])/1_000_000, timezone.utc) < tombstone_mark:
                    tombstones.append(tombstone)
            return tombstones

        async def list_all_tombstones(tombstone_mark):
            tombstones_per_host = await asyncio.gather(*[list_tombstones(tombstone_mark, host)
                                                         for host in hosts])
            all_tombstones = []
            for tombstones in tombstones_per_host:
                all_tombstones += tombstones
            return all_tombstones

        async def tombstone_gc_completed(tombstone_mark):
            # flush and compact the keyspace
            await asyncio.gather(*[flush_ks(s) for s in servers])
            await asyncio.gather(*[compact_ks(s) for s in servers])

            all_tombstones = await list_all_tombstones(tombstone_mark)
            logger.debug(all_tombstones)
            tombstones_count_total = len(all_tombstones)
            if tombstones_count_total != 0:
                return None
            return True

        # should usually run much faster than 30s, but left some margin to avoid flakiness
        async def verify_tombstone_gc(tombstone_mark, timeout=30):
            deadline = time.time() + timeout
            await wait_for(partial(tombstone_gc_completed, tombstone_mark), deadline)


        tombstones = await list_all_tombstones(tombstone_mark)

        assert len(tombstones) > 0, "there should be tombstones at this point"

        # wait for 2 sec to let the current tombstones fully expire
        #await asyncio.sleep(2)
        await manager.api.repair(servers[0].ip_addr, ks2, "tbl2")

        # now we should be able to get to a state where all tombstones are gone.
        await verify_tombstone_gc(tombstone_mark)

        logger.debug("Kill + restart the nodes")

        await asyncio.gather(*[manager.server_stop(s.server_id, False) for s in servers])
        await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])

        manager.driver_close()
        await manager.driver_connect()
        cql = manager.cql

        assert len(list(cql.execute(f"SELECT * FROM {tbl2} WHERE pk = {pk1}"))) == 0

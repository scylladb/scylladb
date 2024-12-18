#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import pytest
import os
import logging
import glob
import json

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
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

    await cql.run_async("create keyspace ks1 with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    await cql.run_async("create keyspace ks2 with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    await cql.run_async("create table ks1.tbl1 (pk int, ck int, primary key(pk, ck))")
    await cql.run_async("create table ks2.tbl2 (pk int, ck int, v text, primary key(pk, ck)) WITH gc_grace_seconds = 0")

    cl_path = commitlog_path()
    segments_before_writes = await get_segments_num()
    segments_after_writes = segments_before_writes

    logger.debug(f"Have {segments_after_writes} segments before writing data")

    insert_id_tbl1 = cql.prepare("INSERT INTO ks1.tbl1 (pk, ck) VALUES (?, ?)")
    insert_id_tbl2 = cql.prepare("INSERT INTO ks2.tbl2 (pk, ck, v) VALUES (?, ?, ?)")
    pk1 = 0
    pk2 = 1
    ck = 0
    value = "v" * 1024

    logger.debug(f"Filling segment with mixed data from ks1.tbl1 and ks2.tbl2")

    # Ensure at least one segment with writes from both tables
    while segments_after_writes < segments_before_writes + 1:
        cql.execute(insert_id_tbl1, (pk1, ck))
        cql.execute(insert_id_tbl2, (pk1, ck, value))
        ck = ck + 1
        segments_after_writes = await get_segments_num()

    logger.debug(f"Filling segment(s) with ks2.tbl2 only")

    while segments_after_writes < segments_before_writes + 3:
        cql.execute(insert_id_tbl2, (pk1, ck, value))
        ck = ck + 1
        segments_after_writes = await get_segments_num()

    cql.execute(f"DELETE FROM ks2.tbl2 WHERE pk = {pk1}")

    # We need to make sure the segment in which the above delete landed in
    # is full, otherwise the memtable flush will not be able to destroy it.
    logger.debug(f"Filling another segment with ks2.tbl2 (pk={pk2})")

    while segments_after_writes < segments_before_writes + 4:
        cql.execute(insert_id_tbl2, (pk2, ck, value))
        ck = ck + 1
        segments_after_writes = await get_segments_num()

    segments_before = get_cl_segments()
    logger.debug(f"Wrote {ck} rows, now have {segments_after_writes} segments ({segments_before}")

    logger.debug("Flush ks2.tbl2")
    await manager.api.keyspace_flush(node_ip=server.ip_addr, keyspace="ks2", table="tbl2")
    await manager.api.keyspace_compaction(node_ip=server.ip_addr, keyspace="ks2", table="tbl2")

    segments_after = get_cl_segments()
    logger.debug(f"After flush+compact, now have {await get_segments_num()} segments ({segments_after})")

    assert len(list(cql.execute(f"SELECT * FROM ks1.tbl1 WHERE pk = {pk1}"))) > 0
    assert len(list(cql.execute(f"SELECT * FROM ks2.tbl2 WHERE pk = {pk1}"))) == 0

    # Need to ensure at least one segment was freed.
    # We assume the last segment, containing the tombstone, was among the freed ones.
    logger.debug(f"before seg {segments_before}, after seg {segments_after}")
    removed_segments = segments_before - segments_after
    assert len(removed_segments) > 0

    logger.debug(f"The following segments were removed: {removed_segments}")

    logger.debug("Kill + restart the node")
    await manager.server_stop(server.server_id)
    await manager.server_start(server.server_id)

    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql

    assert len(list(cql.execute(f"SELECT * FROM ks2.tbl2 WHERE pk = {pk1}"))) == 0

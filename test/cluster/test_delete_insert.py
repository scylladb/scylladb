#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import concurrent.futures
import logging
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from math import floor

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_ks
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

ROWS = [(str(uuid.uuid1()), x, random.choice(["group1", "group2", "group3", "group4"])) for x in range(1000)]


def insert_some_rows(session, rows):
    for row in rows:
        session.execute("INSERT INTO test (id, val1, group) VALUES ({}, '{}', '{}')".format(*row))


def delete_group_rows(session, group):
    """TestDeleteInsert.delete_group_rows(): DELETE ... IN of at most 100
    keys at a time (the last chunk leaves out its last key, as the dtest)."""
    rows = [r for r in ROWS if r[2] == group]
    ids = [r[0] for r in rows]
    start = 0
    last_portion = len(ids)
    if len(ids) >= 100:
        last_portion = len(ids) % 100
        for i in range(floor(len(ids) / 100)):
            end = 100 * (i + 1)
            session.execute(f"DELETE FROM test WHERE id in ({', '.join(ids[start:end])})")
            start = end
    if last_portion:
        session.execute(f"DELETE FROM test WHERE id in ({', '.join(ids[start:-1])})")
    return rows


# Ported from scylla-dtest delete_insert_test.py::TestDeleteInsert::test_delete_insert_search.
# Two DCs of two racks, RF {dc1: 2, dc2: 2}, a table with an index on
# "group": 1000 rows, the group2 rows deleted and inserted back at
# LOCAL_QUORUM, then 20 parallel LOCAL_QUORUM index queries for group2
# through sessions to random nodes. As in the dtest, the queries' outcome
# is only logged: the dtest waits for them without collecting their results.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_delete_insert_search(manager: ScyllaClusterManager):
    servers = await manager.servers_add(4, config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}, property_file=[
        {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}, {"dc": "dc2", "rack": "rack1"}, {"dc": "dc2", "rack": "rack2"}])
    cluster = manager.con_gen([servers[0].ip_addr])
    session = cluster.connect()
    sessions = [cluster]
    try:
        session.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.LOCAL_QUORUM
        create_ks(session, "delete_insert_search_test", {"dc1": 2, "dc2": 2})
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        session.execute("USE delete_insert_search_test")
        session.execute("CREATE TABLE test (id uuid PRIMARY KEY, val1 text, group text)")
        session.execute("CREATE INDEX group_idx ON test (group)")
        insert_some_rows(session, ROWS)
        deleted = delete_group_rows(session, "group2")
        insert_some_rows(session, deleted)

        def run_query(connection):
            query = SimpleStatement("SELECT * FROM delete_insert_search_test.test WHERE group = 'group2'", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            rows = list(connection.execute(query))
            assert len(rows) == len(deleted), f"Expected {len(deleted)}, got {len(rows)}"

        def run_all():
            executor = ThreadPoolExecutor(max_workers=20)
            threads = []
            for _ in range(20):
                c = manager.con_gen([random.choice(servers).ip_addr])
                sessions.append(c)
                threads.append(executor.submit(run_query, c.connect()))
            concurrent.futures.wait(threads, return_when=concurrent.futures.ALL_COMPLETED)
            for t in threads:
                if t.exception():
                    logger.info(f"query failed: {t.exception()}")
        await asyncio.to_thread(run_all)
    finally:
        for c in sessions:
            c.shutdown()

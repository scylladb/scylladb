#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import pytest
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import create_new_test_keyspace


@pytest.mark.skip_mode(mode='release', reason="error injections aren't enabled in release mode")
async def test_flush_during_table_creation(manager: ScyllaClusterManager):
    """
    Idea: a schema change registers a new table and marks it ready for writes
    in schema_applier::commit(), then finishes it in post_commit(). A table
    that accepts writes must be flushable at once: a write between the two
    lands in the memtable, and a keyspace flush that reaches the table in that
    window must not kill the node. It used to, when post_commit() was what
    created the table's data directory: the flush called make_storage() on
    storage options with an empty `dir` and aborted on
    "Local storage options is missing 'dir'".

    Setup: one node, a keyspace with RF=1, the
    schema_applier_pause_before_post_commit injection enabled so CREATE TABLE
    parks between commit() and post_commit().

    The test starts CREATE TABLE in the background, waits for it to park,
    inserts a row into the not-yet-finalized table and flushes the keyspace
    through the REST API. If the flush kills the node the test fails on the
    abort it finds in the log. Otherwise it releases the injection and checks
    that the flush and CREATE TABLE complete and the row is readable.
    """
    server = await manager.server_add()
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    injection = "schema_applier_pause_before_post_commit"
    await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
    create = cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
    await manager.api.wait_for_injection_enter(server.ip_addr, injection)

    # the table is registered and accepts writes, but is not finalized yet
    await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES (1, 1)")

    # the flush may legitimately wait for post_commit, so release the
    # injection if it has not returned by itself
    flush = asyncio.create_task(manager.api.keyspace_flush(server.ip_addr, ks))
    await asyncio.wait({flush}, timeout=30)
    if flush.done() and flush.exception() is not None:
        # the REST call fails only because the node died: show what killed it
        await manager.server_stop(server.server_id, convict=False)
        errors = await log.grep_for_errors(distinct_errors=True, from_mark=mark)
        pytest.fail(f"keyspace flush during table creation killed the node: {errors}")

    await manager.api.message_injection(server.ip_addr, injection)
    await flush
    await create
    rows = await cql.run_async(f"SELECT v FROM {ks}.t WHERE pk = 1")
    assert [r.v for r in rows] == [1]

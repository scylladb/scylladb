#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

INJECTION = "block_raft_io_fiber_commit_past_start"


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_single_node_loads_table_from_uncommitted_log_tail(manager: ScyllaClusterManager):
    """Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-5280.

    A restarting node replays the group0 log before it loads the non-system
    keyspaces, so that they are loaded from the current schema. The replay
    covers the entries up to the persisted commit index. On a one-node cluster
    the sole voter commits the rest of its log by itself as soon as it becomes
    leader, so a CREATE TABLE that is in the persisted log but past the
    persisted commit index is applied too, but asynchronously: when that
    happens after the keyspaces were loaded, the table reaches the schema
    tables on disk only and is not usable until the next restart.

    The test kills the node with a CREATE TABLE in the log past the commit
    index, restarts it holding the commit of the log tail until the keyspaces
    are loaded, and queries the table.
    """
    srv = await manager.server_add()
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    logger.info("Killing the node with the CREATE TABLE entry in the log, past the persisted commit index")
    await manager.api.enable_injection(srv.ip_addr, INJECTION, one_shot=False)
    create = cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY)")
    await manager.api.wait_for_injection_enter(srv.ip_addr, INJECTION)
    await manager.server_stop(srv.server_id, convict=False)
    with pytest.raises(Exception):
        await create

    logger.info("Restarting the node, holding the commit of the log tail until the non-system keyspaces are loaded")
    await manager.server_update_config(srv.server_id, "error_injections_at_startup", [INJECTION])
    log = await manager.server_open_log(srv.server_id)
    mark = await log.mark()
    start = asyncio.create_task(manager.server_start(srv.server_id))
    await log.wait_for("init - recovering logstor", from_mark=mark)
    await manager.api.disable_injection(srv.ip_addr, INJECTION)
    await start

    cql, _ = await manager.get_ready_cql([srv])
    tables = await cql.run_async("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ks'")
    assert [row.table_name for row in tables] == ["t"], "the CREATE TABLE entry was not applied"
    await cql.run_async("SELECT * FROM ks.t")

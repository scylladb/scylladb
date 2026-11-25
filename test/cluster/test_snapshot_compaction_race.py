#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Snapshots taken while compaction replaces sstables must not fail to
# hard-link an sstable that compaction just deleted ("link failed: No such
# file or directory"), ported from scylla-dtest snapshot_test.py: a snapshot
# right after a restart, when the compactions of the sstables loaded from
# disk run, and a snapshot during a major compaction.
# Reproduces https://github.com/scylladb/scylladb/issues/4051

import asyncio
import uuid

import pytest

from test import DEBUG_MODES
from test.cluster.test_compaction import wait_for_compactions
from test.cluster.util import cassandra_stress, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

# Like the dtests' ccm populate(1): the SimpleSnitch, the node in
# datacenter1/rack1, and vnodes keyspaces.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"}
LINK_FAILED = "failed: filesystem error: link failed: No such file or directory"


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_nodetool_snapshot_race_condition_with_compaction_after_node_start.
# 10000 stress writes, a graceful restart, and a snapshot right after the
# node is up.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_race_condition_with_compaction_after_node_start(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    await cassandra_stress(manager, server, "write", "n=10000", "-rate", "threads=10")
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    out, err, _ = await run_nodetool(manager, server, "snapshot", "-t", str(uuid.uuid4()))
    assert LINK_FAILED not in out + err
    assert not err, f"Some errors in creating snapshot: {err}"


# Ported from scylla-dtest snapshot_test.py::TestSnapshot::test_nodetool_snapshot_during_major_compaction.
# Stress writes into a table with compaction disabled, then a snapshot of
# all keyspaces as soon as "nodetool compact" started compacting it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_snapshot_during_major_compaction(manager: ScyllaClusterManager, build_mode):
    server = await manager.server_add(config=CONFIG)
    num_keys = 1000000 if build_mode not in DEBUG_MODES else 10000
    await cassandra_stress(manager, server, "write", f"n={num_keys}", "-rate", "threads=10",
                           "-schema", "compaction(strategy=SizeTieredCompactionStrategy,enabled=false)")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    compaction = asyncio.create_task(run_nodetool(manager, server, "compact"))
    await log.wait_for("User initiated compaction started on behalf of keyspace1.standard1", from_mark=mark)
    await asyncio.sleep(0.1)
    out, err, _ = await run_nodetool(manager, server, "snapshot")
    assert LINK_FAILED not in out + err
    assert not err, f"Some errors in creating snapshot: {err}"
    await compaction
    await wait_for_compactions(manager, server)

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import glob
import os
import shutil
import time

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, run_nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest


# Ported from scylla-dtest sstable_generation_loading_test.py::TestSSTableGenerationAndLoading::test_remove_index_file.
# One node (SimpleSnitch, vnodes, abort_on_malformed_sstable_error false)
# with 10000 cassandra-stress rows, flushed and compacted: without its
# Summary, the node starts and serves the same rows. Then, for each other
# component (Index, Filter, Statistics, Digest, Partitions, Rows), moved
# away, the node fails to start with a malformed sstable (except for a
# missing Digest, which is tolerated) and is killed, and the Data component
# is never deleted; with all components back, it serves the same rows
# (CASSANDRA-343).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_missing_sstable_component(manager: ScyllaClusterManager):
    node1 = await manager.server_add(config={"abort_on_malformed_sstable_error": False, "tablets_mode_for_new_keyspaces": "disabled",
                                             "num_tokens": 256, "endpoint_snitch": "SimpleSnitch"},
                                     property_file={"dc": "datacenter1", "rack": "rack1"})
    await cassandra_stress(manager, node1, "write", "n=10000", "-rate", "threads=8", create_keyspace=False)
    stress_table = "keyspace1.standard1"

    async def rows():
        session = await manager.get_cql_exclusive(node1)
        try:
            return list(session.execute(f"SELECT * FROM {stress_table}"))
        finally:
            session.cluster.shutdown()

    original_rows = await rows()
    await run_nodetool(manager, node1, "flush")
    await run_nodetool(manager, node1, "compact")
    await manager.server_stop_gracefully(node1.server_id)
    time.sleep(1)
    basepath = os.path.join(await manager.server_get_workdir(node1.server_id), "data", "keyspace1")
    path = next(os.path.join(basepath, d) for d in os.listdir(basepath) if d.startswith("standard1"))

    # The Summary can be regenerated and the data is still there.
    for f in glob.glob(f"{path}/*Summary.db"):
        os.unlink(f)
    await manager.server_start(node1.server_id)
    assert original_rows == await rows()

    await manager.server_stop_gracefully(node1.server_id)
    time.sleep(1)
    shutil.rmtree(f"{path}/snapshots", ignore_errors=True)
    os.mkdir(f"{path}/snapshots")
    for comp in ["Index.db", "Filter.db", "Statistics.db", "Digest.*", "Partitions.db", "Rows.db"]:
        files = glob.glob(f"{path}/*{comp}")
        if not files:
            continue
        for f in files:
            shutil.move(f, f"{path}/snapshots/")
        # A missing Digest component is tolerated (since 2026.2).
        if "Digest" in comp:
            await manager.server_start(node1.server_id)
            await manager.server_stop(node1.server_id, convict=False)
        else:
            await manager.server_start(node1.server_id, expected_error="malformed_sstable_exception|Startup failed: std::runtime_error")
        data_found = sum(1 for fname in os.listdir(path) if fname.endswith("Data.db"))
        assert data_found > 0, f"After removing {comp}, the data file was deleted!"
        for f in glob.glob(f"{path}/snapshots/*{comp}"):
            shutil.move(f, f"{path}/")

    await manager.server_start(node1.server_id)
    assert original_rows == await rows()
